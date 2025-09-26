package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cgomezcal/ds-ms-ibfs/internal/ibft"
	"github.com/cgomezcal/ds-ms-ibfs/internal/eth"
)

type httpBroadcaster struct {
	client *http.Client
	peers  []string
	log    *slog.Logger
	token  string
}

func (hb httpBroadcaster) Broadcast(msg ibft.Message) {
	b, _ := json.Marshal(msg)
	for _, p := range hb.peers {
		url := p + "/v1/ibft/message"
		go func(u string) {
			req, _ := http.NewRequest(http.MethodPost, u, bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			if hb.token != "" {
				req.Header.Set("Authorization", "Bearer "+hb.token)
			}
			resp, err := hb.client.Do(req)
			if err != nil {
				if hb.log != nil { hb.log.Error("broadcast failed", "peer", u, "err", err) }
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}(url)
	}
}

type Node struct {
	id    string
	peers []string
	e     *ibft.Engine
	mux   *http.ServeMux
	http  *http.Server
	mu    sync.RWMutex
	log   *slog.Logger
	peerToken string
	// tx aggregation
	leaderURL string
	privKeyHex string
	leaderWallet string
	isLeader bool
	// in-memory aggregation for current tx
	txPendingData string
	txSigs map[string]string // addr -> sigHex
	txItems map[string]collectReq // wallet(lower)->item
	txVoting bool
	// proposals known for IBFT (hash -> proposal)
	proposals map[string]proposalReq
	pendingProposalHash string
	// observability
	lastTxLaunchedAt time.Time
	lastTxLastData   string
}

func NewNode(id string, addr string, peers []string) *Node {
	// Configure structured logger (JSON) with level from LOG_LEVEL (info by default)
	lvl := new(slog.LevelVar)
	lvl.Set(slog.LevelInfo)
	switch os.Getenv("LOG_LEVEL") {
	case "debug":
		lvl.Set(slog.LevelDebug)
	case "warn":
		lvl.Set(slog.LevelWarn)
	case "error":
		lvl.Set(slog.LevelError)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))

	hb := httpBroadcaster{
		peers:  peers,
		client: &http.Client{Timeout: 2 * time.Second},
		log:    logger,
	}
	n := &Node{id: id, peers: peers, log: logger}
	// Start with self-only validators; caller can SetNetwork later
	n.e = ibft.NewEngine(id, []string{id}, hb)
	// metrics intentionally omitted per requirements
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK); _, _ = w.Write([]byte("ok"))
	})
	// metrics endpoint omitted
	mux.HandleFunc("/v1/ibft/message", n.handleMessage)
	mux.HandleFunc("/v1/ibft/propose", n.handlePropose)
	mux.HandleFunc("/v1/ibft/state", n.handleState)
	// transaction endpoints
	mux.HandleFunc("/v1/tx/execute-transaction", n.handleExecuteTx)
	mux.HandleFunc("/v1/tx/collect", n.handleCollect)
	mux.HandleFunc("/v1/tx/vote", n.handleVote)
	mux.HandleFunc("/v1/tx/proposal", n.handleProposal)
	mux.HandleFunc("/v1/tx/status", n.handleTxStatus)
	// Backwards-compatible aliases without versioned prefix
	mux.HandleFunc("/execute-transaction", n.handleExecuteTx)
	mux.HandleFunc("/collect", n.handleCollect)
	mux.HandleFunc("/vote", n.handleVote)
	mux.HandleFunc("/proposal", n.handleProposal)
	mux.HandleFunc("/tx/status", n.handleTxStatus)
	n.mux = mux
	n.http = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadTimeout:       5 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
	return n
}

func (n *Node) Start() error { return n.http.ListenAndServe() }

func (n *Node) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return n.http.Shutdown(ctx)
}

// txDebug returns current pending data and number of collected signatures.
// Intended for tests and diagnostics.
func (n *Node) txDebug() (string, int) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.txPendingData, len(n.txSigs)
}

// SetNetwork wires peers and validators for the node at runtime.
func (n *Node) SetNetwork(peerURLs []string, validators []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers = append([]string(nil), peerURLs...)
	n.e.SetBroadcaster(httpBroadcaster{peers: n.peers, client: &http.Client{Timeout: 2 * time.Second}, log: n.log, token: n.peerToken})
	if len(validators) > 0 {
		n.e.SetValidators(validators)
	}
}

// SetAuthToken configures a shared bearer token used for peer-to-peer requests.
// If set, /v1/ibft/message will require Authorization: Bearer <token>.
func (n *Node) SetAuthToken(token string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peerToken = token
	// Refresh broadcaster with token if peers already set
	n.e.SetBroadcaster(httpBroadcaster{peers: n.peers, client: &http.Client{Timeout: 2 * time.Second}, log: n.log, token: n.peerToken})
}

func (n *Node) handleMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
	// Peer auth (if configured)
	n.mu.RLock()
	token := n.peerToken
	n.mu.RUnlock()
	if token != "" {
		if ah := r.Header.Get("Authorization"); ah != "Bearer "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" { http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType); return }
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var msg ibft.Message
	if err := dec.Decode(&msg); err != nil { http.Error(w, "bad message", http.StatusBadRequest); return }
	// Gate PrePrepare only if we are in proposal-driven flow (proposals cache non-empty)
	if msg.Type == ibft.PrePrepare {
		n.mu.RLock()
		enforce := (n.proposals != nil && len(n.proposals) > 0)
		_, known := n.proposals[msg.Value]
		n.mu.RUnlock()
		if enforce && !known {
			http.Error(w, "unknown proposal", http.StatusBadRequest)
			return
		}
	}
	state, out, err := n.e.Handle(msg)
	if err != nil { http.Error(w, fmt.Sprintf("handle error: %v", err), http.StatusBadRequest); return }
	if out != nil {
		// local broadcast already happened through broadcaster; nothing to do
	}
	// If committed and value matches pending proposal, finalize transaction
	if state == "committed" {
		n.mu.Lock()
		if n.isLeader && n.pendingProposalHash != "" && n.pendingProposalHash == n.e.Value() {
			if n.log != nil { n.log.Info("TRANSACCIÓN LANZADA (IBFT)") }
			n.lastTxLaunchedAt = time.Now()
			n.lastTxLastData = n.txPendingData
			n.txSigs = make(map[string]string)
			n.txItems = make(map[string]collectReq)
			n.txPendingData = ""
			n.txVoting = false
			// prune cached proposals to avoid gating future rounds with stale values
			if n.proposals != nil {
				delete(n.proposals, n.pendingProposalHash)
				// If map gets large or empty, reset to nil to disable gating until next proposal
				if len(n.proposals) == 0 { n.proposals = nil }
			}
			n.pendingProposalHash = ""
		}
		n.mu.Unlock()
	}
	// metrics omitted
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"state": state})
}

func (n *Node) handlePropose(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" { http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType); return }
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body struct{ Value string `json:"value"` }
	if err := dec.Decode(&body); err != nil { http.Error(w, "bad request", http.StatusBadRequest); return }
	msg, err := n.e.Propose(body.Value)
	if err != nil { http.Error(w, err.Error(), http.StatusBadRequest); return }
	// metrics omitted
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msg)
}

func (n *Node) handleState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
	json.NewEncoder(w).Encode(map[string]any{
		"id": n.id,
		"round": n.e.Round(),
		"state": n.e.State(),
		"value": n.e.Value(),
	})
}

// handleProposal receives the aggregated payload from the leader, verifies its signature,
// and stores it under a deterministic hash so IBFT can subsequently propose/commit it.
func (n *Node) handleProposal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
	// Peer auth
	n.mu.RLock()
	token := n.peerToken
	n.mu.RUnlock()
	if token != "" {
		if ah := r.Header.Get("Authorization"); ah != "Bearer "+token { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
	}
	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" { http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType); return }
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var pr proposalReq
	if err := dec.Decode(&pr); err != nil { http.Error(w, "bad request", http.StatusBadRequest); return }
	// Verify leader signature over canonical bytes
	b := canonicalVoteBytes(pr.Data, pr.Items)
	ok, err := eth.VerifyPersonal(b, pr.LeaderSig, pr.LeaderWallet)
	if err != nil || !ok { http.Error(w, "invalid leader signature", http.StatusBadRequest); return }
	// Cache proposal by hash
	h := hashProposal(pr)
	n.mu.Lock()
	if n.proposals == nil { n.proposals = make(map[string]proposalReq) }
	n.proposals[h] = pr
	n.mu.Unlock()
	w.WriteHeader(http.StatusAccepted)
}

func (n *Node) handleTxStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
	n.mu.RLock()
	resp := map[string]any{
		"id":               n.id,
		"isLeader":         n.isLeader,
		"pendingData":      n.txPendingData,
		"collected":        len(n.txSigs),
		"lastLaunchedAt":   n.lastTxLaunchedAt.Format(time.RFC3339Nano),
		"lastLaunchedData": n.lastTxLastData,
	}
	n.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// SetLeader configures the leader URL for tx aggregation and whether this node is the leader.
func (n *Node) SetLeader(url string, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.leaderURL = url
	n.isLeader = isLeader
}

// SetPrivateKey configures the hex-encoded Ethereum private key for signing data.
func (n *Node) SetPrivateKey(hexKey string) {
	n.mu.Lock()
	n.privKeyHex = hexKey
	// Also cache leader wallet if we are the leader
	if hexKey != "" {
		if priv, err := eth.ParsePrivateKey(hexKey); err == nil {
			n.leaderWallet = eth.AddressFromPrivate(priv)
		}
	}
	n.mu.Unlock()
}

type executeReq struct {
	Data string `json:"data"`
}

type collectReq struct {
	Data   string `json:"data"`
	Wallet string `json:"public_wallet"`
	Sig    string `json:"firma_content"`
}

// voteReq is sent by the leader to all peers to request a vote on the aggregate payload.
// The signature (LeaderSig) is the personal_sign over the canonical JSON of {Data, Items}.
type voteReq struct {
	Data         string       `json:"data"`
	Items        []collectReq `json:"items"`
	LeaderWallet string       `json:"leader_wallet"`
	LeaderSig    string       `json:"leader_sig"`
}

// canonicalVoteBytes returns the canonical bytes to sign/verify for vote payload.
func canonicalVoteBytes(data string, items []collectReq) []byte {
	// Ensure deterministic order by wallet, then signature
	cp := make([]collectReq, 0, len(items))
	cp = append(cp, items...)
	// sort
	sort.Slice(cp, func(i, j int) bool {
		wi := strings.ToLower(cp[i].Wallet)
		wj := strings.ToLower(cp[j].Wallet)
		if wi == wj { return cp[i].Sig < cp[j].Sig }
		return wi < wj
	})
	payload := struct {
		Data  string       `json:"data"`
		Items []collectReq `json:"items"`
	}{Data: data, Items: cp}
	b, _ := json.Marshal(payload)
	return b
}

// proposalReq mirrors voteReq; it's the payload distributed to peers before IBFT consensus.
type proposalReq struct {
	Data         string       `json:"data"`
	Items        []collectReq `json:"items"`
	LeaderWallet string       `json:"leader_wallet"`
	LeaderSig    string       `json:"leader_sig"`
}

func hashProposal(p proposalReq) string {
	b := canonicalVoteBytes(p.Data, p.Items)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func (n *Node) handleExecuteTx(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" { http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType); return }
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req executeReq
	if err := dec.Decode(&req); err != nil { http.Error(w, "bad request", http.StatusBadRequest); return }

	n.mu.RLock()
	leader := n.leaderURL
	isLeader := n.isLeader
	keyHex := n.privKeyHex
	token := n.peerToken
	peers := append([]string(nil), n.peers...)
	n.mu.RUnlock()

	if req.Data == "" { http.Error(w, "missing data", http.StatusBadRequest); return }

	if isLeader {
		// Leader signs the data and submits to its own collect endpoint, then broadcasts to peers.
		if keyHex == "" || leader == "" {
			http.Error(w, "leader missing ETH key or leader url", http.StatusFailedDependency)
			return
		}
		priv, err := eth.ParsePrivateKey(keyHex)
		if err != nil { http.Error(w, "invalid private key", http.StatusBadRequest); return }
		sigHex, addr, err := eth.SignPersonal([]byte(req.Data), priv)
		if err != nil { http.Error(w, "sign error", http.StatusInternalServerError); return }

		payload := collectReq{Data: req.Data, Wallet: addr, Sig: sigHex}
		bb, _ := json.Marshal(payload)
		url := leader + "/v1/tx/collect"
		httpClient := &http.Client{Timeout: 3 * time.Second}
		fwd, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(bb))
		fwd.Header.Set("Content-Type", "application/json")
		if token != "" { fwd.Header.Set("Authorization", "Bearer "+token) }
		resp, err := httpClient.Do(fwd)
		if err != nil { http.Error(w, "forward error", http.StatusBadGateway); return }
		io.Copy(io.Discard, resp.Body); resp.Body.Close()
		// Broadcast to peers to have them sign and forward
		if r.Header.Get("X-IBFS-Forwarded") == "" {
			for _, p := range peers {
				go func(base string, data string) {
					jb, _ := json.Marshal(executeReq{Data: data})
					reqPeer, _ := http.NewRequest(http.MethodPost, base+"/v1/tx/execute-transaction", bytes.NewReader(jb))
					reqPeer.Header.Set("Content-Type", "application/json")
					reqPeer.Header.Set("X-IBFS-Forwarded", "1")
					if token != "" { reqPeer.Header.Set("Authorization", "Bearer "+token) }
					cli := &http.Client{Timeout: 1500 * time.Millisecond}
					resp, err := cli.Do(reqPeer)
					if err != nil {
						if n.log != nil { n.log.Warn("peer execute broadcast failed", "peer", base, "err", err) }
						return
					}
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}(p, req.Data)
			}
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}

	if keyHex == "" || leader == "" {
		http.Error(w, "node missing ETH key or leader url", http.StatusFailedDependency)
		return
	}
	priv, err := eth.ParsePrivateKey(keyHex)
	if err != nil { http.Error(w, "invalid private key", http.StatusBadRequest); return }
	sigHex, addr, err := eth.SignPersonal([]byte(req.Data), priv)
	if err != nil { http.Error(w, "sign error", http.StatusInternalServerError); return }

	// forward to leader /collect
	payload := collectReq{Data: req.Data, Wallet: addr, Sig: sigHex}
	bb, _ := json.Marshal(payload)
	url := leader + "/v1/tx/collect"
	httpClient := &http.Client{Timeout: 3 * time.Second}
	fwd, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(bb))
	fwd.Header.Set("Content-Type", "application/json")
	if token != "" { fwd.Header.Set("Authorization", "Bearer "+token) }
	resp, err := httpClient.Do(fwd)
	if err != nil { http.Error(w, "forward error", http.StatusBadGateway); return }
	io.Copy(io.Discard, resp.Body); resp.Body.Close()

	// If this request did not originate from a peer broadcast, propagate to peers so they also sign and forward.
	// Prevent broadcast loops using a hop marker header.
	if r.Header.Get("X-IBFS-Forwarded") == "" {
		for _, p := range peers {
			// fire-and-forget best-effort broadcast
			go func(base string, data string) {
				jb, _ := json.Marshal(executeReq{Data: data})
				reqPeer, _ := http.NewRequest(http.MethodPost, base+"/v1/tx/execute-transaction", bytes.NewReader(jb))
				reqPeer.Header.Set("Content-Type", "application/json")
				reqPeer.Header.Set("X-IBFS-Forwarded", "1")
				if token != "" { reqPeer.Header.Set("Authorization", "Bearer "+token) }
				// short timeout per peer to avoid hanging
				cli := &http.Client{Timeout: 1500 * time.Millisecond}
				resp, err := cli.Do(reqPeer)
				if err != nil {
					if n.log != nil { n.log.Warn("peer execute broadcast failed", "peer", base, "err", err) }
					return
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}(p, req.Data)
		}
	}
	w.WriteHeader(http.StatusAccepted)
}

func (n *Node) handleCollect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
	// Peer auth
	n.mu.RLock()
	token := n.peerToken
	n.mu.RUnlock()
	if token != "" {
		if ah := r.Header.Get("Authorization"); ah != "Bearer "+token { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
	}
	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" { http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType); return }
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req collectReq
	if err := dec.Decode(&req); err != nil { http.Error(w, "bad request", http.StatusBadRequest); return }

	// verify signature
	ok, err := eth.VerifyPersonal([]byte(req.Data), req.Sig, req.Wallet)
	if err != nil || !ok { http.Error(w, "invalid signature", http.StatusBadRequest); return }

	// aggregate
	n.mu.Lock()
	if !n.isLeader { n.mu.Unlock(); http.Error(w, "not a leader", http.StatusBadRequest); return }
	if n.txSigs == nil { n.txSigs = make(map[string]string) }
	if n.txItems == nil { n.txItems = make(map[string]collectReq) }
	if n.txPendingData == "" {
		n.txPendingData = req.Data
		// Leader self-sign on first collect to contribute to quorum if it has a key configured
		if n.privKeyHex != "" {
			if priv, err := eth.ParsePrivateKey(n.privKeyHex); err == nil {
				if sigHex, addr, err := eth.SignPersonal([]byte(req.Data), priv); err == nil {
					n.txSigs[strings.ToLower(addr)] = sigHex
					n.txItems[strings.ToLower(addr)] = collectReq{Data: req.Data, Wallet: addr, Sig: sigHex}
				} else if n.log != nil {
					n.log.Warn("leader self-sign failed", "err", err)
				}
			} else if n.log != nil {
				n.log.Warn("leader private key parse failed", "err", err)
			}
		}
	} else if n.txPendingData != req.Data {
		n.mu.Unlock(); http.Error(w, "different data in progress", http.StatusConflict); return
	}
	// record signature by wallet
	lw := strings.ToLower(req.Wallet)
	n.txSigs[lw] = req.Sig
	n.txItems[lw] = collectReq{Data: req.Data, Wallet: req.Wallet, Sig: req.Sig}
	// compute threshold 2/3 + 1
	total := n.e.ValidatorCount()
	threshold := (2*total)/3 + 1
	count := len(n.txSigs)
	// snapshot for potential voting
	data := n.txPendingData
	// build items slice now for signing and sending
	items := make([]collectReq, 0, len(n.txItems))
	for _, it := range n.txItems { items = append(items, it) }
	keyHex := n.privKeyHex
	peers := append([]string(nil), n.peers...)
	// token reused from earlier auth capture at function start
	leaderWallet := n.leaderWallet
	// Only trigger vote once
	shouldStartVote := !n.txVoting && count >= threshold
	if shouldStartVote { n.txVoting = true }
	n.mu.Unlock()

	if shouldStartVote {
		// Convert to IBFT: sign aggregate, distribute proposal, and propose hash
		if keyHex == "" {
			// Degradación: si el líder no tiene clave ETH configurada,
			// no podemos firmar ni distribuir el proposal. Responder 202
			// y dejar que el operador configure la clave para futuras rondas.
			if n.log != nil { n.log.Warn("threshold reached but leader has no ETH key; skipping IBFT proposal") }
			w.WriteHeader(http.StatusAccepted)
			return
		}
		priv, err := eth.ParsePrivateKey(keyHex)
		if err != nil { http.Error(w, "invalid leader private key", http.StatusBadRequest); return }
		payloadBytes := canonicalVoteBytes(data, items)
		sigHex, addr, err := eth.SignPersonal(payloadBytes, priv)
		if err != nil { http.Error(w, "leader sign error", http.StatusInternalServerError); return }
		if leaderWallet == "" { leaderWallet = addr }
		prop := proposalReq{Data: data, Items: items, LeaderWallet: leaderWallet, LeaderSig: sigHex}
		body, _ := json.Marshal(prop)
		// cache locally and compute hash
		h := hashProposal(prop)
		n.mu.Lock()
		if n.proposals == nil { n.proposals = make(map[string]proposalReq) }
		n.proposals[h] = prop
		n.pendingProposalHash = h
		n.mu.Unlock()
		// broadcast proposal to peers
		cli := &http.Client{Timeout: 1500 * time.Millisecond}
		for _, p := range peers {
			reqp, _ := http.NewRequest(http.MethodPost, p+"/v1/tx/proposal", bytes.NewReader(body))
			reqp.Header.Set("Content-Type", "application/json")
			if token != "" { reqp.Header.Set("Authorization", "Bearer "+token) }
			resp, err := cli.Do(reqp)
			if err == nil { io.Copy(io.Discard, resp.Body); resp.Body.Close() } else if n.log != nil { n.log.Warn("proposal broadcast failed", "peer", p, "err", err) }
		}
		// trigger IBFT propose with hash value
		if _, err := n.e.Propose(h); err != nil {
			if n.log != nil { n.log.Error("IBFT propose failed", "err", err) }
			http.Error(w, "ibft propose failed", http.StatusInternalServerError); return
		}
		// respond Accepted while IBFT runs; clients can poll /v1/tx/status
		w.WriteHeader(http.StatusAccepted)
		return
	}
	// Not at threshold yet
	w.WriteHeader(http.StatusAccepted)
}

// handleVote processes a leader's vote request; verifies the signature and returns affirmative on success.
func (n *Node) handleVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
	// Peer auth
	n.mu.RLock()
	token := n.peerToken
	n.mu.RUnlock()
	if token != "" {
		if ah := r.Header.Get("Authorization"); ah != "Bearer "+token { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
	}
	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" { http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType); return }
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req voteReq
	if err := dec.Decode(&req); err != nil { http.Error(w, "bad request", http.StatusBadRequest); return }
	// Verify leader signature over canonical bytes
	b := canonicalVoteBytes(req.Data, req.Items)
	ok, err := eth.VerifyPersonal(b, req.LeaderSig, req.LeaderWallet)
	if err != nil || !ok { http.Error(w, "invalid leader signature", http.StatusBadRequest); return }
	// Vote affirmative
	w.WriteHeader(http.StatusOK)
}

