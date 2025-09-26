package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
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
	isLeader bool
	// in-memory aggregation for current tx
	txPendingData string
	txSigs map[string]string // addr -> sigHex
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
	mux.HandleFunc("/v1/tx/status", n.handleTxStatus)
	// Backwards-compatible aliases without versioned prefix
	mux.HandleFunc("/execute-transaction", n.handleExecuteTx)
	mux.HandleFunc("/collect", n.handleCollect)
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
	state, out, err := n.e.Handle(msg)
	if err != nil { http.Error(w, fmt.Sprintf("handle error: %v", err), http.StatusBadRequest); return }
	if out != nil {
		// local broadcast already happened through broadcaster; nothing to do
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
func (n *Node) SetPrivateKey(hexKey string) { n.mu.Lock(); n.privKeyHex = hexKey; n.mu.Unlock() }

type executeReq struct {
	Data string `json:"data"`
}

type collectReq struct {
	Data   string `json:"data"`
	Wallet string `json:"public_wallet"`
	Sig    string `json:"firma_content"`
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
	defer n.mu.Unlock()
	if !n.isLeader { http.Error(w, "not a leader", http.StatusBadRequest); return }
	if n.txSigs == nil { n.txSigs = make(map[string]string) }
	if n.txPendingData == "" {
		n.txPendingData = req.Data
		// Leader self-sign on first collect to contribute to quorum if it has a key configured
		if n.privKeyHex != "" {
			if priv, err := eth.ParsePrivateKey(n.privKeyHex); err == nil {
				if sigHex, addr, err := eth.SignPersonal([]byte(req.Data), priv); err == nil {
					n.txSigs[strings.ToLower(addr)] = sigHex
				} else if n.log != nil {
					n.log.Warn("leader self-sign failed", "err", err)
				}
			} else if n.log != nil {
				n.log.Warn("leader private key parse failed", "err", err)
			}
		}
	} else if n.txPendingData != req.Data {
		http.Error(w, "different data in progress", http.StatusConflict); return
	}
	// record signature by wallet
	n.txSigs[strings.ToLower(req.Wallet)] = req.Sig
	// compute threshold 2/3 + 1
	total := n.e.ValidatorCount()
	threshold := (2*total)/3 + 1
	if len(n.txSigs) >= threshold {
		if n.log != nil { n.log.Info("TRANSACCIÓN LANZADA", "signatures", len(n.txSigs), "threshold", threshold) }
		// record observability
		n.lastTxLaunchedAt = time.Now()
		n.lastTxLastData = n.txPendingData
		// reset aggregation
		n.txSigs = make(map[string]string)
		n.txPendingData = ""
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("TRANSACCIÓN LANZADA"))
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

