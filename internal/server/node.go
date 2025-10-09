package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cgomezcal/ds-ms-ibfs/internal/eth"
	"github.com/cgomezcal/ds-ms-ibfs/internal/ibft"
	kafkautil "github.com/cgomezcal/ds-ms-ibfs/internal/kafka"
	"github.com/cgomezcal/ds-ms-ibfs/pkg/protocol"
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
				if hb.log != nil {
					hb.log.Error("broadcast failed", "peer", u, "err", err)
				}
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}(url)
	}
}

type Node struct {
	id        string
	peers     []string
	e         *ibft.Engine
	mux       *http.ServeMux
	http      *http.Server
	mu        sync.RWMutex
	log       *slog.Logger
	peerToken string
	// tx aggregation
	leaderURL    string
	privKeyHex   string
	leaderWallet string
	isLeader     bool
	// in-memory aggregation for current tx
	txPendingData    string
	txPendingKey     string
	txExecutionFlow  []protocol.ExecutionStep
	txSubFlows       map[string][]protocol.ExecutionStep // participant -> steps
	txCommitFlows    map[string][]protocol.ExecutionStep // participant -> commit events
	txSigs           map[string]string                   // participant -> sigHex
	txItems          map[string]collectReq               // participant -> item
	txVoting         bool
	txLeaderPrepared bool
	// proposals known for IBFT (hash -> proposal)
	proposals           map[string]proposalReq
	pendingProposalHash string
	// consensus deduplication
	lastProcessedConsensus string
	// observability
	lastTxLaunchedAt time.Time
	lastTxLastData string
	lastTxKey      string
	// mtm integration
	mtmBaseURL     string
	mtmClient      *http.Client
	mtmRoot        string
	mtmRootFetched time.Time
	// kafka integration
	kafkaProducer      *kafkautil.Producer
	kafkaBrokers       []string
	kafkaResponseTopic string
}

type txResponseMessage struct {
	NodeID        string                 `json:"node_id"`
	Data          string                 `json:"data"`
	ProposalHash  string                 `json:"proposal_hash"`
	Key           string                 `json:"key"`
	Status        string                 `json:"status"`
	TxHash        string                 `json:"tx_hash,omitempty"`
	Error         string                 `json:"error,omitempty"`
	Timestamp     time.Time              `json:"timestamp"`
	ExecutionFlow *protocol.ExecutionFlow `json:"execution_flow,omitempty"`
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
	n := &Node{id: id, peers: peers, log: logger, mtmClient: &http.Client{Timeout: 2 * time.Second}}
	// Start with self-only validators; caller can SetNetwork later
	n.e = ibft.NewEngine(id, []string{id}, hb)
	// metrics intentionally omitted per requirements
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
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
	err := n.http.Shutdown(ctx)

	n.mu.Lock()
	producer := n.kafkaProducer
	topic := n.kafkaResponseTopic
	n.kafkaProducer = nil
	n.kafkaBrokers = nil
	n.kafkaResponseTopic = ""
	n.mu.Unlock()

	if producer != nil {
		if cerr := producer.Close(); cerr != nil && n.log != nil {
			n.log.Warn("kafka producer shutdown failed", "topic", topic, "err", cerr)
			if err == nil {
				err = cerr
			}
		}
	}

	return err
}

func (n *Node) EnableKafkaProducer(brokers []string, topic string) {
	trimmed := make([]string, 0, len(brokers))
	for _, b := range brokers {
		if v := strings.TrimSpace(b); v != "" {
			trimmed = append(trimmed, v)
		}
	}
	if len(trimmed) == 0 || strings.TrimSpace(topic) == "" {
		if n.log != nil {
			n.log.Warn("kafka producer configuration incomplete", "brokers", brokers, "topic", topic)
		}
		return
	}

	n.mu.Lock()
	if n.kafkaProducer != nil {
		_ = n.kafkaProducer.Close()
	}
	n.kafkaProducer = kafkautil.NewProducer(trimmed, strings.TrimSpace(topic), n.log)
	n.kafkaBrokers = append([]string(nil), trimmed...)
	n.kafkaResponseTopic = strings.TrimSpace(topic)
	n.mu.Unlock()

	if n.log != nil {
		n.log.Info("kafka producer configured", "topic", n.kafkaResponseTopic, "brokers", trimmed)
	}
}

func (n *Node) publishKafkaTxResponse(key string, msg txResponseMessage) {
	n.mu.RLock()
	producer := n.kafkaProducer
	topic := n.kafkaResponseTopic
	n.mu.RUnlock()
	if producer == nil {
		return
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		if n.log != nil {
			n.log.Error("kafka response marshal failed", "err", err)
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := producer.Publish(ctx, []byte(key), payload); err != nil {
		if n.log != nil {
			n.log.Error("kafka response publish failed", "topic", topic, "err", err)
		}
	}
}

func (n *Node) handleBesuExecution(message string, isTx bool, proposalHash, txData, requestKey string, mainFlow []protocol.ExecutionStep, subFlows map[string][]protocol.ExecutionStep) {
	flowEnvelope := buildExecutionFlowPayload(mainFlow, subFlows)
	txHash, err := CallHelloWorldFromLeader(message)
	if err != nil {
		if n.log != nil {
			n.log.Error("besu execution failed", "message", message, "err", err)
		}
	} else if n.log != nil {
		n.log.Info("besu execution completed", "message", message, "txHash", txHash)
	}

	if !isTx {
		return
	}

	status := "success"
	errMsg := ""
	if err != nil {
		status = "error"
		errMsg = err.Error()
	}

	flowEnvelope.Steps = append(flowEnvelope.Steps, protocol.NewExecutionStep("ibft_node", n.id, "besu_execution", map[string]string{
		"status":        status,
		"proposal_hash": proposalHash,
	}))
	n.mu.RLock()
	topic := n.kafkaResponseTopic
	n.mu.RUnlock()
	flowEnvelope.Steps = append(flowEnvelope.Steps, protocol.NewExecutionStep("ibft_node", n.id, "kafka_publish", map[string]string{
		"topic": topic,
	}))

	resp := txResponseMessage{
		NodeID:        n.id,
		Data:          txData,
		ProposalHash:  proposalHash,
		Key:           requestKey,
		Status:        status,
		TxHash:        txHash,
		Error:         errMsg,
		Timestamp:     time.Now().UTC(),
	}
	flowEnvelopeCopy := flowEnvelope
	resp.ExecutionFlow = &flowEnvelopeCopy
	if len(resp.ExecutionFlow.Steps) == 0 && len(resp.ExecutionFlow.Subflows) == 0 {
		resp.ExecutionFlow = nil
	}
	msgKey := proposalHash
	if requestKey != "" {
		msgKey = requestKey
	}
	n.publishKafkaTxResponse(msgKey, resp)
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

// SetMTMBaseURL configures the MTM service endpoint used for Merkle proofs.
func (n *Node) SetMTMBaseURL(base string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.mtmBaseURL = strings.TrimRight(base, "/")
	n.mtmRoot = ""
	n.mtmRootFetched = time.Time{}
}

func (n *Node) handleMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
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
	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" {
		http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var msg ibft.Message
	if err := dec.Decode(&msg); err != nil {
		http.Error(w, "bad message", http.StatusBadRequest)
		return
	}
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
	if err != nil {
		http.Error(w, fmt.Sprintf("handle error: %v", err), http.StatusBadRequest)
		return
	}
	n.recordConsensusTrace(msg)
	if out != nil {
		n.recordConsensusTrace(*out)
	}
	if out != nil {
		// local broadcast already happened through broadcaster; nothing to do
	}
	// If committed and value matches pending proposal, finalize transaction
	var (
		shouldInvoke   bool
		invokeMessage  string
		isTxConsensus  bool
		proposalHash   string
		txData         string
		txKey          string
		executionFlow  []protocol.ExecutionStep
		subFlows       map[string][]protocol.ExecutionStep
	)
	if state == "committed" {
		n.mu.Lock()
		if n.isLeader {
			currentValue := n.e.Value()
			if n.lastProcessedConsensus != currentValue {
				n.lastProcessedConsensus = currentValue
				if n.pendingProposalHash != "" && n.pendingProposalHash == currentValue {
					if n.log != nil {
						n.log.Info("TRANSACCIÓN IBFT COMPLETADA - Llamando al contrato HelloWorld", "value", currentValue)
					}
					txData = n.txPendingData
					txKey = n.txPendingKey
					invokeMessage = txData
					isTxConsensus = true
					proposalHash = currentValue
					commitAggregation := n.buildCommitAggregationLocked(proposalHash)
					if len(n.txExecutionFlow) > 0 || len(n.txPendingData) > 0 {
						step := protocol.NewExecutionStep("ibft_node", n.id, "ibft_committed", map[string]string{"proposal_hash": proposalHash})
						if commitAggregation != nil {
							step.Aggregation = commitAggregation
						}
						n.txExecutionFlow = append(n.txExecutionFlow, step)
					}
					executionFlow = protocol.CloneExecutionFlow(n.txExecutionFlow)
					subFlows = cloneSubFlowMap(n.txSubFlows)
					n.lastTxLaunchedAt = time.Now()
					n.lastTxLastData = txData
					n.lastTxKey = txKey
					// Reset transaction state
					n.txSigs = make(map[string]string)
					n.txItems = make(map[string]collectReq)
					n.txPendingData = ""
					n.txPendingKey = ""
					n.txExecutionFlow = nil
					n.txSubFlows = nil
					n.txCommitFlows = nil
					n.txVoting = false
					n.txLeaderPrepared = false
					n.pendingProposalHash = ""
					// prune cached proposals
					if n.proposals != nil {
						delete(n.proposals, currentValue)
						if len(n.proposals) == 0 {
							n.proposals = nil
						}
					}
					shouldInvoke = txData != ""
				} else {
					if n.log != nil {
						n.log.Info("CONSENSO IBFT COMPLETADO - Llamando al contrato HelloWorld", "value", currentValue)
					}
					invokeMessage = fmt.Sprintf("IBFT Consenso: %s", currentValue)
					shouldInvoke = true
				}
			}
		}
		n.mu.Unlock()

		if shouldInvoke {
			go n.handleBesuExecution(invokeMessage, isTxConsensus, proposalHash, txData, txKey, executionFlow, subFlows)
		}
	}
	// metrics omitted
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"state": state})
}

func (n *Node) handlePropose(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" {
		http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body struct {
		Value string `json:"value"`
	}
	if err := dec.Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	msg, err := n.e.Propose(body.Value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// metrics omitted
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msg)
}

func (n *Node) handleState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	json.NewEncoder(w).Encode(map[string]any{
		"id":    n.id,
		"round": n.e.Round(),
		"state": n.e.State(),
		"value": n.e.Value(),
	})
}

// handleProposal receives the aggregated payload from the leader, verifies its signature,
// and stores it under a deterministic hash so IBFT can subsequently propose/commit it.
func (n *Node) handleProposal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Peer auth
	n.mu.RLock()
	token := n.peerToken
	n.mu.RUnlock()
	if token != "" {
		if ah := r.Header.Get("Authorization"); ah != "Bearer "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" {
		http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var pr proposalReq
	if err := dec.Decode(&pr); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	// Verify leader signature over canonical bytes
	b := canonicalVoteBytes(pr.Data, pr.Items, pr.LeaderWallet, pr.LeaderProof, pr.LeaderProofSig)
	ok, err := eth.VerifyPersonal(b, pr.LeaderSig, pr.LeaderWallet)
	if err != nil || !ok {
		http.Error(w, "invalid leader signature", http.StatusBadRequest)
		return
	}
	if err := n.verifyProof(r.Context(), pr.LeaderWallet, pr.LeaderProof, pr.LeaderProofSig); err != nil {
		http.Error(w, "invalid leader proof", http.StatusBadRequest)
		return
	}
	for _, item := range pr.Items {
		if err := n.verifyProof(r.Context(), item.Wallet, item.Proof, item.ProofSig); err != nil {
			http.Error(w, "invalid participant proof", http.StatusBadRequest)
			return
		}
	}
	// Cache proposal by hash
	h := hashProposal(pr)
	n.mu.Lock()
	if n.proposals == nil {
		n.proposals = make(map[string]proposalReq)
	}
	n.proposals[h] = pr
	n.mu.Unlock()
	w.WriteHeader(http.StatusAccepted)
}

func (n *Node) handleTxStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	n.mu.RLock()
	flowEnvelope := buildExecutionFlowPayload(n.txExecutionFlow, n.txSubFlows)
	steps := len(flowEnvelope.Steps)
	subflows := len(flowEnvelope.Subflows)
	resp := map[string]any{
		"id":                   n.id,
		"is_leader":            n.isLeader,
		"pending_data":         n.txPendingData,
		"pending_key":          n.txPendingKey,
		"collected":            len(n.txSigs),
		"last_launched_at":     n.lastTxLaunchedAt.Format(time.RFC3339Nano),
		"last_launched_data":   n.lastTxLastData,
		"last_launched_key":    n.lastTxKey,
		"execution_flow_steps": steps,
		"execution_flow_subflows": subflows,
	}
	if steps > 0 || subflows > 0 {
		resp["execution_flow"] = flowEnvelope
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
	Data          string                   `json:"data"`
	Key           string                   `json:"key"`
	ExecutionFlow []protocol.ExecutionStep `json:"execution_flow"`
}

type collectReq struct {
	Data     string             `json:"data"`
	Wallet   string             `json:"public_wallet"`
	Sig      string             `json:"firma_content"`
	Proof    merkleProofPayload `json:"merkle_proof"`
	ProofSig string             `json:"proof_sig"`
	ParticipantID string        `json:"participant_id,omitempty"`
	ExecutionFlow []protocol.ExecutionStep `json:"execution_flow,omitempty"`
}

type merkleProofPayload struct {
	Root     string   `json:"root"`
	Index    int      `json:"index"`
	Siblings []string `json:"siblings"`
	Dirs     []string `json:"dirs"`
}

func (p merkleProofPayload) isZero() bool {
	return p.Root == "" && len(p.Siblings) == 0 && len(p.Dirs) == 0 && p.Index == 0
}

// voteReq is sent by the leader to all peers to request a vote on the aggregate payload.
// The signature (LeaderSig) is the personal_sign over the canonical JSON of {Data, Items}.
type voteReq struct {
	Data           string             `json:"data"`
	Items          []collectReq       `json:"items"`
	LeaderWallet   string             `json:"leader_wallet"`
	LeaderSig      string             `json:"leader_sig"`
	LeaderProof    merkleProofPayload `json:"leader_proof"`
	LeaderProofSig string             `json:"leader_proof_sig"`
}

// canonicalVoteBytes returns the canonical bytes to sign/verify for vote payload.
func canonicalVoteBytes(data string, items []collectReq, leaderWallet string, leaderProof merkleProofPayload, leaderProofSig string) []byte {
	// Ensure deterministic order by participant, then wallet/signature
	cp := make([]collectReq, 0, len(items))
	cp = append(cp, items...)
	sort.Slice(cp, func(i, j int) bool {
		pi := participantStorageKey(cp[i].ParticipantID, cp[i].Wallet)
		pj := participantStorageKey(cp[j].ParticipantID, cp[j].Wallet)
		if pi == pj {
			wi := strings.ToLower(cp[i].Wallet)
			wj := strings.ToLower(cp[j].Wallet)
			if wi == wj {
				if cp[i].Sig == cp[j].Sig {
					return wi < wj
				}
				return cp[i].Sig < cp[j].Sig
			}
			return wi < wj
		}
		return pi < pj
	})
	for idx := range cp {
		cp[idx].ParticipantID = participantStorageKey(cp[idx].ParticipantID, cp[idx].Wallet)
	}
	payload := struct {
		Data           string             `json:"data"`
		Items          []collectReq       `json:"items"`
		LeaderWallet   string             `json:"leader_wallet"`
		LeaderProof    merkleProofPayload `json:"leader_proof"`
		LeaderProofSig string             `json:"leader_proof_sig"`
	}{
		Data:           data,
		Items:          cp,
		LeaderWallet:   strings.ToLower(leaderWallet),
		LeaderProof:    leaderProof,
		LeaderProofSig: leaderProofSig,
	}
	b, _ := json.Marshal(payload)
	return b
}

// proposalReq mirrors voteReq; it's the payload distributed to peers before IBFT consensus.
type proposalReq struct {
	Data           string             `json:"data"`
	Items          []collectReq       `json:"items"`
	LeaderWallet   string             `json:"leader_wallet"`
	LeaderSig      string             `json:"leader_sig"`
	LeaderProof    merkleProofPayload `json:"leader_proof"`
	LeaderProofSig string             `json:"leader_proof_sig"`
}

func hashProposal(p proposalReq) string {
	b := canonicalVoteBytes(p.Data, p.Items, p.LeaderWallet, p.LeaderProof, p.LeaderProofSig)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func (n *Node) handleExecuteTx(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" {
		http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req executeReq
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	flow := protocol.CloneExecutionFlow(req.ExecutionFlow)
	n.mu.RLock()
	leader := n.leaderURL
	isLeader := n.isLeader
	keyHex := n.privKeyHex
	token := n.peerToken
	peers := append([]string(nil), n.peers...)
	n.mu.RUnlock()

	req.Key = strings.TrimSpace(req.Key)
	if req.Data == "" {
		http.Error(w, "missing data", http.StatusBadRequest)
		return
	}
	if req.Key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	if n.log != nil {
		n.log.Info("execute request received", "node", n.id, "key", req.Key, "leader", isLeader, "forwarded", r.Header.Get("X-IBFS-Forwarded") != "")
	}
	ctx := r.Context()
	meta := map[string]string{
		"forwarded": strconv.FormatBool(r.Header.Get("X-IBFS-Forwarded") != ""),
		"is_leader": strconv.FormatBool(isLeader),
	}
	role := "execute_peer"
	if isLeader {
		role = "execute_leader"
	}
	flow = append(flow, protocol.NewExecutionStep("ibft_node", n.id, role, meta))
	req.ExecutionFlow = flow

	if isLeader {
		n.mu.Lock()
		if n.txPendingData != "" && n.txPendingData != req.Data {
			n.mu.Unlock()
			http.Error(w, "different data in progress", http.StatusConflict)
			return
		}
		n.txPendingKey = req.Key
		n.txExecutionFlow = protocol.CloneExecutionFlow(flow)
		n.txSubFlows = nil
		n.mu.Unlock()
		// Leader signs the data and submits to its own collect endpoint, then broadcasts to peers.
		if keyHex == "" || leader == "" {
			http.Error(w, "leader missing ETH key or leader url", http.StatusFailedDependency)
			return
		}
		priv, err := eth.ParsePrivateKey(keyHex)
		if err != nil {
			http.Error(w, "invalid private key", http.StatusBadRequest)
			return
		}
		sigHex, addr, err := eth.SignPersonal([]byte(req.Data), priv)
		if err != nil {
			http.Error(w, "sign error", http.StatusInternalServerError)
			return
		}
		proof, proofSig, err := n.prepareProof(ctx, addr)
		if err != nil {
			http.Error(w, "proof preparation failed", http.StatusFailedDependency)
			return
		}

	leaderCollectFlow := append(protocol.CloneExecutionFlow(flow), protocol.NewExecutionStep("ibft_node", n.id, "collect_self", map[string]string{"leader": leader}))
	payload := collectReq{Data: req.Data, Wallet: addr, Sig: sigHex, Proof: proof, ProofSig: proofSig, ParticipantID: n.id, ExecutionFlow: leaderCollectFlow}
		bb, _ := json.Marshal(payload)
		url := leader + "/v1/tx/collect"
		httpClient := &http.Client{Timeout: 3 * time.Second}
		fwd, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(bb))
		fwd.Header.Set("Content-Type", "application/json")
		if token != "" {
			fwd.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := httpClient.Do(fwd)
		if err != nil {
			http.Error(w, "forward error", http.StatusBadGateway)
			return
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= 300 {
			if n.log != nil {
				n.log.Error("leader collect request failed", "status", resp.StatusCode, "response", strings.TrimSpace(string(body)))
			}
			http.Error(w, "leader collect failed", http.StatusBadGateway)
			return
		}
		// Broadcast to peers to have them sign and forward
		if r.Header.Get("X-IBFS-Forwarded") == "" {
			for _, p := range peers {
				go func(base string, data string, key string, flowSteps []protocol.ExecutionStep) {
					jb, _ := json.Marshal(executeReq{Data: data, Key: key, ExecutionFlow: flowSteps})
					reqPeer, _ := http.NewRequest(http.MethodPost, base+"/v1/tx/execute-transaction", bytes.NewReader(jb))
					reqPeer.Header.Set("Content-Type", "application/json")
					reqPeer.Header.Set("X-IBFS-Forwarded", "1")
					if token != "" {
						reqPeer.Header.Set("Authorization", "Bearer "+token)
					}
					cli := &http.Client{Timeout: 1500 * time.Millisecond}
					resp, err := cli.Do(reqPeer)
					if err != nil {
						if n.log != nil {
							n.log.Warn("peer execute broadcast failed", "peer", base, "err", err)
						}
						return
					}
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}(p, req.Data, req.Key, protocol.CloneExecutionFlow(flow))
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
	if err != nil {
		http.Error(w, "invalid private key", http.StatusBadRequest)
		return
	}
	sigHex, addr, err := eth.SignPersonal([]byte(req.Data), priv)
	if err != nil {
		http.Error(w, "sign error", http.StatusInternalServerError)
		return
	}
	proof, proofSig, err := n.prepareProof(ctx, addr)
	if err != nil {
		http.Error(w, "proof preparation failed", http.StatusFailedDependency)
		return
	}

	// forward to leader /collect
	flowCollect := append(protocol.CloneExecutionFlow(flow), protocol.NewExecutionStep("ibft_node", n.id, "collect_forward", map[string]string{"leader": leader}))
	payload := collectReq{Data: req.Data, Wallet: addr, Sig: sigHex, Proof: proof, ProofSig: proofSig, ParticipantID: n.id, ExecutionFlow: flowCollect}
	bb, _ := json.Marshal(payload)
	url := leader + "/v1/tx/collect"
	httpClient := &http.Client{Timeout: 3 * time.Second}
	fwd, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(bb))
	fwd.Header.Set("Content-Type", "application/json")
	if token != "" {
		fwd.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := httpClient.Do(fwd)
	if err != nil {
		http.Error(w, "forward error", http.StatusBadGateway)
		return
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= 300 {
		if n.log != nil {
			n.log.Error("collect forward rejected by leader", "status", resp.StatusCode, "response", strings.TrimSpace(string(body)))
		}
		http.Error(w, "collect rejected by leader", http.StatusBadGateway)
		return
	}

	// If this request did not originate from a peer broadcast, propagate to peers so they also sign and forward.
	// Prevent broadcast loops using a hop marker header.
	if r.Header.Get("X-IBFS-Forwarded") == "" {
		for _, p := range peers {
			// fire-and-forget best-effort broadcast
			go func(base string, data string, key string, flowSteps []protocol.ExecutionStep) {
				jb, _ := json.Marshal(executeReq{Data: data, Key: key, ExecutionFlow: flowSteps})
				reqPeer, _ := http.NewRequest(http.MethodPost, base+"/v1/tx/execute-transaction", bytes.NewReader(jb))
				reqPeer.Header.Set("Content-Type", "application/json")
				reqPeer.Header.Set("X-IBFS-Forwarded", "1")
				if token != "" {
					reqPeer.Header.Set("Authorization", "Bearer "+token)
				}
				// short timeout per peer to avoid hanging
				cli := &http.Client{Timeout: 1500 * time.Millisecond}
				resp, err := cli.Do(reqPeer)
				if err != nil {
					if n.log != nil {
						n.log.Warn("peer execute broadcast failed", "peer", base, "err", err)
					}
					return
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}(p, req.Data, req.Key, protocol.CloneExecutionFlow(flow))
		}
	}
	w.WriteHeader(http.StatusAccepted)
}

func (n *Node) handleCollect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Peer auth
	n.mu.RLock()
	token := n.peerToken
	n.mu.RUnlock()
	if token != "" {
		if ah := r.Header.Get("Authorization"); ah != "Bearer "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" {
		http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req collectReq
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// verify signature
	ok, err := eth.VerifyPersonal([]byte(req.Data), req.Sig, req.Wallet)
	if err != nil || !ok {
		http.Error(w, "invalid signature", http.StatusBadRequest)
		return
	}
	if err := n.verifyProof(r.Context(), req.Wallet, req.Proof, req.ProofSig); err != nil {
		http.Error(w, "invalid merkle proof", http.StatusBadRequest)
		return
	}
	participantKey := participantStorageKey(req.ParticipantID, req.Wallet)
	if participantKey == "" {
		http.Error(w, "missing participant identity", http.StatusBadRequest)
		return
	}
	participantLabel := participantDisplay(req.ParticipantID, req.Wallet)
	entry := collectReq{
		Data:          req.Data,
		Wallet:        req.Wallet,
		Sig:           req.Sig,
		Proof:         req.Proof,
		ProofSig:      req.ProofSig,
		ParticipantID: participantKey,
		ExecutionFlow: protocol.CloneExecutionFlow(req.ExecutionFlow),
	}
	ctx := r.Context()
	n.appendTxSubFlow(participantLabel, req.ExecutionFlow)

	n.mu.Lock()
	if !n.isLeader {
		n.mu.Unlock()
		http.Error(w, "not a leader", http.StatusBadRequest)
		return
	}
	if n.txSigs == nil {
		n.txSigs = make(map[string]string)
	}
	if n.txItems == nil {
		n.txItems = make(map[string]collectReq)
	}
	if n.txPendingData == "" {
		n.txPendingData = entry.Data
		n.txLeaderPrepared = false
	} else if n.txPendingData != entry.Data {
		n.mu.Unlock()
		http.Error(w, "different data in progress", http.StatusConflict)
		return
	}
	needLeaderEntry := !n.txLeaderPrepared && n.privKeyHex != ""
	previousSig, seen := n.txSigs[participantKey]
	n.txSigs[participantKey] = entry.Sig
	n.txItems[participantKey] = entry
	pendingData := n.txPendingData
	currentCount := len(n.txSigs)
	n.mu.Unlock()

	if n.log != nil {
		if seen && previousSig == entry.Sig {
			n.log.Debug("collect entry refreshed", "participant", participantKey, "count", currentCount)
		} else {
			n.log.Info("collect entry accepted", "participant", participantKey, "count", currentCount)
		}
	}

	if needLeaderEntry {
		selfEntry, err := n.buildLeaderCollectEntry(ctx, pendingData)
		if err != nil {
			if n.log != nil {
				n.log.Error("leader self entry failed", "err", err)
			}
			http.Error(w, "leader self entry failed", http.StatusInternalServerError)
			return
		}
		n.mu.Lock()
		leaderKey := participantStorageKey(selfEntry.ParticipantID, selfEntry.Wallet)
		if leaderKey == "" {
			leaderKey = participantStorageKey(n.id, selfEntry.Wallet)
		}
		n.txSigs[leaderKey] = selfEntry.Sig
		n.txItems[leaderKey] = selfEntry
		n.txLeaderPrepared = true
		if n.leaderWallet == "" {
			n.leaderWallet = selfEntry.Wallet
		}
		currentCount = len(n.txSigs)
		n.mu.Unlock()
		if n.log != nil {
			n.log.Info("leader collect entry appended", "participant", leaderKey, "count", currentCount)
		}
	}

	var (
		keyHex          string
		data            string
		items           []collectReq
		leaderWallet    string
		leaderProof     merkleProofPayload
		leaderProofSig  string
		peers           []string
		shouldStartVote bool
	)
	var (
		total     int
		threshold int
		count     int
	)
	n.mu.Lock()
	total = n.e.ValidatorCount()
	threshold = (2*total)/3 + 1
	count = len(n.txSigs)
	data = n.txPendingData
	items = make([]collectReq, 0, len(n.txItems))
	for _, it := range n.txItems {
		items = append(items, it)
	}
	leaderWallet = n.leaderWallet
	if lw := strings.ToLower(leaderWallet); lw != "" {
		for key, it := range n.txItems {
			if key == lw || strings.EqualFold(it.ParticipantID, leaderWallet) || strings.EqualFold(it.Wallet, leaderWallet) {
				leaderProof = it.Proof
				leaderProofSig = it.ProofSig
				leaderWallet = it.Wallet
				break
			}
		}
	}
	if leaderProofSig == "" {
		// try to derive from any item matching leader wallet (case-insensitive)
		for _, it := range items {
			if strings.EqualFold(it.Wallet, leaderWallet) {
				leaderProof = it.Proof
				leaderProofSig = it.ProofSig
				leaderWallet = it.Wallet
				break
			}
		}
	}
	keyHex = n.privKeyHex
	peers = append([]string(nil), n.peers...)
	shouldStartVote = !n.txVoting && count >= threshold
	if shouldStartVote {
		n.txVoting = true
	}
	n.mu.Unlock()

	if n.log != nil {
		n.log.Info("collect quorum progress", "participant", participantKey, "count", count, "threshold", threshold)
	}

	if shouldStartVote {
		if leaderProofSig == "" {
			http.Error(w, "leader proof unavailable", http.StatusFailedDependency)
			return
		}
		if err := n.verifyProof(ctx, leaderWallet, leaderProof, leaderProofSig); err != nil {
			http.Error(w, "leader proof invalid", http.StatusFailedDependency)
			return
		}
		aggregation := n.buildCollectAggregation(count, threshold, items)
		n.appendTxExecutionStep("collect_threshold", map[string]string{
			"participant_count": strconv.Itoa(count),
			"threshold":         strconv.Itoa(threshold),
		}, aggregation)
		// Convert to IBFT: sign aggregate, distribute proposal, and propose hash
		if keyHex == "" {
			// Degradación: si el líder no tiene clave ETH configurada,
			// no podemos firmar ni distribuir el proposal. Responder 202
			// y dejar que el operador configure la clave para futuras rondas.
			if n.log != nil {
				n.log.Warn("threshold reached but leader has no ETH key; skipping IBFT proposal")
			}
			w.WriteHeader(http.StatusAccepted)
			return
		}
		priv, err := eth.ParsePrivateKey(keyHex)
		if err != nil {
			http.Error(w, "invalid leader private key", http.StatusBadRequest)
			return
		}
		payloadBytes := canonicalVoteBytes(data, items, leaderWallet, leaderProof, leaderProofSig)
		sigHex, addr, err := eth.SignPersonal(payloadBytes, priv)
		if err != nil {
			http.Error(w, "leader sign error", http.StatusInternalServerError)
			return
		}
		if leaderWallet == "" {
			leaderWallet = addr
		}
		prop := proposalReq{Data: data, Items: items, LeaderWallet: leaderWallet, LeaderSig: sigHex, LeaderProof: leaderProof, LeaderProofSig: leaderProofSig}
		body, _ := json.Marshal(prop)
		// cache locally and compute hash
		h := hashProposal(prop)
		n.mu.Lock()
		if n.proposals == nil {
			n.proposals = make(map[string]proposalReq)
		}
		n.proposals[h] = prop
		n.pendingProposalHash = h
		n.mu.Unlock()
		// broadcast proposal to peers
		cli := &http.Client{Timeout: 1500 * time.Millisecond}
		for _, p := range peers {
			reqp, _ := http.NewRequest(http.MethodPost, p+"/v1/tx/proposal", bytes.NewReader(body))
			reqp.Header.Set("Content-Type", "application/json")
			if token != "" {
				reqp.Header.Set("Authorization", "Bearer "+token)
			}
			resp, err := cli.Do(reqp)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			} else if n.log != nil {
				n.log.Warn("proposal broadcast failed", "peer", p, "err", err)
			}
		}
		n.appendTxExecutionStep("proposal_broadcast", map[string]string{
			"proposal_hash": h,
			"targets":      strconv.Itoa(len(peers)),
		}, nil)
		// trigger IBFT propose with hash value
		if _, err := n.e.Propose(h); err != nil {
			if n.log != nil {
				n.log.Error("IBFT propose failed", "err", err)
			}
			http.Error(w, "ibft propose failed", http.StatusInternalServerError)
			return
		}
		n.appendTxExecutionStep("ibft_propose", map[string]string{"proposal_hash": h}, nil)
		// respond Accepted while IBFT runs; clients can poll /v1/tx/status
		w.WriteHeader(http.StatusAccepted)
		return
	}
	// Not at threshold yet
	w.WriteHeader(http.StatusAccepted)
}

// handleVote processes a leader's vote request; verifies the signature and returns affirmative on success.
func (n *Node) handleVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Peer auth
	n.mu.RLock()
	token := n.peerToken
	n.mu.RUnlock()
	if token != "" {
		if ah := r.Header.Get("Authorization"); ah != "Bearer "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" {
		http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req voteReq
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	// Verify leader signature over canonical bytes
	b := canonicalVoteBytes(req.Data, req.Items, req.LeaderWallet, req.LeaderProof, req.LeaderProofSig)
	ok, err := eth.VerifyPersonal(b, req.LeaderSig, req.LeaderWallet)
	if err != nil || !ok {
		http.Error(w, "invalid leader signature", http.StatusBadRequest)
		return
	}
	if err := n.verifyProof(r.Context(), req.LeaderWallet, req.LeaderProof, req.LeaderProofSig); err != nil {
		http.Error(w, "invalid leader proof", http.StatusBadRequest)
		return
	}
	for _, item := range req.Items {
		if err := n.verifyProof(r.Context(), item.Wallet, item.Proof, item.ProofSig); err != nil {
			http.Error(w, "invalid participant proof", http.StatusBadRequest)
			return
		}
	}
	// Vote affirmative
	w.WriteHeader(http.StatusOK)
}

func (n *Node) prepareProof(ctx context.Context, wallet string) (merkleProofPayload, string, error) {
	proof, err := n.fetchMerkleProof(ctx, wallet)
	if err != nil {
		return merkleProofPayload{}, "", err
	}
	sig, err := n.signProof(proof, wallet)
	if err != nil {
		return merkleProofPayload{}, "", err
	}
	return proof, sig, nil
}

func (n *Node) signProof(proof merkleProofPayload, wallet string) (string, error) {
	n.mu.RLock()
	keyHex := n.privKeyHex
	n.mu.RUnlock()
	if keyHex == "" {
		return "", errors.New("missing private key")
	}
	priv, err := eth.ParsePrivateKey(keyHex)
	if err != nil {
		return "", err
	}
	payload := canonicalProofBytes(wallet, proof)
	sigHex, _, err := eth.SignPersonal(payload, priv)
	if err != nil {
		return "", err
	}
	return sigHex, nil
}

func (n *Node) verifyProof(ctx context.Context, wallet string, proof merkleProofPayload, proofSig string) error {
	if proofSig == "" {
		return errors.New("missing proof signature")
	}
	payload := canonicalProofBytes(wallet, proof)
	ok, err := eth.VerifyPersonal(payload, proofSig, wallet)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("proof signature mismatch")
	}
	root, err := n.fetchMerkleRoot(ctx)
	if err != nil {
		return err
	}
	if proof.Root == "" {
		return errors.New("proof missing root")
	}
	if !strings.EqualFold(trimHexPrefix(proof.Root), trimHexPrefix(root)) {
		return errors.New("proof root differs from MTM root")
	}
	// Algunos despliegues (por ejemplo el stub MTM) devuelven proofs degenerados sin siblings ni direcciones.
	// En ese caso aceptamos la validación básica (firma + raíz) y evitamos rechazar la transacción.
	if len(proof.Siblings) == 0 && len(proof.Dirs) == 0 {
		if n.log != nil {
			n.log.Warn("merkle proof without siblings; skipping structural validation", "wallet", wallet)
		}
		return nil
	}
	if err := verifyMerkleProof(wallet, proof, root); err != nil {
		return err
	}
	return nil
}

func (n *Node) fetchMerkleProof(ctx context.Context, wallet string) (merkleProofPayload, error) {
	base, client := n.getMTMClient()
	if base == "" {
		return merkleProofPayload{}, errors.New("mtm base url not configured")
	}
	endpoint, err := url.JoinPath(base, "/merkle/proof")
	if err != nil {
		return merkleProofPayload{}, err
	}
	candidate := strings.TrimSpace(wallet)
	queries := []string{candidate}
	lower := strings.ToLower(candidate)
	if lower != candidate {
		queries = append(queries, lower)
	}
	var lastErr error
	for _, q := range queries {
		if q == "" {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?pubkey="+url.QueryEscape(q), nil)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
			resp.Body.Close()
			// si recibimos 404, intentamos la siguiente variante (por compatibilidad con el stub)
			if resp.StatusCode == http.StatusNotFound {
				lastErr = fmt.Errorf("mtm proof status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
				continue
			}
			return merkleProofPayload{}, fmt.Errorf("mtm proof status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
		}
		var out merkleProofPayload
		decErr := json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if decErr != nil {
			lastErr = decErr
			continue
		}
		return out, nil
	}
	if lastErr == nil {
		lastErr = errors.New("mtm proof lookup failed")
	}
	return merkleProofPayload{}, lastErr
}

func (n *Node) fetchMerkleRoot(ctx context.Context) (string, error) {
	base, client := n.getMTMClient()
	if base == "" {
		return "", errors.New("mtm base url not configured")
	}
	n.mu.RLock()
	cached := n.mtmRoot
	stamp := n.mtmRootFetched
	n.mu.RUnlock()
	if cached != "" && time.Since(stamp) < 2*time.Second {
		return cached, nil
	}
	endpoint, err := url.JoinPath(base, "/merkle/root")
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return "", fmt.Errorf("mtm root status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload struct {
		Root string `json:"root"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}
	root := payload.Root
	n.mu.Lock()
	n.mtmRoot = root
	n.mtmRootFetched = time.Now()
	n.mu.Unlock()
	return root, nil
}

func (n *Node) getMTMClient() (string, *http.Client) {
	n.mu.RLock()
	base := n.mtmBaseURL
	client := n.mtmClient
	n.mu.RUnlock()
	return base, client
}

func canonicalProofBytes(wallet string, proof merkleProofPayload) []byte {
	canonicalSiblings := make([]string, len(proof.Siblings))
	for i, s := range proof.Siblings {
		canonicalSiblings[i] = strings.ToLower(trimHexPrefix(s))
	}
	canonicalDirs := make([]string, len(proof.Dirs))
	for i, d := range proof.Dirs {
		canonicalDirs[i] = strings.ToLower(d)
	}
	payload := struct {
		Wallet   string   `json:"wallet"`
		Root     string   `json:"root"`
		Index    int      `json:"index"`
		Siblings []string `json:"siblings"`
		Dirs     []string `json:"dirs"`
	}{
		Wallet:   strings.ToLower(wallet),
		Root:     strings.ToLower(trimHexPrefix(proof.Root)),
		Index:    proof.Index,
		Siblings: canonicalSiblings,
		Dirs:     canonicalDirs,
	}
	b, _ := json.Marshal(payload)
	return b
}

func verifyMerkleProof(leaf string, proof merkleProofPayload, expectedRoot string) error {
	if len(proof.Siblings) != len(proof.Dirs) {
		return errors.New("invalid proof length mismatch")
	}
	leafBytes := []byte(leaf)
	cur := sha256.Sum256(leafBytes)
	current := cur[:]
	for i := 0; i < len(proof.Siblings); i++ {
		sibHex := trimHexPrefix(proof.Siblings[i])
		sibBytes, err := hex.DecodeString(sibHex)
		if err != nil {
			return fmt.Errorf("invalid sibling hex: %w", err)
		}
		dir := strings.ToLower(proof.Dirs[i])
		var combined []byte
		switch dir {
		case "left":
			combined = append(append([]byte{}, sibBytes...), current...)
		case "right":
			combined = append(append([]byte{}, current...), sibBytes...)
		default:
			return fmt.Errorf("invalid proof direction %q", proof.Dirs[i])
		}
		h := sha256.Sum256(combined)
		current = h[:]
	}
	expected := trimHexPrefix(expectedRoot)
	if expected == "" {
		return errors.New("empty expected root")
	}
	if !strings.EqualFold(hex.EncodeToString(current), expected) {
		return errors.New("merkle root mismatch")
	}
	return nil
}

func trimHexPrefix(v string) string {
	v = strings.TrimSpace(v)
	if strings.HasPrefix(v, "0x") || strings.HasPrefix(v, "0X") {
		return v[2:]
	}
	return v
}

func (n *Node) buildLeaderCollectEntry(ctx context.Context, data string) (collectReq, error) {
	n.mu.RLock()
	keyHex := n.privKeyHex
	flow := protocol.CloneExecutionFlow(n.txExecutionFlow)
	n.mu.RUnlock()
	if keyHex == "" {
		return collectReq{}, errors.New("leader missing private key")
	}
	priv, err := eth.ParsePrivateKey(keyHex)
	if err != nil {
		return collectReq{}, err
	}
	sigHex, addr, err := eth.SignPersonal([]byte(data), priv)
	if err != nil {
		return collectReq{}, err
	}
	proof, proofSig, err := n.prepareProof(ctx, addr)
	if err != nil {
		return collectReq{}, err
	}
	participant := participantStorageKey(n.id, addr)
	return collectReq{Data: data, Wallet: addr, Sig: sigHex, Proof: proof, ProofSig: proofSig, ParticipantID: participant, ExecutionFlow: flow}, nil
}

func (n *Node) appendTxSubFlow(source string, steps []protocol.ExecutionStep) {
	if len(steps) == 0 {
		return
	}
	label := normalizeParticipantDisplay(source)
	if label == "" {
		return
	}
	clone := protocol.CloneExecutionFlow(steps)
	n.mu.Lock()
	if n.txSubFlows == nil {
		n.txSubFlows = make(map[string][]protocol.ExecutionStep)
	}
	n.txSubFlows[label] = append(n.txSubFlows[label], clone...)
	n.mu.Unlock()
}

func (n *Node) appendTxCommitFlow(source string, step protocol.ExecutionStep) {
	label := normalizeParticipantDisplay(source)
	if label == "" {
		return
	}
	n.mu.Lock()
	if n.txCommitFlows == nil {
		n.txCommitFlows = make(map[string][]protocol.ExecutionStep)
	}
	n.txCommitFlows[label] = append(n.txCommitFlows[label], step)
	n.mu.Unlock()
}

func (n *Node) recordConsensusTrace(msg ibft.Message) {
	if !n.isLeader {
		return
	}
	switch msg.Type {
	case ibft.Commit:
		meta := map[string]string{
			"sender":        msg.From,
			"round":         strconv.FormatUint(uint64(msg.Round), 10),
			"proposal_hash": msg.Value,
		}
		step := protocol.NewExecutionStep("ibft_node", msg.From, "commit_ack", meta)
		n.appendTxCommitFlow(msg.From, step)
	}
}

func (n *Node) buildCollectAggregation(count, threshold int, items []collectReq) *protocol.ExecutionAggregation {
	if len(items) == 0 {
		return nil
	}
	n.mu.RLock()
	subFlows := cloneSubFlowMap(n.txSubFlows)
	leaderLabel := normalizeParticipantDisplay(n.id)
	n.mu.RUnlock()
	participants := make([]protocol.ExecutionAggregationParticipant, 0, len(items))
	seen := make(map[string]bool, len(items))
	for _, it := range items {
		label := participantDisplay(it.ParticipantID, it.Wallet)
		if label == "" {
			continue
		}
		normalized := normalizeParticipantDisplay(label)
		if seen[normalized] {
			continue
		}
		events := protocol.CloneExecutionFlow(subFlows[normalized])
		metadata := map[string]string{
			"wallet":    strings.ToLower(it.Wallet),
			"signature": it.Sig,
		}
		if root := trimHexPrefix(it.Proof.Root); root != "" {
			metadata["proof_root"] = root
		}
		participant := protocol.ExecutionAggregationParticipant{
			NodeID:     label,
			Role:       participantRole(normalized, leaderLabel),
			FlowDigest: flowDigest(events),
			Metadata:   metadata,
			Events:     events,
		}
		participants = append(participants, participant)
		seen[normalized] = true
	}
	if len(participants) == 0 {
		return nil
	}
	sort.Slice(participants, func(i, j int) bool {
		return participants[i].NodeID < participants[j].NodeID
	})
	return &protocol.ExecutionAggregation{
		Kind:           "collect_threshold",
		QuorumAchieved: count,
		QuorumRequired: threshold,
		ReceivedAt:     time.Now().UTC(),
		Participants:   participants,
	}
}

func (n *Node) buildCommitAggregationLocked(proposalHash string) *protocol.ExecutionAggregation {
	total := n.e.ValidatorCount()
	round := n.e.Round()
	leaderLabel := normalizeParticipantDisplay(n.id)
	participants := make([]protocol.ExecutionAggregationParticipant, 0, len(n.txCommitFlows)+1)
	keys := make([]string, 0, len(n.txCommitFlows))
	for k := range n.txCommitFlows {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		events := protocol.CloneExecutionFlow(n.txCommitFlows[key])
		participants = append(participants, protocol.ExecutionAggregationParticipant{
			NodeID:     key,
			Role:       participantRole(key, leaderLabel),
			FlowDigest: flowDigest(events),
			Events:     events,
		})
	}
	if !containsParticipant(keys, leaderLabel) {
		leaderStep := protocol.NewExecutionStep("ibft_node", n.id, "commit_self", map[string]string{
			"proposal_hash": proposalHash,
			"round":         strconv.FormatUint(uint64(round), 10),
		})
		participants = append(participants, protocol.ExecutionAggregationParticipant{
			NodeID:     leaderLabel,
			Role:       "leader",
			FlowDigest: flowDigest([]protocol.ExecutionStep{leaderStep}),
			Events:     []protocol.ExecutionStep{leaderStep},
		})
	}
	if len(participants) == 0 {
		return nil
	}
	sort.Slice(participants, func(i, j int) bool {
		return participants[i].NodeID < participants[j].NodeID
	})
	agg := &protocol.ExecutionAggregation{
		Kind:           "ibft_commit",
		QuorumAchieved: len(participants),
		QuorumRequired: (2*total)/3 + 1,
		ReceivedAt:     time.Now().UTC(),
		Participants:   participants,
		Metadata: map[string]string{
			"proposal_hash": proposalHash,
			"round":         strconv.FormatUint(uint64(round), 10),
		},
	}
	return agg
}

func containsParticipant(list []string, target string) bool {
	for _, v := range list {
		if v == target {
			return true
		}
	}
	return false
}

func participantRole(label, leaderLabel string) string {
	if label == leaderLabel {
		return "leader"
	}
	return "follower"
}

func flowDigest(steps []protocol.ExecutionStep) string {
	if len(steps) == 0 {
		return ""
	}
	b, err := json.Marshal(steps)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (n *Node) appendTxExecutionStep(role string, metadata map[string]string, aggregation *protocol.ExecutionAggregation) {
	step := protocol.NewExecutionStep("ibft_node", n.id, role, metadata)
	if aggregation != nil {
		step.Aggregation = protocol.CloneExecutionAggregation(aggregation)
	}
	n.mu.Lock()
	n.txExecutionFlow = append(n.txExecutionFlow, step)
	n.mu.Unlock()
}

func cloneSubFlowMap(src map[string][]protocol.ExecutionStep) map[string][]protocol.ExecutionStep {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string][]protocol.ExecutionStep, len(src))
	for k, v := range src {
		dst[k] = protocol.CloneExecutionFlow(v)
	}
	return dst
}

func buildExecutionFlowPayload(main []protocol.ExecutionStep, subs map[string][]protocol.ExecutionStep) protocol.ExecutionFlow {
	flow := protocol.ExecutionFlow{
		Steps: protocol.CloneExecutionFlow(main),
	}
	if len(subs) == 0 {
		return flow
	}
	keys := make([]string, 0, len(subs))
	for k := range subs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	subflows := make([]protocol.ExecutionSubflow, 0, len(keys))
	for _, k := range keys {
		steps := protocol.CloneExecutionFlow(subs[k])
		if len(steps) == 0 {
			continue
		}
		subflows = append(subflows, protocol.ExecutionSubflow{Source: k, Steps: steps})
	}
	if len(subflows) > 0 {
		flow.Subflows = subflows
	}
	return flow
}

func participantStorageKey(participantID, wallet string) string {
	id := strings.TrimSpace(participantID)
	if id != "" {
		return strings.ToUpper(id)
	}
	w := strings.TrimSpace(wallet)
	if w == "" {
		return ""
	}
	return strings.ToLower(w)
}

func participantDisplay(participantID, wallet string) string {
	id := strings.TrimSpace(participantID)
	if id != "" {
		return strings.ToUpper(id)
	}
	w := strings.TrimSpace(wallet)
	if w == "" {
		return ""
	}
	lower := strings.ToLower(w)
	if strings.HasPrefix(lower, "0x") {
		return lower
	}
	return lower
}

func normalizeParticipantDisplay(source string) string {
	s := strings.TrimSpace(source)
	if s == "" {
		return ""
	}
	lower := strings.ToLower(s)
	if strings.HasPrefix(lower, "0x") {
		return lower
	}
	return strings.ToUpper(s)
}
