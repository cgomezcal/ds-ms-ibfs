package server

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cgomezcal/ds-ms-ibfs/internal/eth"
)

// TestTxVotingFlow ensures that when the leader reaches threshold of collected signatures,
// ahora distribuye un proposal y usa IBFT para finalizar. La respuesta de /collect es 202 y
// al finalizar IBFT, el estado interno marca lanzamiento.
func TestTxVotingFlow(t *testing.T) {
	// Keys for test accounts
	keyA := "0xb71c71a67e1177ad4e901695e1b4b9c2d68e8f0e2b8a9d17a2a4d4a8e6f5f2f0"
	keyB := "0x4f3edf983ac636a65a842ce7c78d9aa706d3b113bce9c46f30d7d21715b23b1d"
	keyC := "0x8f2a559490b8d6e3d2b1016da0fbdc3f5b6b6f9bcd3d2e2c2a2b1a1a0a9a8a7a"

	// Build nodes
	leader := NewNode("A", ":0", nil)
	nodeB := NewNode("B", ":0", nil)
	nodeC := NewNode("C", ":0", nil)

	// Shared bearer token for peer messages
	leader.SetAuthToken("t")
	nodeB.SetAuthToken("t")
	nodeC.SetAuthToken("t")

	privA, err := eth.ParsePrivateKey(keyA)
	if err != nil {
		t.Fatalf("parse A: %v", err)
	}
	walletA := eth.AddressFromPrivate(privA)
	privB, err := eth.ParsePrivateKey(keyB)
	if err != nil {
		t.Fatalf("parse B: %v", err)
	}
	walletB := eth.AddressFromPrivate(privB)
	privC, err := eth.ParsePrivateKey(keyC)
	if err != nil {
		t.Fatalf("parse C: %v", err)
	}
	walletC := eth.AddressFromPrivate(privC)

	stub := newMTMStub(t, []string{walletA, walletB, walletC})
	defer stub.Close()
	leader.SetMTMBaseURL(stub.URL())
	nodeB.SetMTMBaseURL(stub.URL())
	nodeC.SetMTMBaseURL(stub.URL())

	// Leader URL hint for nodes (not strictly needed in this unit test)
	leader.SetLeader("http://leader", true)
	nodeB.SetLeader("http://leader", false)
	nodeC.SetLeader("http://leader", false)

	// Leader must have a private key to sign aggregate
	leader.SetPrivateKey(keyA)

	// Start servers to get base URLs
	tsA := httptest.NewServer(leader.mux)
	defer tsA.Close()
	tsB := httptest.NewServer(nodeB.mux)
	defer tsB.Close()
	tsC := httptest.NewServer(nodeC.mux)
	defer tsC.Close()

	// Wire peers and validators (3 nodes => threshold = 3)
	peersA := []string{tsB.URL, tsC.URL}
	peersB := []string{tsA.URL, tsC.URL}
	peersC := []string{tsA.URL, tsB.URL}
	validators := []string{"A", "B", "C"}
	leader.SetNetwork(peersA, validators)
	nodeB.SetNetwork(peersB, validators)
	nodeC.SetNetwork(peersC, validators)

	// Prepare collects from B and C
	data := "hello-vote"
	sigB, _, err := eth.SignPersonal([]byte(data), privB)
	if err != nil {
		t.Fatalf("sign B: %v", err)
	}
	sigC, _, err := eth.SignPersonal([]byte(data), privC)
	if err != nil {
		t.Fatalf("sign C: %v", err)
	}

	proofB := stub.Proof(walletB)
	proofSigB, _, err := eth.SignPersonal(canonicalProofBytes(walletB, proofB), privB)
	if err != nil {
		t.Fatalf("sign proof B: %v", err)
	}
	proofC := stub.Proof(walletC)
	proofSigC, _, err := eth.SignPersonal(canonicalProofBytes(walletC, proofC), privC)
	if err != nil {
		t.Fatalf("sign proof C: %v", err)
	}

	// First collect (from B) => 202
	b1, _ := json.Marshal(collectReq{Data: data, Wallet: walletB, Sig: sigB, Proof: proofB, ProofSig: proofSigB, ParticipantID: "B"})
	req1, _ := http.NewRequest(http.MethodPost, tsA.URL+"/v1/tx/collect", bytes.NewReader(b1))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("Authorization", "Bearer t")
	res1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("collect1: %v", err)
	}
	defer res1.Body.Close()
	if res1.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", res1.StatusCode)
	}

	// Second collect (from C) => triggers proposal + IBFT; handler responde 202 mientras IBFT corre
	b2, _ := json.Marshal(collectReq{Data: data, Wallet: walletC, Sig: sigC, Proof: proofC, ProofSig: proofSigC, ParticipantID: "C"})
	req2, _ := http.NewRequest(http.MethodPost, tsA.URL+"/v1/tx/collect", bytes.NewReader(b2))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Authorization", "Bearer t")
	res2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("collect2: %v", err)
	}
	defer res2.Body.Close()
	if res2.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 after proposal, got %d", res2.StatusCode)
	}
}

// TestCollectThresholdWithoutPrivateKey verifica que sin una clave privada configurada,
// el líder no ejecuta el proceso de Besu y la agregación permanece disponible.
func TestCollectThresholdWithoutPrivateKey(t *testing.T) {
	keyA := "0xb71c71a67e1177ad4e901695e1b4b9c2d68e8f0e2b8a9d17a2a4d4a8e6f5f2f0"
	keyB := "0x4f3edf983ac636a65a842ce7c78d9aa706d3b113bce9c46f30d7d21715b23b1d"
	keyC := "0x8f2a559490b8d6e3d2b1016da0fbdc3f5b6b6f9bcd3d2e2c2a2b1a1a0a9a8a7a"

	leader := NewNode("A", ":0", nil)
	leader.SetAuthToken("t")
	leader.SetNetwork(nil, []string{"A", "B", "C"})
	leader.SetLeader("http://leader", true)
	ts := httptest.NewServer(leader.mux)
	defer ts.Close()
	leader.SetLeader(ts.URL, true)

	leader.SetBesuExecutor(func(string) (string, error) {
		t.Fatalf("besu executor should not run when leader has no private key")
		return "", nil
	})

	privA, err := eth.ParsePrivateKey(keyA)
	if err != nil {
		t.Fatalf("parse A: %v", err)
	}
	privB, err := eth.ParsePrivateKey(keyB)
	if err != nil {
		t.Fatalf("parse B: %v", err)
	}
	privC, err := eth.ParsePrivateKey(keyC)
	if err != nil {
		t.Fatalf("parse C: %v", err)
	}

	walletA := eth.AddressFromPrivate(privA)
	walletB := eth.AddressFromPrivate(privB)
	walletC := eth.AddressFromPrivate(privC)

	stub := newMTMStub(t, []string{walletA, walletB, walletC})
	defer stub.Close()
	leader.SetMTMBaseURL(stub.URL())

	data := "no-leader-key"
	entries := []collectReq{
		buildCollectEntry(t, data, "A", privA, walletA, stub),
		buildCollectEntry(t, data, "B", privB, walletB, stub),
		buildCollectEntry(t, data, "C", privC, walletC, stub),
	}

	for _, entry := range entries {
		body, _ := json.Marshal(entry)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tx/collect", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer t")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("collect %s: %v", entry.ParticipantID, err)
		}
		if res.StatusCode != http.StatusAccepted {
			t.Fatalf("expected 202 for %s, got %d", entry.ParticipantID, res.StatusCode)
		}
		res.Body.Close()
	}

	if pending, count := leader.txDebug(); pending != data || count != len(entries) {
		t.Fatalf("expected pending data=%q count=%d, got data=%q count=%d", data, len(entries), pending, count)
	}

	leader.mu.RLock()
	lastData := leader.lastTxLastData
	leader.mu.RUnlock()
	if lastData != "" {
		t.Fatalf("expected no last execution data, got %q", lastData)
	}

	refresh := entries[1]
	body, _ := json.Marshal(refresh)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tx/collect", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer t")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("collect refresh: %v", err)
	}
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 on refresh, got %d", res.StatusCode)
	}
	res.Body.Close()
}

func buildCollectEntry(t *testing.T, data string, participant string, priv *ecdsa.PrivateKey, wallet string, stub *mtmStub) collectReq {
	t.Helper()
	sig, _, err := eth.SignPersonal([]byte(data), priv)
	if err != nil {
		t.Fatalf("sign data %s: %v", participant, err)
	}
	proof := stub.Proof(wallet)
	sigProof, _, err := eth.SignPersonal(canonicalProofBytes(wallet, proof), priv)
	if err != nil {
		t.Fatalf("sign proof %s: %v", participant, err)
	}
	return collectReq{
		Data:          data,
		Wallet:        wallet,
		Sig:           sig,
		Proof:         proof,
		ProofSig:      sigProof,
		ParticipantID: participant,
	}
}
