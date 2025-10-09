package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/cgomezcal/ds-ms-ibfs/internal/eth"
)

func TestTxAggregationLeaderQuorum(t *testing.T) {
	// Leader setup
	leader := NewNode("A", ":0", nil)
	leader.SetAuthToken("t")
	leader.SetNetwork(nil, []string{"A", "B", "C", "D"})
	leader.SetLeader("http://leader", true)
	tsL := httptest.NewServer(leader.mux)
	defer tsL.Close()
	leader.SetLeader(tsL.URL, true)

	data := "hello"
	// three distinct dev keys
	keys := []string{
		"0xb71c71a67e1177ad4e901695e1b4b9c2d68e8f0e2b8a9d17a2a4d4a8e6f5f2f0",
		"0x4f3edf983ac636a65a842ce7c78d9aa706d3b113bce9c46f30d7d21715b23b1d",
		"0x8f2a559490b8d6e3d2b1016da0fbdc3f5b6b6f9bcd3d2e2c2a2b1a1a0a9a8a7a",
	}
	leader.SetPrivateKey(keys[0])

	type collect struct {
		Data          string             `json:"data"`
		Wallet        string             `json:"public_wallet"`
		Sig           string             `json:"firma_content"`
		Proof         merkleProofPayload `json:"merkle_proof"`
		ProofSig      string             `json:"proof_sig"`
		ParticipantID string             `json:"participant_id"`
	}
	type signer struct {
		collect
		privHex string
	}
	signers := make([]signer, 0, len(keys))
	wallets := make([]string, 0, len(keys))
	for idx, k := range keys {
		priv, err := eth.ParsePrivateKey(k)
		if err != nil {
			t.Fatalf("parse key: %v", err)
		}
		sig, addr, err := eth.SignPersonal([]byte(data), priv)
		if err != nil {
			t.Fatalf("sign: %v", err)
		}
		participantID := string(rune('A' + idx))
		signers = append(signers, signer{collect: collect{Data: data, Wallet: addr, Sig: sig, ParticipantID: participantID}, privHex: k})
		wallets = append(wallets, addr)
	}
	stub := newMTMStub(t, wallets)
	defer stub.Close()
	leader.SetMTMBaseURL(stub.URL())
	for i := range signers {
		proof := stub.Proof(signers[i].Wallet)
		priv, err := eth.ParsePrivateKey(signers[i].privHex)
		if err != nil {
			t.Fatalf("parse key for proof: %v", err)
		}
		sigProof, _, err := eth.SignPersonal(canonicalProofBytes(signers[i].Wallet, proof), priv)
		if err != nil {
			t.Fatalf("sign proof: %v", err)
		}
		signers[i].Proof = proof
		signers[i].ProofSig = sigProof
	}
	// Primeras dos 202, la tercera ahora es 202 (IBFT corre asíncronamente)
	for i, pl := range signers {
		b, _ := json.Marshal(pl.collect)
		req, _ := http.NewRequest(http.MethodPost, tsL.URL+"/v1/tx/collect", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer t")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if i < 2 && res.StatusCode != http.StatusAccepted {
			t.Fatalf("expected 202 on %d, got %d", i, res.StatusCode)
		}
		if i == 2 && res.StatusCode != http.StatusAccepted {
			t.Fatalf("expected 202 on third (IBFT), got %d", res.StatusCode)
		}
		res.Body.Close()
	}
}

func TestTxAggregationSameWalletParticipants(t *testing.T) {
	leader := NewNode("A", ":0", nil)
	leader.SetAuthToken("t")
	leader.SetNetwork(nil, []string{"A", "B", "C"})
	leader.SetLeader("http://leader", true)
	ts := httptest.NewServer(leader.mux)
	defer ts.Close()
	leader.SetLeader(ts.URL, true)

	key := "0xb71c71a67e1177ad4e901695e1b4b9c2d68e8f0e2b8a9d17a2a4d4a8e6f5f2f0"
	leader.SetPrivateKey(key)

	priv, err := eth.ParsePrivateKey(key)
	if err != nil {
		t.Fatalf("parse key: %v", err)
	}
	wallet := eth.AddressFromPrivate(priv)
	data := "hola-same-wallet"
	sig, _, err := eth.SignPersonal([]byte(data), priv)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	stub := newMTMStub(t, []string{wallet})
	defer stub.Close()
	leader.SetMTMBaseURL(stub.URL())

	proof := stub.Proof(wallet)
	proofSig, _, err := eth.SignPersonal(canonicalProofBytes(wallet, proof), priv)
	if err != nil {
		t.Fatalf("sign proof: %v", err)
	}

	type collect struct {
		Data          string             `json:"data"`
		Wallet        string             `json:"public_wallet"`
		Sig           string             `json:"firma_content"`
		Proof         merkleProofPayload `json:"merkle_proof"`
		ProofSig      string             `json:"proof_sig"`
		ParticipantID string             `json:"participant_id"`
	}

	participants := []string{"A", "B", "C"}
	for idx, participant := range participants {
		payload := collect{Data: data, Wallet: wallet, Sig: sig, Proof: proof, ProofSig: proofSig, ParticipantID: participant}
		body, _ := json.Marshal(payload)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tx/collect", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer t")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("collect %s: %v", participant, err)
		}
		if res.StatusCode != http.StatusAccepted {
			t.Fatalf("expected 202 for %s, got %d", participant, res.StatusCode)
		}
		res.Body.Close()
		if idx < len(participants)-1 {
			if _, count := leader.txDebug(); count != idx+1 {
				t.Fatalf("expected %d collected entries, got %d", idx+1, count)
			}
		}
	}
}

func TestVerifyProofAllowsDegeneratePayload(t *testing.T) {
	leader := NewNode("A", ":0", nil)
	privHex := "0xb71c71a67e1177ad4e901695e1b4b9c2d68e8f0e2b8a9d17a2a4d4a8e6f5f2f0"
	leader.SetPrivateKey(privHex)

	priv, err := eth.ParsePrivateKey(privHex)
	if err != nil {
		t.Fatalf("parse key: %v", err)
	}
	wallet := eth.AddressFromPrivate(priv)

	root := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	degProof := merkleProofPayload{Root: root, Index: -1}
	sigProof, _, err := eth.SignPersonal(canonicalProofBytes(wallet, degProof), priv)
	if err != nil {
		t.Fatalf("sign proof: %v", err)
	}

	mtm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/merkle/root":
			json.NewEncoder(w).Encode(map[string]string{"root": root})
		case "/merkle/proof":
			json.NewEncoder(w).Encode(degProof)
		default:
			http.NotFound(w, r)
		}
	}))
	defer mtm.Close()
	leader.SetMTMBaseURL(mtm.URL)

	if err := leader.verifyProof(context.Background(), wallet, degProof, sigProof); err != nil {
		t.Fatalf("unexpected verifyProof error: %v", err)
	}
}

func TestExecuteLeaderCollectErrorPropagates(t *testing.T) {
	leader := NewNode("A", ":0", nil)
	leader.SetAuthToken("t")
	leader.SetNetwork(nil, []string{"A"})
	privHex := "0xb71c71a67e1177ad4e901695e1b4b9c2d68e8f0e2b8a9d17a2a4d4a8e6f5f2f0"
	leader.SetPrivateKey(privHex)
	root := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	degProof := merkleProofPayload{Root: root, Index: -1}

	mtm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/merkle/root":
			json.NewEncoder(w).Encode(map[string]string{"root": root})
		case "/merkle/proof":
			json.NewEncoder(w).Encode(degProof)
		default:
			http.NotFound(w, r)
		}
	}))
	defer mtm.Close()
	leader.SetMTMBaseURL(mtm.URL)

	var (
		collectHits  int32
		collectStatus int32
	)
	collectSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&collectHits, 1)
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method %s", r.Method)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer t" {
			t.Fatalf("missing auth header, got %q", auth)
		}
		http.Error(w, "invalid merkle proof", http.StatusBadRequest)
		atomic.StoreInt32(&collectStatus, http.StatusBadRequest)
	}))
	defer collectSrv.Close()
	leader.SetLeader(collectSrv.URL, true)

	ts := httptest.NewServer(leader.mux)
	defer ts.Close()

	reqPayload := executeReq{Data: "demo", Key: "k"}
	body, _ := json.Marshal(reqPayload)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tx/execute-transaction", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("execute request: %v", err)
	}
	defer resp.Body.Close()
	if atomic.LoadInt32(&collectHits) == 0 {
		t.Fatalf("leader collect endpoint was not hit")
	}
	if status := atomic.LoadInt32(&collectStatus); status != http.StatusBadRequest {
		t.Fatalf("expected collect status 400, got %d", status)
	}
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("expected 502 when collect fails, got %d", resp.StatusCode)
	}

	// asegurar que el flujo no queda registrado tras el fallo
	if data, count := leader.txDebug(); data != "" || count != 0 {
		t.Fatalf("expected no pending aggregation, got data=%q count=%d", data, count)
	}
}
