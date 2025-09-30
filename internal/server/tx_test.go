package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		Data     string             `json:"data"`
		Wallet   string             `json:"public_wallet"`
		Sig      string             `json:"firma_content"`
		Proof    merkleProofPayload `json:"merkle_proof"`
		ProofSig string             `json:"proof_sig"`
	}
	type signer struct {
		collect
		privHex string
	}
	signers := make([]signer, 0, len(keys))
	wallets := make([]string, 0, len(keys))
	for _, k := range keys {
		priv, err := eth.ParsePrivateKey(k)
		if err != nil {
			t.Fatalf("parse key: %v", err)
		}
		sig, addr, err := eth.SignPersonal([]byte(data), priv)
		if err != nil {
			t.Fatalf("sign: %v", err)
		}
		signers = append(signers, signer{collect: collect{Data: data, Wallet: addr, Sig: sig}, privHex: k})
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
