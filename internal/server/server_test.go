package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cgomezcal/ds-ms-ibfs/pkg/protocol"
)

func TestCollectRejectsMissingAuth(t *testing.T) {
	n := NewNode("A", ":0", nil)
	n.SetAuthToken("shared-token")
	n.SetNetwork(nil, []string{"A", "B", "C"})
	ts := httptest.NewServer(n.mux)
	defer ts.Close()

	payload := collectReq{
		Data:          "payload",
		Wallet:        "0xabc",
		Sig:           "sig",
		Proof:         merkleProofPayload{},
		ProofSig:      "",
		ParticipantID: "B",
		ExecutionFlow: []protocol.ExecutionStep{protocol.NewExecutionStep("client", "B", "sign", nil)},
	}
	body, _ := json.Marshal(payload)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tx/collect", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("collect request failed: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 when missing auth header, got %d", res.StatusCode)
	}
}
