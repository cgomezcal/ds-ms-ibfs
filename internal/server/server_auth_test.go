package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cgomezcal/ds-ms-ibfs/internal/ibft"
)

func TestUnauthorizedPeerMessage(t *testing.T) {
	n := NewNode("A", ":0", nil)
	n.SetAuthToken("secret")
	ts := httptest.NewServer(n.mux)
	defer ts.Close()

	// Without Authorization header should be 401
	b, _ := json.Marshal(ibft.Message{Type: ibft.Prepare, Round: 0, From: "B", Value: "v", Sig: "00"})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ibft/message", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", res.StatusCode)
	}

	// With Authorization but invalid signature should be 400
	req2, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/ibft/message", bytes.NewReader(b))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Authorization", "Bearer secret")
	res2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	if res2.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", res2.StatusCode)
	}
}
