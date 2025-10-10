package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCollectRejectsWrongToken(t *testing.T) {
	n := NewNode("A", ":0", nil)
	n.SetAuthToken("expected-token")
	ts := httptest.NewServer(n.mux)
	defer ts.Close()

	body, _ := json.Marshal(collectReq{Data: "payload"})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/tx/collect", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer wrong-token")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("collect request failed: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", res.StatusCode)
	}
}
