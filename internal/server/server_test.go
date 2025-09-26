package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cgomezcal/ds-ms-ibfs/internal/ibft"
)

func TestRejectInvalidSignature(t *testing.T) {
	s := NewNode("A", ":0", nil)
	rr := httptest.NewRecorder()
	// bogus signature
	m := ibft.Message{Type: ibft.Prepare, Round: 0, From: "A", Value: "v", Sig: "deadbeef"}
	body, _ := json.Marshal(m)
	s.handleMessage(rr, httptest.NewRequest(http.MethodPost, "/v1/ibft/message", bytes.NewReader(body)))
	if rr.Code == http.StatusOK {
		t.Fatalf("expected rejection due to invalid signature")
	}
}
