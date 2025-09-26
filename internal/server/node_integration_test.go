package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

type testCluster struct {
	srvA, srvB, srvC, srvD *httptest.Server
}

func startCluster() *testCluster {
	// Use httptest servers with base URLs as peers
	nA := NewNode("A", ":0", nil)
	nB := NewNode("B", ":0", nil)
	nC := NewNode("C", ":0", nil)
	nD := NewNode("D", ":0", nil)

	tsA := httptest.NewServer(nA.mux)
	tsB := httptest.NewServer(nB.mux)
	tsC := httptest.NewServer(nC.mux)
	tsD := httptest.NewServer(nD.mux)

	// Wire peers after servers know URLs
	peersA := []string{tsB.URL, tsC.URL, tsD.URL}
	peersB := []string{tsA.URL, tsC.URL, tsD.URL}
	peersC := []string{tsA.URL, tsB.URL, tsD.URL}
	peersD := []string{tsA.URL, tsB.URL, tsC.URL}
	validators := []string{"A", "B", "C", "D"}
	nA.SetNetwork(peersA, validators)
	nB.SetNetwork(peersB, validators)
	nC.SetNetwork(peersC, validators)
	nD.SetNetwork(peersD, validators)

	return &testCluster{srvA: tsA, srvB: tsB, srvC: tsC, srvD: tsD}
}

func (tc *testCluster) Close() {
	tc.srvA.Close()
	tc.srvB.Close()
	tc.srvC.Close()
	tc.srvD.Close()
}

func TestFourNodeConsensus(t *testing.T) {
	tc := startCluster()
	defer tc.Close()

	// Propose on A
	body, _ := json.Marshal(map[string]string{"value": "block-1"})
	resp, err := http.Post(tc.srvA.URL+"/v1/ibft/propose", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("propose error: %v", err)
	}
	resp.Body.Close()

	// Poll until B reports committed
	for i := 0; i < 20; i++ {
		res, err := http.Get(tc.srvB.URL + "/v1/ibft/state")
		if err != nil {
			t.Fatal(err)
		}
		var cur map[string]any
		json.NewDecoder(res.Body).Decode(&cur)
		res.Body.Close()
		if s, ok := cur["state"].(string); ok && s == "committed" {
			if v := fmt.Sprintf("%v", cur["value"]); v == "" {
				t.Fatalf("no value in committed state")
			}
			return
		}
	}
	t.Fatalf("node B did not commit in time")
}
