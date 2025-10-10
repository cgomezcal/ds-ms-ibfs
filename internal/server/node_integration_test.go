package server

import (
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
	t.Skip("Flujo IBFT eliminado; la integración ahora depende del colector directo de transacciones")
}

func TestIBFTEndpointsRemoved(t *testing.T) {
	t.Skip("IBFT endpoints removed; direct execution flow no longer exposes /v1/ibft/*")
}
