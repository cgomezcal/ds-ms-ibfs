package server

import (
    "bytes"
    "encoding/json"
    "io"
    "net/http"
    "net/http/httptest"
    "testing"
    "time"

    "github.com/cgomezcal/ds-ms-ibfs/internal/eth"
)

// TestExecuteTransaction_IBFT ensures the final voting is performed via IBFT when threshold is reached.
func TestExecuteTransaction_IBFT(t *testing.T) {
    // test keys
    keyA := "0xb71c71a67e1177ad4e901695e1b4b9c2d68e8f0e2b8a9d17a2a4d4a8e6f5f2f0"
    keyB := "0x4f3edf983ac636a65a842ce7c78d9aa706d3b113bce9c46f30d7d21715b23b1d"
    keyC := "0x8f2a559490b8d6e3d2b1016da0fbdc3f5b6b6f9bcd3d2e2c2a2b1a1a0a9a8a7a"

    // nodes
    leader := NewNode("A", ":0", nil)
    nodeB := NewNode("B", ":0", nil)
    nodeC := NewNode("C", ":0", nil)

    privA, err := eth.ParsePrivateKey(keyA)
    if err != nil { t.Fatalf("parse A: %v", err) }
    walletA := eth.AddressFromPrivate(privA)
    privB, err := eth.ParsePrivateKey(keyB)
    if err != nil { t.Fatalf("parse B: %v", err) }
    walletB := eth.AddressFromPrivate(privB)
    privC, err := eth.ParsePrivateKey(keyC)
    if err != nil { t.Fatalf("parse C: %v", err) }
    walletC := eth.AddressFromPrivate(privC)

    stub := newMTMStub(t, []string{walletA, walletB, walletC})
    defer stub.Close()
    leader.SetMTMBaseURL(stub.URL())
    nodeB.SetMTMBaseURL(stub.URL())
    nodeC.SetMTMBaseURL(stub.URL())

    // auth
    leader.SetAuthToken("t")
    nodeB.SetAuthToken("t")
    nodeC.SetAuthToken("t")

    // Leader will be assigned actual URL after starting servers
    leader.SetPrivateKey(keyA)

    // servers
    tsA := httptest.NewServer(leader.mux)
    defer tsA.Close()
    tsB := httptest.NewServer(nodeB.mux)
    defer tsB.Close()
    tsC := httptest.NewServer(nodeC.mux)
    defer tsC.Close()

    // network
    peersA := []string{tsB.URL, tsC.URL}
    peersB := []string{tsA.URL, tsC.URL}
    peersC := []string{tsA.URL, tsB.URL}
    validators := []string{"A", "B", "C"}
    leader.SetNetwork(peersA, validators)
    nodeB.SetNetwork(peersB, validators)
    nodeC.SetNetwork(peersC, validators)

    // set leader URL to leader's actual base URL for forwarding
    leader.SetLeader(tsA.URL, true)
    nodeB.SetLeader(tsA.URL, false)
    nodeC.SetLeader(tsA.URL, false)

    // Each node needs a private key for /execute-transaction signing and forwarding.
    nodeB.SetPrivateKey(keyB)
    nodeC.SetPrivateKey(keyC)

    // Execute from leader; leader should broadcast to peers for signing, reach threshold, distribute proposal, and IBFT commit.
    payload := map[string]string{"data": "hola-ibft"}
    body, _ := json.Marshal(payload)
    req, _ := http.NewRequest(http.MethodPost, tsA.URL+"/v1/tx/execute-transaction", bytes.NewReader(body))
    req.Header.Set("Content-Type", "application/json")
    res, err := http.DefaultClient.Do(req)
    if err != nil { t.Fatalf("execute: %v", err) }
    if res.StatusCode != http.StatusAccepted { t.Fatalf("expected 202, got %d", res.StatusCode) }
    io.Copy(io.Discard, res.Body); res.Body.Close()

    // Poll leader status until lastTxLaunchedAt set (or timeout)
    deadline := time.Now().Add(3 * time.Second)
    for time.Now().Before(deadline) {
        rs, err := http.Get(tsA.URL+"/v1/tx/status")
        if err != nil { t.Fatalf("status: %v", err) }
        var cur map[string]any
        json.NewDecoder(rs.Body).Decode(&cur)
        rs.Body.Close()
        if s, _ := cur["lastLaunchedAt"].(string); s != "" {
            return
        }
        time.Sleep(50 * time.Millisecond)
    }
    t.Fatalf("transaction was not launched via IBFT in time")
}
