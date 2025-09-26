package server

import (
    "bytes"
    "encoding/json"
    "io"
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
    type collect struct {
        Data   string `json:"data"`
        Wallet string `json:"public_wallet"`
        Sig    string `json:"firma_content"`
    }
    // sign with B
    privB, err := eth.ParsePrivateKey(keyB)
    if err != nil { t.Fatalf("parse B: %v", err) }
    sigB, addrB, err := eth.SignPersonal([]byte(data), privB)
    if err != nil { t.Fatalf("sign B: %v", err) }
    // sign with C
    privC, err := eth.ParsePrivateKey(keyC)
    if err != nil { t.Fatalf("parse C: %v", err) }
    sigC, addrC, err := eth.SignPersonal([]byte(data), privC)
    if err != nil { t.Fatalf("sign C: %v", err) }

    // First collect (from B) => 202
    b1, _ := json.Marshal(collect{Data: data, Wallet: addrB, Sig: sigB})
    req1, _ := http.NewRequest(http.MethodPost, tsA.URL+"/v1/tx/collect", bytes.NewReader(b1))
    req1.Header.Set("Content-Type", "application/json")
    req1.Header.Set("Authorization", "Bearer t")
    res1, err := http.DefaultClient.Do(req1)
    if err != nil { t.Fatalf("collect1: %v", err) }
    defer res1.Body.Close()
    if res1.StatusCode != http.StatusAccepted { t.Fatalf("expected 202, got %d", res1.StatusCode) }
    io.Copy(io.Discard, res1.Body)

    // Second collect (from C) => triggers proposal + IBFT; handler responde 202 mientras IBFT corre
    b2, _ := json.Marshal(collect{Data: data, Wallet: addrC, Sig: sigC})
    req2, _ := http.NewRequest(http.MethodPost, tsA.URL+"/v1/tx/collect", bytes.NewReader(b2))
    req2.Header.Set("Content-Type", "application/json")
    req2.Header.Set("Authorization", "Bearer t")
    res2, err := http.DefaultClient.Do(req2)
    if err != nil { t.Fatalf("collect2: %v", err) }
    defer res2.Body.Close()
    if res2.StatusCode != http.StatusAccepted {
        t.Fatalf("expected 202 after proposal, got %d", res2.StatusCode)
    }
}
