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

	data := "hello"
	// three distinct dev keys
	keys := []string{
		"0xb71c71a67e1177ad4e901695e1b4b9c2d68e8f0e2b8a9d17a2a4d4a8e6f5f2f0",
		"0x4f3edf983ac636a65a842ce7c78d9aa706d3b113bce9c46f30d7d21715b23b1d",
		"0x8f2a559490b8d6e3d2b1016da0fbdc3f5b6b6f9bcd3d2e2c2a2b1a1a0a9a8a7a",
	}
	type collect struct {
		Data   string `json:"data"`
		Wallet string `json:"public_wallet"`
		Sig    string `json:"firma_content"`
	}
	posts := make([]collect, 0, 3)
	for _, k := range keys {
		priv, err := eth.ParsePrivateKey(k)
		if err != nil {
			t.Fatalf("parse key: %v", err)
		}
		sig, addr, err := eth.SignPersonal([]byte(data), priv)
		if err != nil {
			t.Fatalf("sign: %v", err)
		}
		posts = append(posts, collect{Data: data, Wallet: addr, Sig: sig})
	}
	// First two should be 202, third should be 200 and contain message
	for i, pl := range posts {
		b, _ := json.Marshal(pl)
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
		if i == 2 && res.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 on third, got %d", res.StatusCode)
		}
		res.Body.Close()
	}
}
