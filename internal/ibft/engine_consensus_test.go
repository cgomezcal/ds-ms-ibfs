package ibft

import (
	"sync"
	"testing"
	"time"
)

type inmemNet struct {
	mu    sync.RWMutex
	nodes map[string]*Engine
}

func newInmemNet() *inmemNet { return &inmemNet{nodes: make(map[string]*Engine)} }

// Broadcast forwards the message to all engines (including sender for simplicity).
func (n *inmemNet) Broadcast(msg Message) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for id, e := range n.nodes {
		_ = id
		// deliver asynchronously to avoid lock contention
		go func(target *Engine, m Message) { target.Handle(m) }(e, msg)
	}
}

func TestIBFTConsensusBroadcast(t *testing.T) {
	vals := []string{"A", "B", "C", "D"}
	net := newInmemNet()

	mk := func(id string) *Engine {
		eng := NewEngine(id, vals, net)
		net.mu.Lock()
		net.nodes[id] = eng
		net.mu.Unlock()
		return eng
	}

	eA := mk("A")
	eB := mk("B")
	eC := mk("C")
	eD := mk("D")
	_ = eB
	_ = eC
	_ = eD

	if _, err := eA.Propose("hello-world"); err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	want := eA.Value()
	for time.Now().Before(deadline) {
		if eA.State() == "committed" && eB.State() == "committed" && eC.State() == "committed" && eD.State() == "committed" {
			if eB.Value() == want && eC.Value() == want && eD.Value() == want {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("consensus not reached: A=%s B=%s C=%s D=%s want=%s", eA.State(), eB.State(), eC.State(), eD.State(), want)
}
