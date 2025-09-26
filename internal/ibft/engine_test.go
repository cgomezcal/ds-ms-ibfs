package ibft

import "testing"

type nopBroadcaster struct{}

func (nopBroadcaster) Broadcast(Message) {}

func TestQuorumAndFlow(t *testing.T) {
	vals := []string{"A", "B", "C", "D"} // f=1, quorum=3
	eA := NewEngine("A", vals, nopBroadcaster{})
	eB := NewEngine("B", vals, nopBroadcaster{})
	eC := NewEngine("C", vals, nopBroadcaster{})

	// drive messages manually
	pp, err := eA.Propose("hello")
	if err != nil {
		t.Fatal(err)
	}

	// B and C handle PrePrepare -> they reply Prepare (internally), but we just simulate prepares
	_, prepB, err := eB.Handle(pp)
	if err != nil {
		t.Fatal(err)
	}
	if prepB == nil || prepB.Type != Prepare {
		t.Fatalf("B did not prepare")
	}
	_, prepC, err := eC.Handle(pp)
	if err != nil {
		t.Fatal(err)
	}
	if prepC == nil || prepC.Type != Prepare {
		t.Fatalf("C did not prepare")
	}

	// A handles prepares from B and C -> should move to prepared and emit commit
	state, comA, err := eA.Handle(*prepB)
	if err != nil {
		t.Fatal(err)
	}
	state, comA, err = eA.Handle(*prepC)
	if err != nil {
		t.Fatal(err)
	}
	if state != "prepared" || comA == nil || comA.Type != Commit {
		t.Fatalf("A not prepared/commit: %v %v", state, comA)
	}

	// B handles commit from A then a properly signed commit from C -> committed
	_, _, _ = eB.Handle(*comA)
	mc := Message{Type: Commit, Round: eB.Round(), From: "C", Value: eA.Value()}
	mc.Sig = signMessage(DerivedKeyStore{}.Private("C"), mc)
	_, _, _ = eB.Handle(mc)
	if eB.State() != "committed" {
		t.Fatalf("B not committed: %s", eB.State())
	}
}
