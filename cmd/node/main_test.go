package main

import "testing"

func TestParseFlagsDefaults(t *testing.T) {
	id, addr, peers, vals := parseFlags([]string{})
	if id != "node1" || addr != ":8081" {
		t.Fatalf("defaults mismatch: id=%s addr=%s", id, addr)
	}
	if len(peers) != 0 {
		t.Fatalf("expected no peers, got %v", peers)
	}
	if len(vals) != 1 || vals[0] != "node1" {
		t.Fatalf("default validators unexpected: %v", vals)
	}
}

func TestParseFlagsCustom(t *testing.T) {
	args := []string{"--id", "A", "--addr", ":18081", "--peers", "http://x:1,http://y:2", "--validators", "A,B,C,D"}
	id, addr, peers, vals := parseFlags(args)
	if id != "A" || addr != ":18081" {
		t.Fatalf("custom mismatch: id=%s addr=%s", id, addr)
	}
	if len(peers) != 2 || peers[0] != "http://x:1" || peers[1] != "http://y:2" {
		t.Fatalf("peers parsed incorrectly: %v", peers)
	}
	if len(vals) != 4 || vals[0] != "A" || vals[3] != "D" {
		t.Fatalf("validators parsed incorrectly: %v", vals)
	}
}
