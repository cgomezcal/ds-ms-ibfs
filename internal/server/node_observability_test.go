package server

import (
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "testing"

    "github.com/cgomezcal/ds-ms-ibfs/pkg/protocol"
)

func TestNodeStatusIncludesExecutionFlow(t *testing.T) {
    n := NewNode("node-test", ":0", nil)
    t.Cleanup(func() { _ = n.Shutdown() })

    n.appendTxExecutionStep("unit-step", map[string]string{"phase": "test"}, nil)

    req := httptest.NewRequest(http.MethodGet, "/v1/tx/status", nil)
    rec := httptest.NewRecorder()
    n.handleTxStatus(rec, req)

    if rec.Code != http.StatusOK {
        t.Fatalf("unexpected status: %d", rec.Code)
    }

    var payload map[string]any
    if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
        t.Fatalf("decode: %v", err)
    }

    steps, ok := payload["execution_flow_steps"].(float64)
    if !ok || int(steps) != 1 {
        t.Fatalf("expected execution_flow_steps=1, got %#v", payload["execution_flow_steps"])
    }

    flowObj, ok := payload["execution_flow"].(map[string]any)
    if !ok {
        t.Fatalf("expected execution_flow object, got %#v", payload["execution_flow"])
    }

    stepsArr, ok := flowObj["steps"].([]any)
    if !ok || len(stepsArr) != 1 {
        t.Fatalf("expected steps array with one entry, got %#v", flowObj["steps"])
    }

    entry, ok := stepsArr[0].(map[string]any)
    if !ok {
        t.Fatalf("unexpected entry type: %T", stepsArr[0])
    }
    if entry["role"] != "unit-step" {
        t.Fatalf("expected role unit-step, got %v", entry["role"])
    }
    meta, _ := entry["metadata"].(map[string]any)
    if meta == nil || meta["phase"] != "test" {
        t.Fatalf("expected metadata.phase=test, got %#v", meta)
    }
}

func TestBuildCollectAggregation(t *testing.T) {
    n := NewNode("A", ":0", nil)
    t.Cleanup(func() { _ = n.Shutdown() })

    leaderFlow := []protocol.ExecutionStep{protocol.NewExecutionStep("ibft_node", "A", "execute_leader", nil)}
    followerFlow := []protocol.ExecutionStep{protocol.NewExecutionStep("ibft_node", "B", "collect_forward", map[string]string{"leader": "http://nodea:8091"})}

    n.mu.Lock()
    n.txSubFlows = map[string][]protocol.ExecutionStep{
        normalizeParticipantDisplay("A"): leaderFlow,
        normalizeParticipantDisplay("B"): followerFlow,
    }
    n.mu.Unlock()

    items := []collectReq{
        {ParticipantID: "A", Wallet: "0xLeader", Sig: "sig-leader", Proof: merkleProofPayload{Root: "0xabc"}},
        {ParticipantID: "B", Wallet: "0xFollower", Sig: "sig-follower", Proof: merkleProofPayload{Root: "0xabc"}},
    }

    agg := n.buildCollectAggregation(2, 2, items)
    if agg == nil {
        t.Fatalf("expected aggregation")
    }
    if agg.Kind != "collect_threshold" {
        t.Fatalf("unexpected kind: %s", agg.Kind)
    }
    if agg.QuorumAchieved != 2 || agg.QuorumRequired != 2 {
        t.Fatalf("unexpected quorum: %+v", agg)
    }
    if agg.ReceivedAt.IsZero() {
        t.Fatalf("expected received timestamp")
    }
    if len(agg.Participants) != 2 {
        t.Fatalf("expected two participants, got %d", len(agg.Participants))
    }

    roles := map[string]string{}
    for _, p := range agg.Participants {
        roles[p.NodeID] = p.Role
        if p.FlowDigest == "" {
            t.Fatalf("missing flow digest for %s", p.NodeID)
        }
    }

    if roles["A"] != "leader" {
        t.Fatalf("expected leader role for A, got %q", roles["A"])
    }
    if roles["B"] != "follower" {
        t.Fatalf("expected follower role for B, got %q", roles["B"])
    }
}

func TestBuildCommitAggregationLocked(t *testing.T) {
    n := NewNode("A", ":0", nil)
    t.Cleanup(func() { _ = n.Shutdown() })
    n.isLeader = true
    n.SetNetwork(nil, []string{"A", "B", "C", "D"})

    followerStep := protocol.NewExecutionStep("ibft_node", "B", "commit_ack", map[string]string{"proposal_hash": "hash"})
    n.appendTxCommitFlow("B", followerStep)

    n.mu.Lock()
    agg := n.buildCommitAggregationLocked("hash")
    n.mu.Unlock()

    if agg == nil {
        t.Fatalf("expected commit aggregation")
    }
    if agg.Kind != "ibft_commit" {
        t.Fatalf("unexpected kind: %s", agg.Kind)
    }
    if agg.Metadata["proposal_hash"] != "hash" {
        t.Fatalf("missing proposal hash metadata: %+v", agg.Metadata)
    }
    if agg.QuorumRequired != 3 {
        t.Fatalf("expected quorum required 3, got %d", agg.QuorumRequired)
    }
    if agg.QuorumAchieved != len(agg.Participants) {
        t.Fatalf("quorum achieved should match participants: %+v", agg)
    }
    if agg.ReceivedAt.IsZero() {
        t.Fatalf("expected received timestamp")
    }
    if len(agg.Participants) != 2 {
        t.Fatalf("expected leader and follower, got %d", len(agg.Participants))
    }

    roles := map[string]string{}
    for _, p := range agg.Participants {
        roles[p.NodeID] = p.Role
        if p.Role == "follower" && p.FlowDigest == "" {
            t.Fatalf("missing follower digest")
        }
    }

    if roles[normalizeParticipantDisplay("A")] != "leader" {
        t.Fatalf("expected leader entry present, roles=%v", roles)
    }
    if roles[normalizeParticipantDisplay("B")] != "follower" {
        t.Fatalf("expected follower entry present, roles=%v", roles)
    }
}
