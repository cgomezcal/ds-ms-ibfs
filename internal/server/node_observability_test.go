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

	pendingType, ok := payload["pending_type"].(string)
	if !ok || pendingType != "" {
		t.Fatalf("expected pending_type empty string, got %#v", payload["pending_type"])
	}
	lastType, ok := payload["last_launched_type"].(string)
	if !ok || lastType != "" {
		t.Fatalf("expected last_launched_type empty string, got %#v", payload["last_launched_type"])
	}

	steps, ok := payload["execution_flow_steps"].(float64)
	if !ok || int(steps) != 1 {
		t.Fatalf("expected execution_flow_steps=1, got %#v", payload["execution_flow_steps"])
	}

	flowObj, ok := payload["execution_flow"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution_flow object, got %#v", payload["execution_flow"])
	}
	if flowObj["type"] != protocol.ExecutionFlowType {
		t.Fatalf("expected execution_flow type, got %v", flowObj["type"])
	}

	stepsArr, ok := flowObj["steps"].([]any)
	if !ok || len(stepsArr) != 1 {
		t.Fatalf("expected steps array with one entry, got %#v", flowObj["steps"])
	}

	entry, ok := stepsArr[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected entry type: %T", stepsArr[0])
	}
	if entry["execution_type"] != protocol.ExecutionStepType {
		t.Fatalf("expected execution step execution_type, got %v", entry["execution_type"])
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

	items := []collectReq{
		{ParticipantID: "A", Wallet: "0xLeader", Sig: "sig-leader", Proof: merkleProofPayload{Root: "0xabc"}, Type: "contract-call"},
		{ParticipantID: "B", Wallet: "0xFollower", Sig: "sig-follower", Proof: merkleProofPayload{Root: "0xabc"}, Type: "contract-call"},
	}

	agg := n.buildCollectAggregation(2, 2, items)
	if agg == nil {
		t.Fatalf("expected aggregation")
	}
	if agg.Kind != "collect_threshold" {
		t.Fatalf("unexpected kind: %s", agg.Kind)
	}
	if agg.Type != protocol.ExecutionAggregationType {
		t.Fatalf("expected aggregation type, got %s", agg.Type)
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
		if p.Type != protocol.ExecutionAggregationParticipantType {
			t.Fatalf("expected participant type, got %s", p.Type)
		}
		roles[p.NodeID] = p.Role
		if p.Metadata["type"] != "contract-call" {
			t.Fatalf("expected metadata type contract-call, got %#v", p.Metadata)
		}
		if p.FlowDigest != "" {
			t.Fatalf("expected empty flow digest, got %q", p.FlowDigest)
		}
	}

	if roles["A"] != "leader" {
		t.Fatalf("expected leader role for A, got %q", roles["A"])
	}
	if roles["B"] != "follower" {
		t.Fatalf("expected follower role for B, got %q", roles["B"])
	}
}
