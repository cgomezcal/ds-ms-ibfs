package protocol

import (
	"testing"
	"time"
)

func TestNewExecutionStepIncludesHostMetadata(t *testing.T) {
	meta := map[string]string{"stage": "unit", "empty": ""}
	step := NewExecutionStep("component-x", "instance-1", "role-y", meta)

	if step.Type != ExecutionStepType {
		t.Fatalf("unexpected type: %s", step.Type)
	}
	if step.Component != "component-x" {
		t.Fatalf("unexpected component: %s", step.Component)
	}
	if step.InstanceID != "instance-1" {
		t.Fatalf("expected instance-1, got %s", step.InstanceID)
	}
	if step.Hostname == "" {
		t.Fatalf("hostname should not be empty")
	}
	if step.Timestamp.IsZero() {
		t.Fatalf("timestamp should be set")
	}
	if time.Since(step.Timestamp) > time.Second {
		t.Fatalf("timestamp too old: %v", step.Timestamp)
	}
	if step.Metadata == nil {
		t.Fatalf("metadata should include pid")
	}
	if step.Metadata["pid"] == "" {
		t.Fatalf("pid metadata missing")
	}
	if step.Metadata["stage"] != "unit" {
		t.Fatalf("expected metadata stage=unit, got %s", step.Metadata["stage"])
	}
	if _, ok := step.Metadata["empty"]; ok {
		t.Fatalf("empty metadata values should be omitted")
	}
}

func TestCloneExecutionFlowProducesCopy(t *testing.T) {
	original := []ExecutionStep{
		NewExecutionStep("c1", "i1", "r1", nil),
		NewExecutionStep("c2", "i2", "r2", map[string]string{"k": "v"}),
	}

	clone := CloneExecutionFlow(original)
	if len(clone) != len(original) {
		t.Fatalf("clone length mismatch: %d vs %d", len(clone), len(original))
	}

	if clone[0].Type != ExecutionStepType || clone[1].Type != ExecutionStepType {
		t.Fatalf("clone should have execution_step type: %+v", clone)
	}

	if &clone[0] == &original[0] {
		t.Fatalf("clone should not share backing array")
	}

	clone[0].Component = "mutated"
	if original[0].Component == "mutated" {
		t.Fatalf("mutating clone should not affect original")
	}

	empty := CloneExecutionFlow(nil)
	if empty != nil {
		t.Fatalf("nil clone should stay nil")
	}
}

func TestCloneExecutionFlowEnvelope(t *testing.T) {
	steps := []ExecutionStep{NewExecutionStep("alpha", "inst", "role", nil)}
	env := ExecutionFlow{Type: ExecutionFlowType, Steps: steps}
	clone := CloneExecutionFlowEnvelope(env)

	if clone.Type != ExecutionFlowType {
		t.Fatalf("expected execution flow type, got %s", clone.Type)
	}
	if &clone.Steps == &env.Steps {
		t.Fatalf("steps slice should be copied")
	}

	cloneNil := CloneExecutionFlowEnvelope(ExecutionFlow{})
	if cloneNil.Type != ExecutionFlowType {
		t.Fatalf("empty clone should still have type execution_flow")
	}
	if cloneNil.Steps != nil {
		t.Fatalf("empty envelope should stay empty")
	}
}
