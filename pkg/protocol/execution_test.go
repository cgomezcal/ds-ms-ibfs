package protocol

import (
    "testing"
    "time"
)

func TestNewExecutionStepIncludesHostMetadata(t *testing.T) {
    meta := map[string]string{"stage": "unit", "empty": ""}
    step := NewExecutionStep("component-x", "instance-1", "role-y", meta)

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
    sub := []ExecutionSubflow{{
        Source: "wallet-1",
        Steps:  []ExecutionStep{NewExecutionStep("peer", "nodeB", "collect_forward", map[string]string{"leader": "http://nodea:8091"})},
    }}
    env := ExecutionFlow{Steps: steps, Subflows: sub}
    clone := CloneExecutionFlowEnvelope(env)

    if &clone.Steps == &env.Steps {
        t.Fatalf("steps slice should be copied")
    }
    if &clone.Subflows == &env.Subflows {
        t.Fatalf("subflows slice should be copied")
    }
    if len(clone.Subflows) != 1 {
        t.Fatalf("expected one subflow, got %d", len(clone.Subflows))
    }
    clone.Subflows[0].Steps[0].Component = "mutated"
    if env.Subflows[0].Steps[0].Component == "mutated" {
        t.Fatalf("mutating clone should not affect source")
    }

    cloneNil := CloneExecutionFlowEnvelope(ExecutionFlow{})
    if cloneNil.Steps != nil || cloneNil.Subflows != nil {
        t.Fatalf("empty envelope should stay empty")
    }
}
