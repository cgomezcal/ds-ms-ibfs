package protocol

import (
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	ExecutionStepType                   = "execution_step"
	ExecutionAggregationType            = "execution_aggregation"
	ExecutionAggregationParticipantType = "execution_aggregation_participant"
	ExecutionFlowType                   = "execution_flow"
)

// ExecutionStep describe un paso del flujo de ejecución cross-servicio.
type ExecutionStep struct {
	Type        string                `json:"execution_type"`
	Component   string                `json:"component"`
	InstanceID  string                `json:"instance_id"`
	Hostname    string                `json:"hostname"`
	IP          string                `json:"ip,omitempty"`
	Version     string                `json:"version,omitempty"`
	Role        string                `json:"role,omitempty"`
	Timestamp   time.Time             `json:"timestamp"`
	Metadata    map[string]string     `json:"metadata,omitempty"`
	Aggregation *ExecutionAggregation `json:"aggregation,omitempty"`
}

// ExecutionAggregation encapsula la información detallada de un cuórum alcanzado.
type ExecutionAggregation struct {
	Type           string                            `json:"type"`
	Kind           string                            `json:"kind"`
	QuorumAchieved int                               `json:"quorum_achieved"`
	QuorumRequired int                               `json:"quorum_required"`
	ReceivedAt     time.Time                         `json:"received_at"`
	Participants   []ExecutionAggregationParticipant `json:"participants"`
	Metadata       map[string]string                 `json:"metadata,omitempty"`
}

// ExecutionAggregationParticipant describe el aporte individual de cada nodo al cuórum.
type ExecutionAggregationParticipant struct {
	Type       string            `json:"type"`
	NodeID     string            `json:"node_id"`
	Role       string            `json:"role,omitempty"`
	FlowDigest string            `json:"flow_digest,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	Events     []ExecutionStep   `json:"events,omitempty"`
}

// ExecutionFlow encapsula la traza principal de la ejecución agregada.
type ExecutionFlow struct {
	Type  string          `json:"type"`
	Steps []ExecutionStep `json:"steps"`
}

// CloneExecutionFlow devuelve una copia superficial del slice de pasos.
func CloneExecutionFlow(src []ExecutionStep) []ExecutionStep {
	if len(src) == 0 {
		return nil
	}
	dst := make([]ExecutionStep, len(src))
	for i := range src {
		dst[i] = src[i]
		dst[i].Type = ExecutionStepType
		if src[i].Aggregation != nil {
			dst[i].Aggregation = CloneExecutionAggregation(src[i].Aggregation)
		}
		if len(src[i].Metadata) > 0 {
			copied := make(map[string]string, len(src[i].Metadata))
			for k, v := range src[i].Metadata {
				copied[k] = v
			}
			dst[i].Metadata = copied
		}
	}
	return dst
}

// CloneExecutionFlowEnvelope devuelve una copia profunda del ExecutionFlow.
func CloneExecutionFlowEnvelope(src ExecutionFlow) ExecutionFlow {
	return ExecutionFlow{
		Type:  ExecutionFlowType,
		Steps: CloneExecutionFlow(src.Steps),
	}
}

// CloneExecutionAggregation devuelve una copia profunda de la agregación.
func CloneExecutionAggregation(src *ExecutionAggregation) *ExecutionAggregation {
	if src == nil {
		return nil
	}
	agg := &ExecutionAggregation{
		Type:           ExecutionAggregationType,
		Kind:           src.Kind,
		QuorumAchieved: src.QuorumAchieved,
		QuorumRequired: src.QuorumRequired,
		ReceivedAt:     src.ReceivedAt,
	}
	if len(src.Metadata) > 0 {
		agg.Metadata = make(map[string]string, len(src.Metadata))
		for k, v := range src.Metadata {
			agg.Metadata[k] = v
		}
	}
	if len(src.Participants) > 0 {
		agg.Participants = make([]ExecutionAggregationParticipant, len(src.Participants))
		for i := range src.Participants {
			agg.Participants[i] = cloneAggregationParticipant(src.Participants[i])
		}
	}
	return agg
}

func cloneAggregationParticipant(src ExecutionAggregationParticipant) ExecutionAggregationParticipant {
	participant := ExecutionAggregationParticipant{
		Type:       ExecutionAggregationParticipantType,
		NodeID:     src.NodeID,
		Role:       src.Role,
		FlowDigest: src.FlowDigest,
		Events:     CloneExecutionFlow(src.Events),
	}
	if len(src.Metadata) > 0 {
		participant.Metadata = make(map[string]string, len(src.Metadata))
		for k, v := range src.Metadata {
			participant.Metadata[k] = v
		}
	}
	return participant
}

// NewExecutionStep crea un paso estándar enriqueciendo con metadatos de host.
func NewExecutionStep(component, instanceID, role string, metadata map[string]string) ExecutionStep {
	info := hostInfo()
	if instanceID == "" {
		instanceID = info.hostname
	}
	step := ExecutionStep{
		Type:       ExecutionStepType,
		Component:  component,
		InstanceID: instanceID,
		Hostname:   info.hostname,
		IP:         info.ip,
		Version:    info.version,
		Role:       role,
		Timestamp:  time.Now().UTC(),
	}
	if info.pid != 0 || len(metadata) > 0 {
		step.Metadata = make(map[string]string, len(metadata)+1)
	}
	if info.pid != 0 {
		step.Metadata["pid"] = strconv.Itoa(info.pid)
	}
	for k, v := range metadata {
		if v == "" {
			continue
		}
		step.Metadata[k] = v
	}
	if len(step.Metadata) == 0 {
		step.Metadata = nil
	}
	return step
}

type hostSnapshot struct {
	hostname string
	ip       string
	version  string
	pid      int
}

var (
	hostOnce sync.Once
	hostData hostSnapshot
)

func hostInfo() hostSnapshot {
	hostOnce.Do(func() {
		hostname, _ := os.Hostname()
		ip := firstNonLoopbackIP()
		version := strings.TrimSpace(os.Getenv("SERVICE_VERSION"))
		if version == "" {
			version = "unknown"
		}
		hostData = hostSnapshot{
			hostname: hostname,
			ip:       ip,
			version:  version,
			pid:      os.Getpid(),
		}
	})
	return hostData
}

func firstNonLoopbackIP() string {
	ifs, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range ifs {
		if (iface.Flags & net.FlagUp) == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue
			}
			return ip.String()
		}
	}
	return ""
}
