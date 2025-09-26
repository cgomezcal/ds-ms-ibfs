package ibft

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

// Minimal IBFT-like engine for demo/testing purposes only.
// It models a single round with N>=4 validators and f = floor((N-1)/3).

type Broadcaster interface {
	Broadcast(msg Message)
}

type Engine struct {
	mu         sync.Mutex
	id         string
	validators []string
	f          int
	round      Round
	value      string
	prepares   map[string]bool
	commits    map[string]bool
	state      string // idle, prepared, committed
	broadcast  Broadcaster
	ks         KeyStore
}

func NewEngine(id string, validators []string, b Broadcaster) *Engine {
	f := (len(validators) - 1) / 3
	return &Engine{
		id:         id,
		validators: validators,
		f:          f,
		round:      0,
		prepares:   make(map[string]bool),
		commits:    make(map[string]bool),
		state:      "idle",
		broadcast:  b,
		ks:         DerivedKeyStore{},
	}
}

func (e *Engine) IsValidator(id string) bool {
	for _, v := range e.validators {
		if v == id {
			return true
		}
	}
	return false
}

func (e *Engine) quorum() int { return 2*e.f + 1 }

func (e *Engine) Propose(value string) (Message, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.IsValidator(e.id) {
		return Message{}, errors.New("not a validator")
	}
	if e.state != "idle" {
		return Message{}, errors.New("round already in progress")
	}
	e.value = valueHash(value)
	// Broadcast PrePrepare to peers
	pp := Message{Type: PrePrepare, Round: e.round, From: e.id, Value: e.value}
	pp.Sig = signMessage(e.ks.Private(e.id), pp)
	if e.broadcast != nil {
		e.broadcast.Broadcast(pp)
	}
	// The proposer also prepares its own value
	e.prepares[e.id] = true
	prep := Message{Type: Prepare, Round: e.round, From: e.id, Value: e.value}
	prep.Sig = signMessage(e.ks.Private(e.id), prep)
	if e.broadcast != nil {
		e.broadcast.Broadcast(prep)
	}
	return pp, nil
}

func (e *Engine) Handle(msg Message) (string, *Message, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.IsValidator(msg.From) {
		return e.state, nil, fmt.Errorf("unknown validator %s", msg.From)
	}
	// verify signature
	if msg.Sig == "" || !verifyMessage(e.ks.Public(msg.From), msg) {
		return e.state, nil, errors.New("invalid signature")
	}
	if msg.Round != e.round {
		return e.state, nil, fmt.Errorf("unexpected round %d", msg.Round)
	}

	switch msg.Type {
	case PrePrepare:
		if e.state != "idle" {
			return e.state, nil, nil
		}
		// accept value and send prepare
		e.value = msg.Value
		prep := Message{Type: Prepare, Round: e.round, From: e.id, Value: e.value}
		prep.Sig = signMessage(e.ks.Private(e.id), prep)
		// self-count prepare
		e.prepares[e.id] = true
		if e.broadcast != nil {
			e.broadcast.Broadcast(prep)
		}
		return e.state, &prep, nil
	case Prepare:
		if e.state != "idle" && e.state != "prepared" {
			return e.state, nil, nil
		}
		e.prepares[msg.From] = true
		if len(e.prepares) >= e.quorum() && e.state == "idle" {
			e.state = "prepared"
			com := Message{Type: Commit, Round: e.round, From: e.id, Value: e.value}
			com.Sig = signMessage(e.ks.Private(e.id), com)
			// self-count commit
			e.commits[e.id] = true
			if e.broadcast != nil {
				e.broadcast.Broadcast(com)
			}
			return e.state, &com, nil
		}
		return e.state, nil, nil
	case Commit:
		// In strict IBFT, commits are processed after prepared. For demo/testing,
		// allow adopting the value and self-committing if a commit is seen first.
		if e.value == "" && msg.Value != "" {
			// adopt value if missing
			e.value = msg.Value
		}
		if e.state == "idle" {
			// adopt prepared state implicitly and self-commit
			e.state = "prepared"
			if !e.commits[e.id] {
				e.commits[e.id] = true
				com := Message{Type: Commit, Round: e.round, From: e.id, Value: e.value}
				com.Sig = signMessage(e.ks.Private(e.id), com)
				if e.broadcast != nil {
					e.broadcast.Broadcast(com)
				}
			}
		}
		e.commits[msg.From] = true
		if len(e.commits) >= e.quorum() && e.state != "committed" {
			e.state = "committed"
		}
		return e.state, nil, nil
	default:
		return e.state, nil, fmt.Errorf("unknown message type %s", msg.Type)
	}
}

func (e *Engine) State() string { e.mu.Lock(); defer e.mu.Unlock(); return e.state }
func (e *Engine) Round() Round  { e.mu.Lock(); defer e.mu.Unlock(); return e.round }
func (e *Engine) Value() string { e.mu.Lock(); defer e.mu.Unlock(); return e.value }

// SetBroadcaster allows wiring peers after construction (useful for tests/demo).
func (e *Engine) SetBroadcaster(b Broadcaster) { e.mu.Lock(); e.broadcast = b; e.mu.Unlock() }

// SetValidators updates the validator set and recalculates f.
func (e *Engine) SetValidators(vs []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.validators = append([]string(nil), vs...)
	e.f = (len(e.validators) - 1) / 3
}

// ValidatorCount returns the number of validators in the current set.
func (e *Engine) ValidatorCount() int { e.mu.Lock(); defer e.mu.Unlock(); return len(e.validators) }

func valueHash(v string) string {
	h := sha256.Sum256([]byte(v))
	return hex.EncodeToString(h[:])
}

func demoSig(id string, r Round, v string) string {
	// Not secure: just a placeholder deterministic signature
	return valueHash(fmt.Sprintf("%s|%d|%s", id, r, v))
}
