package ibft

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

type Round uint64

type MsgType string

const (
	PrePrepare MsgType = "PREPREPARE"
	Prepare    MsgType = "PREPARE"
	Commit     MsgType = "COMMIT"
)

type Message struct {
	Type  MsgType `json:"type"`
	Round Round   `json:"round"`
	From  string  `json:"from"`
	Value string  `json:"value"` // payload hash or value
	Sig   string  `json:"sig"`   // demo signature
}

func (m Message) Hash() string {
	b, _ := json.Marshal(m)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func (m Message) String() string {
	return fmt.Sprintf("%s r=%d from=%s val=%s", m.Type, m.Round, m.From, m.Value)
}
