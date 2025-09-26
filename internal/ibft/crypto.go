package ibft

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// KeyStore provides access to private/public keys for validators.
type KeyStore interface {
	Private(id string) ed25519.PrivateKey
	Public(id string) ed25519.PublicKey
}

// DerivedKeyStore derives keys deterministically from the validator id for demo purposes.
// Not secure; for demonstration only.
type DerivedKeyStore struct{}

func (DerivedKeyStore) seed(id string) [32]byte {
	return sha256.Sum256([]byte(id))
}

func (d DerivedKeyStore) Private(id string) ed25519.PrivateKey {
	s := d.seed(id)
	return ed25519.NewKeyFromSeed(s[:])
}

func (d DerivedKeyStore) Public(id string) ed25519.PublicKey {
	return d.Private(id).Public().(ed25519.PublicKey)
}

func canonicalBytes(m Message) []byte {
	// Serialize deterministically excluding Sig
	return []byte(fmt.Sprintf("%s|%d|%s|%s", m.Type, m.Round, m.From, m.Value))
}

func signMessage(priv ed25519.PrivateKey, m Message) string {
	sig := ed25519.Sign(priv, canonicalBytes(m))
	return hex.EncodeToString(sig)
}

func verifyMessage(pub ed25519.PublicKey, m Message) bool {
	// Decode hex signature
	b, err := hex.DecodeString(m.Sig)
	if err != nil {
		return false
	}
	return ed25519.Verify(pub, canonicalBytes(m), b)
}
