package eth

import (
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// prefix used by Ethereum personal_sign
const prefix = "\x19Ethereum Signed Message:\n"

// ParsePrivateKey parses a hex-encoded private key (with or without 0x prefix).
func ParsePrivateKey(hexKey string) (*ecdsa.PrivateKey, error) {
	hk := strings.TrimSpace(hexKey)
	hk = strings.TrimPrefix(hk, "0x")
	b, err := hex.DecodeString(hk)
	if err != nil {
		return nil, err
	}
	return crypto.ToECDSA(b)
}

// AddressFromPrivate derives the Ethereum address from the given private key.
func AddressFromPrivate(priv *ecdsa.PrivateKey) string {
	addr := crypto.PubkeyToAddress(priv.PublicKey)
	return addr.Hex()
}

// personalHash computes the hash for personal_sign of arbitrary data.
func personalHash(data []byte) []byte {
	// message = prefix + len(data) + data
	m := []byte(fmt.Sprintf("%s%d", prefix, len(data)))
	m = append(m, data...)
	h := crypto.Keccak256(m)
	return h
}

// SignPersonal signs the given data with the private key using Ethereum personal_sign semantics.
// Returns the signature (65 bytes hex) and the hex address string.
func SignPersonal(data []byte, priv *ecdsa.PrivateKey) (sigHex string, addr string, err error) {
	hash := personalHash(data)
	sig, err := crypto.Sign(hash, priv)
	if err != nil {
		return "", "", err
	}
	return hex.EncodeToString(sig), AddressFromPrivate(priv), nil
}

// VerifyPersonal verifies a personal_sign style signature for the given address.
func VerifyPersonal(data []byte, sigHex string, addr string) (bool, error) {
	sigBytes, err := hex.DecodeString(strings.TrimPrefix(sigHex, "0x"))
	if err != nil {
		return false, err
	}
	if len(sigBytes) != 65 {
		return false, errors.New("invalid signature length")
	}
	hash := personalHash(data)
	pub, err := crypto.SigToPub(hash, sigBytes)
	if err != nil {
		return false, err
	}
	recovered := crypto.PubkeyToAddress(*pub)
	// Compare addresses case-insensitive
	if !strings.EqualFold(recovered.Hex(), common.HexToAddress(addr).Hex()) {
		return false, nil
	}
	return true, nil
}
