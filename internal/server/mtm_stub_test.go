package server

import (
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "sort"
    "strings"
    "sync"
    "testing"
)

type mtmStub struct {
    t       *testing.T
    server  *httptest.Server
    mu      sync.RWMutex
    root    string
    proofs  map[string]merkleProofPayload
    wallets []string
}

func newMTMStub(t *testing.T, wallets []string) *mtmStub {
    t.Helper()
    stub := &mtmStub{t: t}
    stub.rebuild(wallets)
    stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
    return stub
}

func (m *mtmStub) Close() {
    m.server.Close()
}

func (m *mtmStub) URL() string {
    return m.server.URL
}

func (m *mtmStub) Proof(wallet string) merkleProofPayload {
    m.mu.RLock()
    defer m.mu.RUnlock()
    p, ok := m.proofs[strings.ToLower(wallet)]
    if !ok {
        m.t.Fatalf("proof not found for wallet %s", wallet)
    }
    return p
}

func (m *mtmStub) handle(w http.ResponseWriter, r *http.Request) {
    m.mu.RLock()
    root := m.root
    proofs := m.proofs
    m.mu.RUnlock()

    switch r.URL.Path {
    case "/merkle/root":
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]string{"root": root})
    case "/merkle/proof":
        wallet := strings.ToLower(r.URL.Query().Get("pubkey"))
        proof, ok := proofs[wallet]
        if !ok {
            http.Error(w, "not found", http.StatusNotFound)
            return
        }
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(proof)
    default:
        http.NotFound(w, r)
    }
}

func (m *mtmStub) rebuild(wallets []string) {
    m.mu.Lock()
    defer m.mu.Unlock()
    uniq := uniqueStrings(wallets)
    sort.Slice(uniq, func(i, j int) bool { return strings.ToLower(uniq[i]) < strings.ToLower(uniq[j]) })
    levels := buildMerkleLevels(uniq)
    rootHash := levels[len(levels)-1][0]
    root := hex.EncodeToString(rootHash)
    proofs := make(map[string]merkleProofPayload, len(uniq))
    for idx, wallet := range uniq {
        payload := buildProofForIndex(wallet, idx, uniq, levels)
        payload.Root = root
        proofs[strings.ToLower(wallet)] = payload
    }
    m.root = root
    m.proofs = proofs
    m.wallets = append([]string(nil), uniq...)
}

func uniqueStrings(in []string) []string {
    seen := make(map[string]struct{})
    out := make([]string, 0, len(in))
    for _, v := range in {
        if v == "" {
            continue
        }
        key := strings.ToLower(v)
        if _, ok := seen[key]; ok {
            continue
        }
        seen[key] = struct{}{}
        out = append(out, v)
    }
    return out
}

func buildMerkleLevels(leaves []string) [][][]byte {
    level := make([][]byte, len(leaves))
    for i, leaf := range leaves {
        h := sha256.Sum256([]byte(leaf))
        level[i] = h[:]
    }
    levels := [][][]byte{level}
    current := level
    for len(current) > 1 {
        next := make([][]byte, 0, (len(current)+1)/2)
        for i := 0; i < len(current); i += 2 {
            if i+1 < len(current) {
                h := sha256.Sum256(append(append([]byte{}, current[i]...), current[i+1]...))
                next = append(next, h[:])
            } else {
                next = append(next, current[i])
            }
        }
        levels = append(levels, next)
        current = next
    }
    return levels
}

func buildProofForIndex(wallet string, idx int, leaves []string, levels [][][]byte) merkleProofPayload {
    siblings := make([]string, 0)
    dirs := make([]string, 0)
    position := idx
    for level := 0; level < len(levels)-1; level++ {
        nodes := levels[level]
        if position%2 == 0 {
            if position+1 < len(nodes) {
                siblings = append(siblings, hex.EncodeToString(nodes[position+1]))
                dirs = append(dirs, "right")
            }
        } else {
            siblings = append(siblings, hex.EncodeToString(nodes[position-1]))
            dirs = append(dirs, "left")
        }
        position /= 2
    }
    return merkleProofPayload{
        Index:    idx,
        Siblings: siblings,
        Dirs:     dirs,
    }
}
