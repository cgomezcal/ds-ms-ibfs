package main

import (
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "flag"
    "log"
    "net/http"
    "sort"
    "strings"
    "sync"

    "github.com/cgomezcal/ds-ms-ibfs/internal/eth"
)

type proofPayload struct {
    Root     string   `json:"root"`
    Index    int      `json:"index"`
    Siblings []string `json:"siblings"`
    Dirs     []string `json:"dirs"`
}

type mtmServer struct {
    mu     sync.RWMutex
    root   string
    proofs map[string]proofPayload
}

func newMTMServer(wallets []string) *mtmServer {
    srv := &mtmServer{}
    srv.rebuild(wallets)
    return srv
}

func (m *mtmServer) rebuild(wallets []string) {
    uniq := uniqueStrings(wallets)
    sort.Slice(uniq, func(i, j int) bool { return strings.ToLower(uniq[i]) < strings.ToLower(uniq[j]) })
    levels := buildMerkleLevels(uniq)
    rootHash := levels[len(levels)-1][0]
    root := hex.EncodeToString(rootHash)
    proofs := make(map[string]proofPayload, len(uniq))
    for idx, wallet := range uniq {
        payload := buildProofForIndex(wallet, idx, uniq, levels)
        payload.Root = root
        proofs[strings.ToLower(wallet)] = payload
    }
    m.mu.Lock()
    defer m.mu.Unlock()
    m.root = root
    m.proofs = proofs
}

func (m *mtmServer) handle(w http.ResponseWriter, r *http.Request) {
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

func uniqueStrings(in []string) []string {
    seen := make(map[string]struct{}, len(in))
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

func buildProofForIndex(wallet string, idx int, leaves []string, levels [][][]byte) proofPayload {
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
    return proofPayload{
        Index:    idx,
        Siblings: siblings,
        Dirs:     dirs,
    }
}

func main() {
    listenAddr := flag.String("addr", ":8081", "HTTP listen address")
    flag.Parse()

    keys := []string{
        "0xb71c71a67e1177ad4e901695e1b4b9c2d68e8f0e2b8a9d17a2a4d4a8e6f5f2f0",
        "0x4f3edf983ac636a65a842ce7c78d9aa706d3b113bce9c46f30d7d21715b23b1d",
        "0x8f2a559490b8d6e3d2b1016da0fbdc3f5b6b6f9bcd3d2e2c2a2b1a1a0a9a8a7a",
        "0x5c51f7b14a6b3c2e9d8a7f6e5d4c3b2a190817161514131211100f0e0d0c0b0a",
    }
    wallets := make([]string, 0, len(keys))
    for _, k := range keys {
        priv, err := eth.ParsePrivateKey(k)
        if err != nil {
            log.Fatalf("failed to parse key %s: %v", k, err)
        }
        wallets = append(wallets, eth.AddressFromPrivate(priv))
    }

    srv := newMTMServer(wallets)
    mux := http.NewServeMux()
    mux.HandleFunc("/merkle/root", srv.handle)
    mux.HandleFunc("/merkle/proof", srv.handle)

    log.Printf("MTM stub listening on %s with %d wallets", *listenAddr, len(wallets))
    if err := http.ListenAndServe(*listenAddr, mux); err != nil {
        log.Fatalf("server error: %v", err)
    }
}
