package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/cgomezcal/ds-ms-ibfs/internal/server"
)

func main() {
	id, addr, peerList, valList := parseFlags(os.Args[1:])
	n := server.NewNode(id, addr, peerList)
	// Wire network info (peers + validators)
	n.SetNetwork(peerList, valList)
	if tok := os.Getenv("PEER_TOKEN"); tok != "" {
		n.SetAuthToken(tok)
	}
	if l := os.Getenv("LEADER_URL"); l != "" {
		isLeader := false
		if v := os.Getenv("IS_LEADER"); v == "true" || v == "1" {
			isLeader = true
		}
		n.SetLeader(l, isLeader)
	}
	if pk := os.Getenv("ETH_PRIVATE_KEY"); pk != "" {
		n.SetPrivateKey(pk)
	}
	if mtm := os.Getenv("MTM_BASE_URL"); mtm != "" {
		n.SetMTMBaseURL(mtm)
	}
	log.Printf("starting node %s on %s with %d peers and %d validators", id, addr, len(peerList), len(valList))

	// Run server in background to support graceful shutdown on signals
	errCh := make(chan error, 1)
	go func() { errCh <- n.Start() }()

	// Wait for termination signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		log.Printf("received signal %s, shutting down...", sig)
		if err := n.Shutdown(); err != nil {
			log.Printf("shutdown error: %v", err)
		}
		log.Printf("shutdown complete")
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}
}
