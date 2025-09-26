package main

import (
	"flag"
	"strings"
)

// parseFlags parses command-line flags into values for id, addr, peers and validators.
// It is split out for testing.
func parseFlags(args []string) (id string, addr string, peers []string, validators []string) {
	fs := flag.NewFlagSet("node", flag.ContinueOnError)
	idp := fs.String("id", "node1", "validator id (e.g., A)")
	addrp := fs.String("addr", ":8081", "listen address")
	peersp := fs.String("peers", "", "comma-separated peer base URLs, e.g. http://localhost:18082,http://localhost:18083")
	valsp := fs.String("validators", "", "comma-separated validator ids including self, e.g. A,B,C,D")
	_ = fs.Parse(args)

	var peerList []string
	if *peersp != "" {
		for _, p := range strings.Split(*peersp, ",") {
			pp := strings.TrimSpace(p)
			if pp != "" {
				peerList = append(peerList, pp)
			}
		}
	}
	var valList []string
	if *valsp != "" {
		for _, v := range strings.Split(*valsp, ",") {
			vv := strings.TrimSpace(v)
			if vv != "" {
				valList = append(valList, vv)
			}
		}
	} else {
		valList = []string{*idp}
	}
	return *idp, *addrp, peerList, valList
}
