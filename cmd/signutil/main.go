package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/cgomezcal/ds-ms-ibfs/internal/eth"
)

func main() {
	pk := os.Getenv("ETH_PRIVATE_KEY")
	data := os.Getenv("DATA")
	if pk == "" || data == "" {
		log.Fatalf("ETH_PRIVATE_KEY and DATA env vars required")
	}
	priv, err := eth.ParsePrivateKey(pk)
	if err != nil {
		log.Fatalf("parse key: %v", err)
	}
	sig, addr, err := eth.SignPersonal([]byte(data), priv)
	if err != nil {
		log.Fatalf("sign: %v", err)
	}
	out := map[string]string{
		"data":          data,
		"public_wallet": addr,
		"firma_content": sig,
	}
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
}
