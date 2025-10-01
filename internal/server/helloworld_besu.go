package server

import (
	"context"
	"crypto/ecdsa"
	"log"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Replace with your deployed contract address
const HelloWorldAddress = "0xYourHelloWorldContractAddress"

// Replace with your contract ABI
const HelloWorldABI = `[
  {"inputs":[{"internalType":"string","name":"_message","type":"string"}],"stateMutability":"nonpayable","type":"constructor"},
  {"inputs":[],"name":"getMessage","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},
  {"inputs":[{"internalType":"string","name":"_message","type":"string"}],"name":"setMessage","outputs":[],"stateMutability":"nonpayable","type":"function"},
  {"inputs":[],"name":"message","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"}
]`

// SendHelloWorldTx sends a setMessage transaction to the HelloWorld contract on Besu
func SendHelloWorldTx(rpcURL, privKeyHex, message string) error {
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return err
	}
	defer client.Close()

	privKey, err := crypto.HexToECDSA(privKeyHex)
	if err != nil {
		return err
	}
	fromAddr := crypto.PubkeyToAddress(privKey.PublicKey)

	nonce, err := client.PendingNonceAt(context.Background(), fromAddr)
	if err != nil {
		return err
	}

	gasPrice, err := client.SuggestGasPrice(context.Background())
	if err != nil {
		return err
	}

	parsedABI, err := abi.JSON(strings.NewReader(HelloWorldABI))
	if err != nil {
		return err
	}

	input, err := parsedABI.Pack("setMessage", message)
	if err != nil {
		return err
	}

	toAddr := common.HexToAddress(HelloWorldAddress)
	var value *big.Int = big.NewInt(0)
	gasLimit := uint64(200000)

	tx := types.NewTransaction(nonce, toAddr, value, gasLimit, gasPrice, input)

	chainID, err := client.NetworkID(context.Background())
	if err != nil {
		return err
	}

	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), privKey)
	if err != nil {
		return err
	}

	err = client.SendTransaction(context.Background(), signedTx)
	if err != nil {
		return err
	}

	log.Printf("HelloWorld setMessage tx sent: %s", signedTx.Hash().Hex())
	return nil
}

// Example usage: call from leader node when authorized
func CallHelloWorldFromLeader(msg string) {
	rpc := os.Getenv("BESU_RPC_URL") // e.g. http://besu-dev.sirt-xfsc.click:8545
	priv := os.Getenv("ETH_PRIVATE_KEY")
	if rpc == "" || priv == "" {
		log.Println("Missing BESU_RPC_URL or ETH_PRIVATE_KEY")
		return
	}
	if err := SendHelloWorldTx(rpc, priv, msg); err != nil {
		log.Printf("Error sending HelloWorld tx: %v", err)
	}
}
