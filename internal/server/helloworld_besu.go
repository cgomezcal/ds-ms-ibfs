package server

import (
	"context"
	"errors"
	"log"
	"math/big"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Replace with your deployed contract address
const HelloWorldAddress = "0x914b73cc9c6f74F4DBB3a6b2e5ee274cBCEB5Cc3"

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

	// Remove 0x prefix if present
	cleanPrivKey := privKeyHex
	if strings.HasPrefix(cleanPrivKey, "0x") {
		cleanPrivKey = cleanPrivKey[2:]
	}
	
	log.Printf("DEBUG: privKeyHex='%s', cleanPrivKey='%s'", privKeyHex, cleanPrivKey)

	privKey, err := crypto.HexToECDSA(cleanPrivKey)
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

	log.Printf("IBFT HelloWorld setMessage tx sent: %s", signedTx.Hash().Hex())
	return nil
}

// CallHelloWorldFromLeader calls HelloWorld contract from IBFT leader when consensus completes
func CallHelloWorldFromLeader(msg string) (string, error) {
	rpcURL := os.Getenv("BESU_RPC_URL")
	if rpcURL == "" {
		rpcURL = "http://besu-dev.sirt-xfsc.click:8545"
	}

	privKeyHex := os.Getenv("ETH_PRIVATE_KEY")
	if privKeyHex == "" {
		err := errors.New("IBFT HelloWorld: ETH_PRIVATE_KEY not set")
		log.Printf(err.Error())
		return "", err
	}

	if strings.HasPrefix(privKeyHex, "0x") {
		privKeyHex = privKeyHex[2:]
	}

	log.Printf("IBFT: Calling contract with message: %s", msg)

	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		log.Printf("IBFT HelloWorld RPC connection failed: %v", err)
		return "", err
	}
	defer client.Close()

	privKey, err := crypto.HexToECDSA(privKeyHex)
	if err != nil {
		log.Printf("IBFT HelloWorld private key parse failed: %v", err)
		return "", err
	}

	fromAddr := crypto.PubkeyToAddress(privKey.PublicKey)
	nonce, err := client.PendingNonceAt(context.Background(), fromAddr)
	if err != nil {
		log.Printf("IBFT HelloWorld nonce fetch failed: %v", err)
		return "", err
	}

	gasPrice, err := client.SuggestGasPrice(context.Background())
	if err != nil {
		log.Printf("IBFT HelloWorld gas price fetch failed: %v", err)
		return "", err
	}

	parsedABI, err := abi.JSON(strings.NewReader(HelloWorldABI))
	if err != nil {
		log.Printf("IBFT HelloWorld ABI parse failed: %v", err)
		return "", err
	}

	input, err := parsedABI.Pack("setMessage", msg)
	if err != nil {
		log.Printf("IBFT HelloWorld function pack failed: %v", err)
		return "", err
	}

	toAddr := common.HexToAddress(HelloWorldAddress)
	tx := types.NewTransaction(nonce, toAddr, big.NewInt(0), 200000, gasPrice, input)

	chainID, err := client.NetworkID(context.Background())
	if err != nil {
		log.Printf("IBFT HelloWorld chain ID fetch failed: %v", err)
		return "", err
	}

	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), privKey)
	if err != nil {
		log.Printf("IBFT HelloWorld transaction signing failed: %v", err)
		return "", err
	}

	if err := client.SendTransaction(context.Background(), signedTx); err != nil {
		log.Printf("IBFT HelloWorld transaction send failed: %v", err)
		return "", err
	}

	txHash := signedTx.Hash().Hex()
	log.Printf("IBFT HelloWorld SUCCESS: tx %s, message: %s", txHash, msg)
	return txHash, nil
}
