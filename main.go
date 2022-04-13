package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/tobiasfamos/KVStore/kv"
)

const memoryLimit = 100_000_000 // 100 MB

func main() {
	args := os.Args[1:]
	if c := len(args); c != 1 {
		help()
	}

	dir := args[0]
	fmt.Printf("Loading KV store from %s\n", dir)
	cli, err := NewCLI(dir)
	if err != nil {
		abort(fmt.Sprintf("Error loading KV store: %v\nMake sure the target directory exists.\n", err))
	}

	for {
		cmd := prompt(fmt.Sprintf("KV Store @ %s>", dir))
		response, cont := cli.Handle(cmd)
		fmt.Println(response)
		if !cont {
			os.Exit(0)
		}
	}

	err = cli.Close()
	if err != nil {
		abort(fmt.Sprintf("Error closing KV store: %v\n", err))
	}

}

func prompt(label string) string {
	var out string

	r := bufio.NewReader(os.Stdin)
	for {
		fmt.Fprint(os.Stderr, label+" ")
		out, _ = r.ReadString('\n')
		if out != "" {
			break
		}
	}

	return strings.TrimSpace(out)
}

type CLI struct {
	store *kv.BTree
}

func NewCLI(dir string) (*CLI, error) {
	cli := CLI{}
	cli.store = &kv.BTree{}

	err := cli.store.Create(
		kv.KvStoreConfig{
			MemorySize:       memoryLimit,
			WorkingDirectory: dir,
		},
	)

	if err != nil {
		return &cli, err
	}

	return &cli, nil
}

func (cli *CLI) Close() error {
	return cli.store.Close()
}

func (cli *CLI) Handle(cmd string) (string, bool) {
	parts := strings.Split(cmd, " ")

	switch parts[0] {
	case "get":
		if len(parts) != 2 {
			return cli.Help(), true
		}

		keyString := parts[1]
		key, err := strconv.ParseUint(keyString, 10, 64)
		if err != nil {
			return fmt.Sprintf("Invalid key %s: %v", keyString, err), true
		}

		val, err := cli.store.Get(key)
		if err != nil {
			return fmt.Sprintf("Error retrieving key: %v", err), true
		}

		return fmt.Sprintf("%d = %x", key, val), true

	case "set":
		if len(parts) != 3 {
			return cli.Help(), true
		}

		keyString := parts[1]
		key, err := strconv.ParseUint(keyString, 10, 64)
		if err != nil {
			return fmt.Sprintf("Invalid key %s: %v", keyString, err), true
		}

		valString := parts[2]
		if len(valString) < 2 || valString[0:2] != "0x" {
			return fmt.Sprintf("Invalid value: Must be hex-encoded with leading 0x prefix"), true
		}
		valString = valString[2:]

		val, err := hex.DecodeString(valString)
		if err != nil {
			return fmt.Sprintf("Invalid hex-encoded string: %v", err), true
		}

		if len(val) > 10 {
			return fmt.Sprintf("Value must be 10 bytes at most, was %d", len(val)), true
		}

		valAry := [10]byte{}
		copy(valAry[:], val)
		err = cli.store.Put(key, valAry)
		if err != nil {
			return fmt.Sprintf("Error storing key: %v", err), true
		}

		return fmt.Sprintf("Successfully stored %d = %x", key, valAry), true

	case "exit":
		err := cli.Close()
		if err == nil {
			return "KV store successfully closed", false
		} else {
			return fmt.Sprintf("Error closing KV store: %v", err), false
		}
	default:
		return cli.Help(), true
	}
}

func (cli *CLI) Help() string {
	out := ""
	out += "Valid commands:\n"
	out += "\n"
	out += "\tget <key>\n"
	out += "\tExample: get 123\n"
	out += "\n"
	out += "\tset <key> <value>\n"
	out += "\tExample: set 123 0x4242\n"
	out += "\n"
	out += "\texit\n"

	return out
}

func help() {
	fmt.Println("Usage: ./KVStore <persistence_directory>")
	os.Exit(2)
}

func abort(msg string) {
	fmt.Printf("Error: %s\n", msg)
	os.Exit(1)
}
