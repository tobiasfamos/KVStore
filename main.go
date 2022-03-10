package main

import (
	"fmt"
	"github.com/tobiasfamos/KVStore/kv"
	"os"
	"strconv"
)

func main() {
	args := os.Args[1:]
	if c := len(args); c != 2 {
		fmt.Printf("expected 2 but got %d args\n", c)
		os.Exit(1)
	}

	var err error
	var val int
	if val, err = strconv.Atoi(args[0]); err != nil {
		fmt.Printf("first argument should be numeric instead of %s\n", args[0])
	}

	var str string = args[1]

	// myif, err := NewExampleKv(val, str)
	_, err = kv.NewKvStoreInstance(val, str)
	if err != nil {
		fmt.Printf("could not create kv interface: %s\n", err)
		os.Exit(1)
	}

	fmt.Println("mykv successfully created")
	os.Exit(0)
}
