package kv

import (
	"errors"
	"fmt"
)

const MaxMem = 1 << (10 * 3) // 1 GB
const defPath = "."          // create in local directory

type intfExample interface {
	Foo(a1 int, a2 [10]byte)
	Bar(a1 int) [10]byte
}

func NewIntfInst(size int, path string) (*intfExample, error) {
	if size > MaxMem || size == 0 {
		return nil, errors.New("'size' is out of range")
	}
	if len(path) == 0 {
		fmt.Println(path)
		return nil, errors.New("'path' is not valid")
	}
	return nil, nil
}
