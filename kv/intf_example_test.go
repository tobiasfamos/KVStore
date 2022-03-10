package kv

import (
	"testing"
)

func TestIntfSize(t *testing.T) {

	tests := []struct {
		size int
		fail bool
	}{
		{0, false},
		{100, false},
		{MaxMem + 1, true},
	}

	for _, test := range tests {
		_, err := NewIntfInst(test.size, ".")
		if (err != nil) != test.fail {
			t.Errorf("Size = %d, Expected fail == %t", test.size, test.fail)
		}
	}
}
