package search

import "testing"

func TestBinary(t *testing.T) {
	vals := []uint32{1, 7, 12, 13, 22, 153}

	tests := []struct {
		value uint32
		index uint
		exist bool
	}{
		{1, 0, true},
		{7, 1, true},
		{12, 2, true},
		{13, 3, true},
		{22, 4, true},
		{153, 5, true},
		{0, 0, false},
		{42, 0, false},
		{154, 0, false},
	}

	for _, test := range tests {
		idx, exists := Binary(test.value, vals)

		if test.exist {
			if !exists {
				t.Errorf(
					"Expected element %d to exist, but did not",
					test.value,
				)
			}
			if idx != test.index {
				t.Errorf(
					"Expected element %d to be at index %d; but was at %d",
					test.value,
					test.index,
					idx,
				)
			}
		} else {
			if exists {
				t.Errorf(
					"Expected element %d to not exist, but did at index %d",
					test.value,
					idx,
				)
			}
		}
	}
}

func TestBinaryWithEmptySet(t *testing.T) {
	vals := []int{}

	idx, exists := Binary(42, vals)
	if exists {
		t.Errorf(
			"Expected element %d to not exist, but did at index %d",
			42,
			idx,
		)
	}
}
