package search

import (
	"golang.org/x/exp/constraints"
)

func Binary[T constraints.Ordered](key T, values []T) (uint, bool) {
	left := 0
	right := len(values) - 1

	for left <= right {
		// Int division, so an implicit floor operation
		idx := (left + right) / 2

		if values[idx] == key {
			return uint(idx), true
		} else if key < values[idx] {
			right = idx - 1
		} else { // key > values[idx]
			left = idx + 1
		}
	}

	return 0, false
}
