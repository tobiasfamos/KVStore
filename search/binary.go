package search

import (
	"golang.org/x/exp/constraints"
)

// Binary performs an exact binary search on a sorted slice.
//
// The second return value indicates whether a matching value was found. If so,
// the first return value indicates the index within the `values` slice where
// the value was found.
//
// If the value was not found, the returned index indicates the index of the
// *next largest* value in the slice. Be mindful that for an array of length
// `n`, with indices in `[0, n-1]`, this returned next-larger index will thus
// be in `[1, n]`.
func Binary[T constraints.Ordered](key T, values []T) (uint, bool) {
	idx := 0
	left := 0
	right := len(values) - 1

	for left <= right {
		// Int division, so an implicit floor operation
		idx = (left + right) / 2

		if values[idx] == key {
			return uint(idx), true
		} else if key < values[idx] {
			right = idx - 1
		} else { // key > values[idx]
			left = idx + 1
		}
	}

	// No exact match found. Now we'll ensure that the returned index in
	// this case is the one of the next *greater* element.
	if len(values) > 0 && values[idx] < key {
		idx++
	}

	return uint(idx), false
}
