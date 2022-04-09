package util

import (
	"golang.org/x/exp/constraints"
)

// Fill a slice with a specific value
func Fill[T any](slice []T, val T) {
	for i := 0; i < len(slice); i++ {
		slice[i] = val
	}
}

// Filled fills a slice with a specific value and returns it
func Filled[T any](slice []T, val T) []T {
	for i := 0; i < len(slice); i++ {
		slice[i] = val
	}
	return slice
}

// Swap the content of `a` and `b`.
func Swap[T any](a *T, b *T) {
	tmp := *a
	*a = *b
	*b = tmp
}

// Replace the destination with a value and return the old value.
func Replace[T any](dst *T, val T) T {
	out := *dst
	*dst = val
	return out
}

// Move the value of src into dst and replace the src with a new value.
func Move[T any](dst *T, src *T, newSrc T) {
	*dst = *src
	*src = newSrc
}

// MoveSlice moves all values of src into dst and fills all moved src values with a new value.
// This function does NO CHECK whether the slices overlap or have different sizes.
// The caller is expected to provide two different slices with same length.
// Otherwise they may manually move the the slice content.
func MoveSlice[T any](dst []T, src []T, fillSrc T) {
	copy(dst, src)
	Fill(src, fillSrc)
}

// ShiftLeft shifts a slice range one to the left, filling the empty spot with a replacement value.
func ShiftLeft[T any, I constraints.Integer](slice []T, from I, to I, replacement T) {
	copy(slice[from-1:to-1], slice[from:to])
	slice[to-1] = replacement
}

// ShiftRight shifts a slice range one to the right, filling the empty spot with a replacement value.
func ShiftRight[T any, I constraints.Integer](slice []T, from I, to I, replacement T) {
	copy(slice[from+1:to+1], slice[from:to])
	slice[from] = replacement
}

func ShiftLeftBy[T any, I constraints.Integer](slice []T, from I, to I, shift I) {
	copy(slice[from-shift:to-shift], slice[from:to])
	for i := to - shift; i < to; i++ {
		slice[i] = *new(T)
	}
}

func ShiftRightBy[T any, I constraints.Integer](slice []T, from I, to I, shift I) {
	copy(slice[from+shift:to+shift], slice[from:to])
	for i := from; i < from+shift; i++ {
		slice[i] = *new(T)
	}
}
