package util

import "golang.org/x/exp/constraints"

func Shift[T any, I constraints.Integer](slice []T, from I, to I, direction I) {
	if direction >= 0 {
		ShiftRight(slice, from, to)
	} else {
		ShiftLeft(slice, from, to)
	}
}

func ShiftLeft[T any, I constraints.Integer](slice []T, from I, to I) {
	copy(slice[from-1:to-1], slice[from:to])
	if from != to {
		slice[to-1] = *new(T)
	}
}

func ShiftRight[T any, I constraints.Integer](slice []T, from I, to I) {
	copy(slice[from+1:to+1], slice[from:to])
	if from != to {
		slice[from] = *new(T)
	}
}

func ShiftBy[T any, I constraints.Integer](slice []T, from I, to I, shift I) {
	if shift >= 0 {
		ShiftRightBy(slice, from, to, shift)
	} else {
		ShiftLeftBy(slice, from, to, -shift)
	}
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
