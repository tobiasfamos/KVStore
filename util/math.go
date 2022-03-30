package util

import "golang.org/x/exp/constraints"

func Min[T constraints.Ordered](a T, b T) T {
	if a <= b {
		return a
	} else {
		return b
	}
}

func Max[T constraints.Ordered](a T, b T) T {
	if a >= b {
		return a
	} else {
		return b
	}
}
