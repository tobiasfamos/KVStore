package util

import (
	"golang.org/x/exp/slices"
	"testing"
	"unsafe"
)

func TestBool(t *testing.T) {
	println(unsafe.Sizeof(new(bool)))
}

func TestShiftLeft(t *testing.T) {
	tests := []struct {
		slice    []int
		expected []int
		from     int
		to       int
	}{
		{[]int{1, 2, 3, 4}, []int{2, 3, 4, 0}, 1, 4},
		{[]int{1, 2, 3, 4}, []int{1, 3, 4, 0}, 2, 4},
		{[]int{1, 2, 3, 4}, []int{1, 2, 3, 4}, 3, 3},
	}

	for _, test := range tests {
		ShiftLeft(test.slice, test.from, test.to)
		if !slices.Equal(test.slice, test.expected) {
			t.Errorf("Actual slice = %d, Expected %d", test.slice, test.expected)
		}
	}
}

func TestShiftRight(t *testing.T) {
	tests := []struct {
		slice    []int
		expected []int
		from     int
		to       int
	}{
		{[]int{1, 2, 3, 4}, []int{0, 1, 2, 3}, 0, 3},
		{[]int{1, 2, 3, 4}, []int{0, 1, 2, 4}, 0, 2},
		{[]int{1, 2, 3, 4}, []int{1, 2, 3, 4}, 3, 3},
	}

	for _, test := range tests {
		ShiftRight(test.slice, test.from, test.to)
		if !slices.Equal(test.slice, test.expected) {
			t.Errorf("Actual slice = %d, Expected %d", test.slice, test.expected)
		}
	}
}

func TestShiftLeftBy(t *testing.T) {
	tests := []struct {
		slice    []int
		expected []int
		from     int
		to       int
		shift    int
	}{
		{[]int{1, 2, 3, 4, 5, 6, 7, 8}, []int{5, 6, 7, 8, 0, 0, 0, 0}, 4, 8, 4},
		{[]int{1, 2, 3, 4, 5, 6, 7, 8}, []int{1, 4, 5, 6, 0, 0, 7, 8}, 3, 6, 2},
		{[]int{1, 2, 3, 4, 5, 6, 7, 8}, []int{2, 3, 4, 5, 6, 7, 8, 0}, 1, 8, 1},
	}

	for _, test := range tests {
		ShiftLeftBy(test.slice, test.from, test.to, test.shift)
		if !slices.Equal(test.slice, test.expected) {
			t.Errorf("Actual slice = %d, Expected %d", test.slice, test.expected)
		}
	}
}

func TestShiftRightBy(t *testing.T) {
	tests := []struct {
		slice    []int
		expected []int
		from     int
		to       int
		shift    int
	}{
		{[]int{1, 2, 3, 4, 5, 6, 7, 8}, []int{0, 0, 0, 0, 1, 2, 3, 4}, 0, 4, 4},
		{[]int{1, 2, 3, 4, 5, 6, 7, 8}, []int{1, 2, 0, 0, 3, 4, 5, 8}, 2, 5, 2},
		{[]int{1, 2, 3, 4, 5, 6, 7, 8}, []int{0, 1, 2, 3, 4, 5, 6, 7}, 0, 7, 1},
	}

	for _, test := range tests {
		ShiftRightBy(test.slice, test.from, test.to, test.shift)
		if !slices.Equal(test.slice, test.expected) {
			t.Errorf("Actual slice = %d, Expected %d", test.slice, test.expected)
		}
	}
}
