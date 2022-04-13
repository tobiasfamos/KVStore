package util

import (
	"golang.org/x/exp/slices"
	"math/rand"
	"testing"
)

func TestFill(t *testing.T) {
	for i := 1; i < 10; i++ {
		for j := 1; j < 10; j++ {
			slice := make([]int, i)
			Fill(slice, j)

			for _, v := range slice {
				if v != j {
					t.Errorf("Actual value %d, Expected %d", v, j)
				}
			}
		}
	}
}

func TestSwap(t *testing.T) {
	for i := 1; i < 10; i++ {
		for j := 1; j < 10; j++ {
			a := i
			b := j
			Swap(&a, &b)

			if a != j {
				t.Errorf("Actual a %d, Expected %d", a, j)
			}
			if b != i {
				t.Errorf("Actual b %d, Expected %d", b, i)
			}
		}
	}
}

func TestReplace(t *testing.T) {
	for i := 1; i < 10; i++ {
		dst := i
		old := Replace(&dst, 0)

		if dst != 0 {
			t.Errorf("Actual dst %d, Expected 0", dst)
		}
		if old != i {
			t.Errorf("Actual old %d, Expected %d", old, i)
		}
	}
}

func TestMove(t *testing.T) {
	for i := 1; i < 10; i++ {
		for j := 1; j < 10; j++ {
			src := i
			dst := 0

			Move(&dst, &src, j)
			if dst != i {
				t.Errorf("Actual dst %d, Expected %d", dst, i)
			}
			if src != j {
				t.Errorf("Actual src %d, Expected %d", src, j)
			}
		}
	}
}

func TestMoveSlice(t *testing.T) {
	for i := 1; i < 10; i++ {
		src := []int{rand.Int(), rand.Int(), rand.Int(), rand.Int(), rand.Int(), rand.Int()}
		dstExp := slices.Clone(src)
		dst := make([]int, len(src))
		fill := rand.Int()
		srcExp := Filled(slices.Clone(src), fill)

		MoveSlice(dst, src, fill)

		if !slices.Equal(dst, dstExp) {
			t.Errorf("Actual dst %d, Expected %d", dst, dstExp)
		}

		if !slices.Equal(src, srcExp) {
			t.Errorf("Actual dst %d, Expected %d", src, srcExp)
		}
	}
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
		{[]int{1, 2, 3, 4}, []int{1, 2, 3, 0}, 4, 4},
	}

	for _, test := range tests {
		ShiftLeft(test.slice, test.from, test.to, 0)
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
		{[]int{1, 2, 3, 4}, []int{0, 2, 3, 4}, 0, 0},
	}

	for _, test := range tests {
		ShiftRight(test.slice, test.from, test.to, 0)
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
