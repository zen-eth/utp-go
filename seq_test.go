package utp_go

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContainsStart(t *testing.T) {
	prop := func(start, end uint16) bool {
		rangeInclusive := newCircularRangeInclusive(start, end)
		return rangeInclusive.Contains(start)
	}

	if !prop(0, 65535) {
		t.Error("TestContainsStart failed")
	}
}

func TestContainsEnd(t *testing.T) {
	prop := func(start, end uint16) bool {
		rangeInclusive := newCircularRangeInclusive(start, end)
		return rangeInclusive.Contains(end)
	}

	if !prop(0, 65535) {
		t.Error("TestContainsEnd failed")
	}
}

func TestIterator(t *testing.T) {
	prop := func(start, end uint16) bool {
		rangeInclusive := newCircularRangeInclusive(start, end)

		var length int
		expectedIdx := start
		for {
			idx, ok := rangeInclusive.Next()
			if !ok {
				break
			}
			require.Equal(t, expectedIdx, idx, "Expected %v, got %v", expectedIdx, idx)
			expectedIdx += 1
			length += 1
		}

		var expectedLen int
		if start <= end {
			expectedLen = int(end-start) + 1
		} else {
			expectedLen = int(65535-start) + int(end) + 2
		}
		if length != expectedLen {
			t.Errorf("Expected length %v, got %v", expectedLen, length)
			return false
		}

		return true
	}

	if !prop(0, 65535) {
		t.Error("TestIterator failed")
	}
}

func TestIteratorSingle(t *testing.T) {
	prop := func(x uint16) bool {
		rangeInclusive := newCircularRangeInclusive(x, x)
		val, ok := rangeInclusive.Next()
		if !ok || val != x {
			t.Errorf("Expected %v, got %v", x, val)
			return false
		}
		if _, ok := rangeInclusive.Next(); ok {
			t.Error("Expected no more elements")
			return false
		}

		return true
	}

	if !prop(0) {
		t.Error("TestIteratorSingle failed")
	}
}
