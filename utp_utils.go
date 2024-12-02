package utp_go

import (
	"math"
	"time"
)

// wrapCompareLess compares if lhs is less than rhs, taking wrapping into account.
func wrapCompareLessUint32(lhs, rhs uint32) bool {
	distDown := lhs - rhs
	distUp := rhs - lhs
	return distUp < distDown
}

// wrapCompareLess compares if lhs is less than rhs, taking wrapping into account.
func wrapCompareLessUint16(lhs, rhs uint16) bool {
	distDown := lhs - rhs
	distUp := rhs - lhs
	return distUp < distDown
}

// max returns the maximum of two durations.
func max(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// min returns the minimum of two durations.
func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minUint32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func maxUint32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func minInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func maxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func clampUint32(value, min, max uint32) uint32 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func nowMicro() uint32 {
	return uint32(time.Now().UnixMicro())
}

func duration_between(earlier uint32, later uint32) time.Duration {
	if later < earlier {
		return time.Duration(math.MaxUint32 - earlier + later)
	} else {
		return time.Duration(later - earlier)
	}
}
