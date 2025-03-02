package utp_go

import (
	"math"
	"time"

	"github.com/valyala/fastrand"
)

func minInt(a, b int) int {
	if a < b {
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

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func ClampUint32(value, min, max uint32) uint32 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func NowMicro() uint32 {
	return uint32(time.Now().UnixMicro())
}

func DurationBetween(earlier uint32, later uint32) time.Duration {
	if later < earlier {
		return time.Duration(math.MaxUint32 - earlier + later)
	} else {
		return time.Duration(later - earlier)
	}
}

func RandomUint16() uint16 {
	return uint16(fastrand.Uint32n(65535))
}
