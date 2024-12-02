package utp_go

// CircularRangeInclusive represents a range bounded inclusively below and above that supports wrapping arithmetic.
type CircularRangeInclusive struct {
	start     uint16
	end       uint16
	exhausted bool
}

// NewCircularRangeInclusive returns a new CircularRangeInclusive.
func NewCircularRangeInclusive(start, end uint16) CircularRangeInclusive {
	return CircularRangeInclusive{
		start:     start,
		end:       end,
		exhausted: false,
	}
}

// Start returns the start of the range (inclusive).
func (r CircularRangeInclusive) Start() uint16 {
	return r.start
}

// End returns the end of the range (inclusive).
func (r CircularRangeInclusive) End() uint16 {
	return r.end
}

// Contains returns true if item is contained in the range.
func (r CircularRangeInclusive) Contains(item uint16) bool {
	if r.end >= r.start {
		return item >= r.start && item <= r.end
	} else if item >= r.start {
		return true
	} else {
		return item <= r.end
	}
}

// Next returns the next item in the range, or nil if the range is exhausted.
func (r CircularRangeInclusive) Next() (uint16, bool) {
	if r.exhausted {
		return 0, false
	} else if r.start == r.end {
		r.exhausted = true
		return r.end, true
	} else {
		step := r.start + 1
		current := r.start
		r.start = step
		return current, true
	}
}
