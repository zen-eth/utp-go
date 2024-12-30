package utp_go

// circularRangeInclusive represents a range bounded inclusively below and above that supports wrapping arithmetic.
type circularRangeInclusive struct {
	start     uint16
	end       uint16
	exhausted bool
}

// newCircularRangeInclusive returns a new circularRangeInclusive.
func newCircularRangeInclusive(start, end uint16) *circularRangeInclusive {
	return &circularRangeInclusive{
		start:     start,
		end:       end,
		exhausted: false,
	}
}

// Start returns the start of the range (inclusive).
func (r *circularRangeInclusive) Start() uint16 {
	return r.start
}

// End returns the end of the range (inclusive).
func (r *circularRangeInclusive) End() uint16 {
	return r.end
}

// Contains returns true if item is contained in the range.
func (r *circularRangeInclusive) Contains(item uint16) bool {
	if r.end >= r.start {
		return item >= r.start && item <= r.end
	} else if item >= r.start {
		return true
	} else {
		return item <= r.end
	}
}

// Next returns the next item in the range, or nil if the range is exhausted.
func (r *circularRangeInclusive) Next() (uint16, bool) {
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
