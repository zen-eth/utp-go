package utp_go

type sendBuffer struct {
	pending [][]byte
	offset  int
	size    int
}

func newSendBuffer(size int) *sendBuffer {
	return &sendBuffer{
		pending: make([][]byte, 0),
		offset:  0,
		size:    size,
	}
}

func (sb *sendBuffer) Available() int {
	used := 0
	for _, data := range sb.pending {
		used += len(data)
	}
	return sb.size + sb.offset - used
}

func (sb *sendBuffer) IsEmpty() bool {
	return len(sb.pending) == 0
}

func (sb *sendBuffer) Write(data []byte) int {
	available := sb.Available()
	if len(data) <= available {
		sb.pending = append(sb.pending, data)
		return len(data)
	} else {
		sb.pending = append(sb.pending, data[:available])
		return available
	}
}

func (sb *sendBuffer) Read(buf []byte) int {
	if len(buf) == 0 {
		return 0
	}

	if len(sb.pending) == 0 {
		return 0
	}

	data := sb.pending[0]
	n := minInt(len(data)-sb.offset, len(buf))
	copy(buf, data[sb.offset:sb.offset+n])

	if sb.offset+n == len(data) {
		sb.offset = 0
		sb.pending = sb.pending[1:]
	} else {
		sb.offset += n
	}

	return n
}
