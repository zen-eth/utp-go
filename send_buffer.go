package utp_go

type SendBuffer struct {
	pending [][]byte
	offset  int
	size    int
}

func NewSendBuffer(size int) *SendBuffer {
	return &SendBuffer{
		pending: make([][]byte, 0),
		offset:  0,
		size:    size,
	}
}

func (sb *SendBuffer) Available() int {
	used := 0
	for _, data := range sb.pending {
		used += len(data)
	}
	return sb.size + sb.offset - used
}

func (sb *SendBuffer) IsEmpty() bool {
	return len(sb.pending) == 0
}

func (sb *SendBuffer) Write(data []byte) (int, error) {
	available := sb.Available()
	if len(data) <= available {
		sb.pending = append(sb.pending, data)
		return len(data), nil
	} else {
		sb.pending = append(sb.pending, data[:available])
		return available, nil
	}
}

func (sb *SendBuffer) Read(buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}

	if len(sb.pending) == 0 {
		return 0, nil
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

	return n, nil
}
