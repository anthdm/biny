package storage

type binyCache struct {
	buf       []byte
	off       int
	bootStrap [256]byte
}

func (c *binyCache) appendBytes(p ...byte) {
	n := len(c.buf)
	total := n + len(p)
	if total > cap(c.buf) {
		// TODO we can track the average size of a key value pair
		// if we have those numbers we can allocate that specific
		// lenght. In case someone stores lots of big data we avoid a lot
		// off allocation and garbage collecting
		newSize := total * 2
		newBuf := make([]byte, total, newSize)
		copy(newBuf, c.buf)
		c.buf = newBuf
	}
	c.buf = c.buf[:total]
	copy(c.buf[n:], p)
}
