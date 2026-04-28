package compression

// NoOpCompressor implements a no-op compressor that does not compress data
type NoOpCompressor struct{}

// NewNoOpCompressor creates a new no-op compressor
func NewNoOpCompressor() *NoOpCompressor {
	return &NoOpCompressor{}
}

// Compress returns the original data without compression
func (n *NoOpCompressor) Compress(data []byte) ([]byte, error) {
	return data, nil
}

// Decompress returns the original data without decompression
func (n *NoOpCompressor) Decompress(data []byte) ([]byte, error) {
	return data, nil
}

// Codec returns the codec type
func (n *NoOpCompressor) Codec() Codec {
	return None
}
