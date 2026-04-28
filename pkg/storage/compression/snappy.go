package compression

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

// FastGzipCompressor implements a fast compression algorithm using gzip at BestSpeed.
// This provides fast compression/decompression performance similar to Snappy,
// while producing standard gzip-compatible output.
type FastGzipCompressor struct {
	level int
}

// NewFastGzipCompressor creates a new fast compressor using gzip best speed
func NewFastGzipCompressor() *FastGzipCompressor {
	return &FastGzipCompressor{level: gzip.BestSpeed}
}

// Compress compresses data using gzip at best speed level
func (s *FastGzipCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := gzip.NewWriterLevel(&buf, s.level)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip writer: %w", err)
	}

	if _, err := writer.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write data: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	return buf.Bytes(), nil
}

// Decompress decompresses data using gzip
func (s *FastGzipCompressor) Decompress(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer reader.Close()

	result, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read decompressed data: %w", err)
	}

	return result, nil
}

// Codec returns the codec type
func (s *FastGzipCompressor) Codec() Codec {
	return Snappy
}

// SnappyCompressor is an alias for FastGzipCompressor for backward compatibility.
// Deprecated: Use FastGzipCompressor instead. This is not a true Snappy implementation.
type SnappyCompressor = FastGzipCompressor

// NewSnappyCompressor is an alias for NewFastGzipCompressor for backward compatibility.
// Deprecated: Use NewFastGzipCompressor instead.
func NewSnappyCompressor() *SnappyCompressor {
	return NewFastGzipCompressor()
}
