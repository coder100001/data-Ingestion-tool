package compression

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

// GzipCompressor implements gzip compression
type GzipCompressor struct {
	level int
}

// NewGzipCompressor creates a new gzip compressor
func NewGzipCompressor() *GzipCompressor {
	return &GzipCompressor{level: gzip.DefaultCompression}
}

// Compress compresses data using gzip
func (g *GzipCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := gzip.NewWriterLevel(&buf, g.level)
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
func (g *GzipCompressor) Decompress(data []byte) ([]byte, error) {
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
func (g *GzipCompressor) Codec() Codec {
	return Gzip
}
