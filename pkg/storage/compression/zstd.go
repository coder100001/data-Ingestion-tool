package compression

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
)

// HighCompressionZlibCompressor implements high compression using zlib at BestCompression.
// This provides high compression ratio similar to Zstd, while producing standard zlib-compatible output.
type HighCompressionZlibCompressor struct {
	level int
}

// NewHighCompressionZlibCompressor creates a new high-compression compressor using zlib
func NewHighCompressionZlibCompressor() *HighCompressionZlibCompressor {
	return &HighCompressionZlibCompressor{level: zlib.BestCompression}
}

// Compress compresses data using zlib at best compression level
func (z *HighCompressionZlibCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, err := zlib.NewWriterLevel(&buf, z.level)
	if err != nil {
		return nil, fmt.Errorf("failed to create zlib writer: %w", err)
	}

	if _, err := writer.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write data: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	return buf.Bytes(), nil
}

// Decompress decompresses data using zlib
func (z *HighCompressionZlibCompressor) Decompress(data []byte) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create zlib reader: %w", err)
	}
	defer reader.Close()

	result, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read decompressed data: %w", err)
	}

	return result, nil
}

// Codec returns the codec type
func (z *HighCompressionZlibCompressor) Codec() Codec {
	return Zstd
}

// SetLevel sets the compression level
func (z *HighCompressionZlibCompressor) SetLevel(level int) {
	z.level = level
}

// ZstdCompressor is an alias for HighCompressionZlibCompressor for backward compatibility.
// Deprecated: Use HighCompressionZlibCompressor instead. This is not a true Zstd implementation.
type ZstdCompressor = HighCompressionZlibCompressor

// NewZstdCompressor is an alias for NewHighCompressionZlibCompressor for backward compatibility.
// Deprecated: Use NewHighCompressionZlibCompressor instead.
func NewZstdCompressor() *ZstdCompressor {
	return NewHighCompressionZlibCompressor()
}
