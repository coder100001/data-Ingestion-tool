package compression

import (
	"fmt"
	"io"
)

// Codec represents a compression codec
type Codec int32

const (
	// None no compression
	None Codec = iota
	// Snappy snappy compression
	Snappy
	// Gzip gzip compression
	Gzip
	// Zstd zstd compression
	Zstd
)

// String returns the string representation of the codec
func (c Codec) String() string {
	switch c {
	case None:
		return "NONE"
	case Snappy:
		return "SNAPPY"
	case Gzip:
		return "GZIP"
	case Zstd:
		return "ZSTD"
	default:
		return "UNKNOWN"
	}
}

// Extension returns the file extension for the codec
func (c Codec) Extension() string {
	switch c {
	case None:
		return ""
	case Snappy:
		return ".snappy"
	case Gzip:
		return ".gz"
	case Zstd:
		return ".zst"
	default:
		return ""
	}
}

// Compressor defines the interface for compression algorithms
type Compressor interface {
	// Compress compresses data
	Compress(data []byte) ([]byte, error)
	// Decompress decompresses data
	Decompress(data []byte) ([]byte, error)
	// Codec returns the codec type
	Codec() Codec
}

// NewCompressor creates a compressor based on codec type
func NewCompressor(codec Codec) (Compressor, error) {
	switch codec {
	case None:
		return NewNoOpCompressor(), nil
	case Snappy:
		return NewSnappyCompressor(), nil
	case Gzip:
		return NewGzipCompressor(), nil
	case Zstd:
		return NewZstdCompressor(), nil
	default:
		return nil, fmt.Errorf("unsupported compression codec: %v", codec)
	}
}

// CompressReader wraps a reader with compression
type CompressReader struct {
	reader     io.Reader
	compressor Compressor
	buffer     []byte
}

// NewCompressReader creates a new compress reader
func NewCompressReader(reader io.Reader, compressor Compressor) *CompressReader {
	return &CompressReader{
		reader:     reader,
		compressor: compressor,
		buffer:     make([]byte, 0, 64*1024),
	}
}

// Read reads compressed data
func (cr *CompressReader) Read(p []byte) (n int, err error) {
	// Read all data
	if len(cr.buffer) == 0 {
		data, err := io.ReadAll(cr.reader)
		if err != nil {
			return 0, err
		}

		decompressed, err := cr.compressor.Decompress(data)
		if err != nil {
			return 0, err
		}

		cr.buffer = decompressed
	}

	// Copy to output
	n = copy(p, cr.buffer)
	cr.buffer = cr.buffer[n:]

	if len(cr.buffer) == 0 {
		return n, io.EOF
	}

	return n, nil
}

// CompressWriter wraps a writer with compression
type CompressWriter struct {
	writer     io.Writer
	compressor Compressor
	buffer     []byte
}

// NewCompressWriter creates a new compress writer
func NewCompressWriter(writer io.Writer, compressor Compressor) *CompressWriter {
	return &CompressWriter{
		writer:     writer,
		compressor: compressor,
		buffer:     make([]byte, 0, 64*1024),
	}
}

// Write writes data to be compressed
func (cw *CompressWriter) Write(p []byte) (n int, err error) {
	cw.buffer = append(cw.buffer, p...)
	return len(p), nil
}

// Flush flushes and compresses data
func (cw *CompressWriter) Flush() error {
	if len(cw.buffer) == 0 {
		return nil
	}

	compressed, err := cw.compressor.Compress(cw.buffer)
	if err != nil {
		return err
	}

	_, err = cw.writer.Write(compressed)
	return err
}

// Close closes the writer and flushes remaining data
func (cw *CompressWriter) Close() error {
	if err := cw.Flush(); err != nil {
		return err
	}

	if closer, ok := cw.writer.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}
