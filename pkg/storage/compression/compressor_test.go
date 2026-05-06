package compression

import (
	"bytes"
	"io"
	"testing"
)

func TestCodecString(t *testing.T) {
	tests := []struct {
		name     string
		codec    Codec
		expected string
	}{
		{"None", None, "NONE"},
		{"Snappy", Snappy, "SNAPPY"},
		{"Gzip", Gzip, "GZIP"},
		{"Zstd", Zstd, "ZSTD"},
		{"Unknown", Codec(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.codec.String(); got != tt.expected {
				t.Errorf("Codec.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestCodecExtension(t *testing.T) {
	tests := []struct {
		name     string
		codec    Codec
		expected string
	}{
		{"None", None, ""},
		{"Snappy", Snappy, ".snappy"},
		{"Gzip", Gzip, ".gz"},
		{"Zstd", Zstd, ".zst"},
		{"Unknown", Codec(99), ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.codec.Extension(); got != tt.expected {
				t.Errorf("Codec.Extension() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestNewCompressor(t *testing.T) {
	tests := []struct {
		name      string
		codec     Codec
		wantErr   bool
		codecType Codec
	}{
		{"None", None, false, None},
		{"Snappy", Snappy, false, Snappy},
		{"Gzip", Gzip, false, Gzip},
		{"Zstd", Zstd, false, Zstd},
		{"Unsupported", Codec(99), true, None},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressor, err := NewCompressor(tt.codec)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCompressor() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && compressor.Codec() != tt.codecType {
				t.Errorf("NewCompressor().Codec() = %v, want %v", compressor.Codec(), tt.codecType)
			}
		})
	}
}

func TestNoOpCompressor(t *testing.T) {
	compressor := NewNoOpCompressor()

	data := []byte("test data")

	compressed, err := compressor.Compress(data)
	if err != nil {
		t.Fatalf("Compress() error = %v", err)
	}
	if !bytes.Equal(compressed, data) {
		t.Errorf("Compress() = %v, want %v", compressed, data)
	}

	decompressed, err := compressor.Decompress(data)
	if err != nil {
		t.Fatalf("Decompress() error = %v", err)
	}
	if !bytes.Equal(decompressed, data) {
		t.Errorf("Decompress() = %v, want %v", decompressed, data)
	}

	if codec := compressor.Codec(); codec != None {
		t.Errorf("Codec() = %v, want %v", codec, None)
	}
}

func TestGzipCompressor(t *testing.T) {
	compressor := NewGzipCompressor()

	tests := []struct {
		name string
		data []byte
	}{
		{"Empty", []byte{}},
		{"Small", []byte("test")},
		{"Medium", bytes.Repeat([]byte("test"), 100)},
		{"Large", bytes.Repeat([]byte("test"), 10000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := compressor.Compress(tt.data)
			if err != nil {
				t.Fatalf("Compress() error = %v", err)
			}

			decompressed, err := compressor.Decompress(compressed)
			if err != nil {
				t.Fatalf("Decompress() error = %v", err)
			}

			if !bytes.Equal(decompressed, tt.data) {
				t.Errorf("Decompress() = %v, want %v", decompressed, tt.data)
			}

			if codec := compressor.Codec(); codec != Gzip {
				t.Errorf("Codec() = %v, want %v", codec, Gzip)
			}
		})
	}
}

func TestSnappyCompressor(t *testing.T) {
	compressor := NewSnappyCompressor()

	tests := []struct {
		name string
		data []byte
	}{
		{"Empty", []byte{}},
		{"Small", []byte("test")},
		{"Medium", bytes.Repeat([]byte("test"), 100)},
		{"Large", bytes.Repeat([]byte("test"), 10000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := compressor.Compress(tt.data)
			if err != nil {
				t.Fatalf("Compress() error = %v", err)
			}

			decompressed, err := compressor.Decompress(compressed)
			if err != nil {
				t.Fatalf("Decompress() error = %v", err)
			}

			if !bytes.Equal(decompressed, tt.data) {
				t.Errorf("Decompress() = %v, want %v", decompressed, tt.data)
			}

			if codec := compressor.Codec(); codec != Snappy {
				t.Errorf("Codec() = %v, want %v", codec, Snappy)
			}
		})
	}
}

func TestZstdCompressor(t *testing.T) {
	compressor := NewZstdCompressor()

	tests := []struct {
		name string
		data []byte
	}{
		{"Empty", []byte{}},
		{"Small", []byte("test")},
		{"Medium", bytes.Repeat([]byte("test"), 100)},
		{"Large", bytes.Repeat([]byte("test"), 10000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := compressor.Compress(tt.data)
			if err != nil {
				t.Fatalf("Compress() error = %v", err)
			}

			decompressed, err := compressor.Decompress(compressed)
			if err != nil {
				t.Fatalf("Decompress() error = %v", err)
			}

			if !bytes.Equal(decompressed, tt.data) {
				t.Errorf("Decompress() = %v, want %v", decompressed, tt.data)
			}

			if codec := compressor.Codec(); codec != Zstd {
				t.Errorf("Codec() = %v, want %v", codec, Zstd)
			}
		})
	}
}

func TestCompressReader(t *testing.T) {
	data := []byte("test data for compress reader")
	compressor := NewGzipCompressor()

	compressed, err := compressor.Compress(data)
	if err != nil {
		t.Fatalf("Compress() error = %v", err)
	}

	reader := NewCompressReader(bytes.NewReader(compressed), compressor)
	result, err := io.ReadAll(reader)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Errorf("ReadAll() = %v, want %v", result, data)
	}
}

func TestCompressWriter(t *testing.T) {
	data := []byte("test data for compress writer")
	compressor := NewGzipCompressor()

	var buf bytes.Buffer
	writer := NewCompressWriter(&buf, compressor)

	n, err := writer.Write(data)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if n != len(data) {
		t.Errorf("Write() = %v, want %v", n, len(data))
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	decompressed, err := compressor.Decompress(buf.Bytes())
	if err != nil {
		t.Fatalf("Decompress() error = %v", err)
	}

	if !bytes.Equal(decompressed, data) {
		t.Errorf("Decompress() = %v, want %v", decompressed, data)
	}
}

func TestCompressWriterFlush(t *testing.T) {
	data := []byte("test data for flush")
	compressor := NewGzipCompressor()

	var buf bytes.Buffer
	writer := NewCompressWriter(&buf, compressor)

	n, err := writer.Write(data)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if n != len(data) {
		t.Errorf("Write() = %v, want %v", n, len(data))
	}

	if err := writer.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if buf.Len() == 0 {
		t.Error("Flush() did not write any data")
	}

	decompressed, err := compressor.Decompress(buf.Bytes())
	if err != nil {
		t.Fatalf("Decompress() error = %v", err)
	}

	if !bytes.Equal(decompressed, data) {
		t.Errorf("Decompress() = %v, want %v", decompressed, data)
	}
}

func BenchmarkGzipCompressor(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark test data"), 1000)
	compressor := NewGzipCompressor()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressed, _ := compressor.Compress(data)
		compressor.Decompress(compressed)
	}
}

func BenchmarkSnappyCompressor(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark test data"), 1000)
	compressor := NewSnappyCompressor()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressed, _ := compressor.Compress(data)
		compressor.Decompress(compressed)
	}
}

func BenchmarkZstdCompressor(b *testing.B) {
	data := bytes.Repeat([]byte("benchmark test data"), 1000)
	compressor := NewZstdCompressor()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressed, _ := compressor.Compress(data)
		compressor.Decompress(compressed)
	}
}
