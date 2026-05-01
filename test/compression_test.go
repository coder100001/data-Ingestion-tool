package test

import (
	"testing"

	"data-ingestion-tool/pkg/storage/compression"
)

func TestCompressors(t *testing.T) {
	testData := []byte(`{"id": 1, "name": "Alice", "description": "This is a test string that should be compressed well because it contains repeated patterns and common words that compression algorithms can optimize"}`)

	compressors := []struct {
		name       string
		compressor compression.Compressor
	}{
		{"none", compression.NewNoOpCompressor()},
		{"snappy", compression.NewSnappyCompressor()},
		{"gzip", compression.NewGzipCompressor()},
		{"zstd", compression.NewZstdCompressor()},
	}

	for _, tc := range compressors {
		t.Run(tc.name, func(t *testing.T) {
			// Compress
			compressed, err := tc.compressor.Compress(testData)
			if err != nil {
				t.Fatalf("Failed to compress: %v", err)
			}

			// Calculate compression ratio
			ratio := float64(len(compressed)) / float64(len(testData))
			t.Logf("%s: original=%d, compressed=%d, ratio=%.2f%%",
				tc.name, len(testData), len(compressed), ratio*100)

			// Decompress
			decompressed, err := tc.compressor.Decompress(compressed)
			if err != nil {
				t.Fatalf("Failed to decompress: %v", err)
			}

			// Verify
			if string(decompressed) != string(testData) {
				t.Fatal("Decompressed data does not match original")
			}

			// For none compressor, sizes should be equal
			if tc.name == "none" && len(compressed) != len(testData) {
				t.Fatal("No-op compressor should not change data size")
			}
		})
	}
}

func TestCompressionRatio(t *testing.T) {
	// Generate repetitive data for better compression
	repetitiveData := make([]byte, 0, 10000)
	for i := 0; i < 1000; i++ {
		repetitiveData = append(repetitiveData, []byte("user_id,username,email,created_at\n")...)
	}

	compressors := []struct {
		name       string
		compressor compression.Compressor
	}{
		{"gzip", compression.NewGzipCompressor()},
		{"zstd", compression.NewZstdCompressor()},
	}

	for _, tc := range compressors {
		compressed, err := tc.compressor.Compress(repetitiveData)
		if err != nil {
			t.Fatalf("Failed to compress: %v", err)
		}

		ratio := float64(len(compressed)) / float64(len(repetitiveData))
		t.Logf("%s compression ratio: %.2f%% (original: %d, compressed: %d)",
			tc.name, ratio*100, len(repetitiveData), len(compressed))

		// Expect at least 50% compression for repetitive data
		if ratio > 0.5 {
			t.Logf("Warning: %s compression ratio is high: %.2f%%", tc.name, ratio*100)
		}
	}
}
