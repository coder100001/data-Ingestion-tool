package parquet

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"data-ingestion-tool/pkg/logger"
)

// FileHeader represents the header of a columnar file
type FileHeader struct {
	Magic        [4]byte `json:"magic"`
	Version      int32   `json:"version"`
	SchemaOffset int64   `json:"schema_offset"`
	SchemaSize   int64   `json:"schema_size"`
	NumRows      int64   `json:"num_rows"`
	NumRowGroups int32   `json:"num_row_groups"`
	CreatedAt    int64   `json:"created_at"`
}

// ColumnChunk represents a chunk of column data
type ColumnChunk struct {
	ColumnName  string `json:"column_name"`
	ColumnType  int32  `json:"column_type"`
	NumValues   int64  `json:"num_values"`
	DataOffset  int64  `json:"data_offset"`
	DataSize    int64  `json:"data_size"`
	NullCount   int64  `json:"null_count"`
	MinValue    []byte `json:"min_value,omitempty"`
	MaxValue    []byte `json:"max_value,omitempty"`
	Compression int32  `json:"compression"`
}

// RowGroupMetadata represents metadata for a row group
type RowGroupMetadata struct {
	NumRows   int64         `json:"num_rows"`
	TotalSize int64         `json:"total_size"`
	Columns   []ColumnChunk `json:"columns"`
}

// FileFooter represents the footer of a columnar file
type FileFooter struct {
	RowGroups []RowGroupMetadata `json:"row_groups"`
	Schema    *Schema            `json:"schema"`
}

const (
	// FileMagic is the magic number for columnar files
	FileMagic = "COL1"
	// CurrentVersion is the current file format version
	CurrentVersion = 1
	// DefaultRowGroupSize is the default number of rows per row group
	DefaultRowGroupSize = 10000
)

// Writer writes data in columnar format
type Writer struct {
	schema         *Schema
	file           *os.File
	bufferedWriter *bufio.Writer
	path           string
	logger         *logger.Logger

	// Row group buffering
	rowGroupSize int
	buffer       []map[string]interface{}
	numRows      int64

	// Statistics
	rowGroups  []RowGroupMetadata
	nullCounts map[string]int64
	minValues  map[string]interface{}
	maxValues  map[string]interface{}

	mu     sync.Mutex
	closed bool
}

// WriterConfig contains configuration for the writer
type WriterConfig struct {
	RowGroupSize int
	Schema       *Schema
}

// NewWriter creates a new columnar file writer
func NewWriter(path string, schema *Schema, logger *logger.Logger) (*Writer, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	writer := &Writer{
		schema:         schema,
		file:           file,
		bufferedWriter: bufio.NewWriterSize(file, 256*1024), // 256KB buffer
		path:           path,
		logger:         logger,
		rowGroupSize:   DefaultRowGroupSize,
		buffer:         make([]map[string]interface{}, 0, DefaultRowGroupSize),
		nullCounts:     make(map[string]int64),
		minValues:      make(map[string]interface{}),
		maxValues:      make(map[string]interface{}),
		rowGroups:      make([]RowGroupMetadata, 0),
	}

	// Write placeholder header
	if err := writer.writeHeader(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	return writer, nil
}

// WriteRow writes a single row
func (w *Writer) WriteRow(row map[string]interface{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	w.buffer = append(w.buffer, row)
	w.numRows++

	// Update statistics
	for _, col := range w.schema.Columns {
		value, exists := row[col.Name]
		if !exists || value == nil {
			w.nullCounts[col.Name]++
			continue
		}

		// Update min/max
		if currentMin, exists := w.minValues[col.Name]; !exists || compareValues(value, currentMin) < 0 {
			w.minValues[col.Name] = value
		}
		if currentMax, exists := w.maxValues[col.Name]; !exists || compareValues(value, currentMax) > 0 {
			w.maxValues[col.Name] = value
		}
	}

	// Flush if buffer is full
	if len(w.buffer) >= w.rowGroupSize {
		return w.flushRowGroup()
	}

	return nil
}

// WriteRows writes multiple rows
func (w *Writer) WriteRows(rows []map[string]interface{}) error {
	for _, row := range rows {
		if err := w.WriteRow(row); err != nil {
			return err
		}
	}
	return nil
}

// Flush flushes any buffered data
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	if len(w.buffer) > 0 {
		return w.flushRowGroup()
	}

	return w.bufferedWriter.Flush()
}

// Close closes the writer and writes the footer
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	// Flush remaining data
	if len(w.buffer) > 0 {
		if err := w.flushRowGroup(); err != nil {
			return err
		}
	}

	// Write footer
	if err := w.writeFooter(); err != nil {
		return err
	}

	// Update header with final stats
	if err := w.updateHeader(); err != nil {
		return err
	}

	// Flush and close
	if err := w.bufferedWriter.Flush(); err != nil {
		return err
	}

	if err := w.file.Close(); err != nil {
		return err
	}

	w.closed = true
	w.logger.WithFields(map[string]interface{}{
		"path":       w.path,
		"num_rows":   w.numRows,
		"row_groups": len(w.rowGroups),
	}).Info("Columnar file writer closed")

	return nil
}

// writeHeader writes the file header
func (w *Writer) writeHeader() error {
	header := FileHeader{
		Version:      CurrentVersion,
		NumRows:      0,
		NumRowGroups: 0,
		CreatedAt:    time.Now().Unix(),
	}
	copy(header.Magic[:], FileMagic)

	return binary.Write(w.bufferedWriter, binary.LittleEndian, header)
}

// updateHeader updates the header with final statistics
func (w *Writer) updateHeader() error {
	// Seek to beginning
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	header := FileHeader{
		Version:      CurrentVersion,
		NumRows:      w.numRows,
		NumRowGroups: int32(len(w.rowGroups)),
		CreatedAt:    time.Now().Unix(),
	}
	copy(header.Magic[:], FileMagic)

	return binary.Write(w.file, binary.LittleEndian, header)
}

// flushRowGroup writes buffered rows as a row group
func (w *Writer) flushRowGroup() error {
	if len(w.buffer) == 0 {
		return nil
	}

	rowGroup := RowGroupMetadata{
		NumRows: int64(len(w.buffer)),
		Columns: make([]ColumnChunk, 0, len(w.schema.Columns)),
	}

	// Write each column
	for _, col := range w.schema.Columns {
		columnData := make([]interface{}, len(w.buffer))
		nullCount := int64(0)

		for i, row := range w.buffer {
			value, exists := row[col.Name]
			if !exists || value == nil {
				columnData[i] = nil
				nullCount++
			} else {
				converted, err := col.Type.ConvertValue(value)
				if err != nil {
					w.logger.WithError(err).WithField("column", col.Name).Warn("Failed to convert value")
					columnData[i] = value
				} else {
					columnData[i] = converted
				}
			}
		}

		// Write column data
		dataOffset, err := w.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		dataSize, err := w.writeColumnData(columnData, col.Type)
		if err != nil {
			return fmt.Errorf("failed to write column %s: %w", col.Name, err)
		}

		chunk := ColumnChunk{
			ColumnName:  col.Name,
			ColumnType:  int32(col.Type),
			NumValues:   int64(len(columnData)),
			DataOffset:  dataOffset,
			DataSize:    dataSize,
			NullCount:   nullCount,
			Compression: 0, // No compression for now
		}

		// Add min/max values if available
		if minVal, exists := w.minValues[col.Name]; exists {
			chunk.MinValue = valueToBytes(minVal)
		}
		if maxVal, exists := w.maxValues[col.Name]; exists {
			chunk.MaxValue = valueToBytes(maxVal)
		}

		rowGroup.Columns = append(rowGroup.Columns, chunk)
		rowGroup.TotalSize += dataSize
	}

	w.rowGroups = append(w.rowGroups, rowGroup)
	w.buffer = w.buffer[:0] // Clear buffer

	w.logger.WithFields(map[string]interface{}{
		"rows":      rowGroup.NumRows,
		"columns":   len(rowGroup.Columns),
		"size":      rowGroup.TotalSize,
		"row_group": len(w.rowGroups),
	}).Debug("Flushed row group")

	return nil
}

// writeColumnData writes column data in columnar format
func (w *Writer) writeColumnData(data []interface{}, dataType Type) (int64, error) {
	// Write null bitmap
	nullBitmap := make([]byte, (len(data)+7)/8)
	for i, v := range data {
		if v == nil {
			nullBitmap[i/8] |= 1 << (i % 8)
		}
	}
	if err := binary.Write(w.bufferedWriter, binary.LittleEndian, nullBitmap); err != nil {
		return 0, err
	}

	// Write values based on type
	switch dataType {
	case TypeBoolean:
		boolData := make([]bool, len(data))
		for i, v := range data {
			if v != nil {
				boolData[i] = v.(bool)
			}
		}
		for _, b := range boolData {
			if err := binary.Write(w.bufferedWriter, binary.LittleEndian, b); err != nil {
				return 0, err
			}
		}

	case TypeInt32:
		for _, v := range data {
			if v != nil {
				val := int32(0)
				switch tv := v.(type) {
				case int32:
					val = tv
				case int:
					val = int32(tv)
				case int64:
					val = int32(tv)
				}
				if err := binary.Write(w.bufferedWriter, binary.LittleEndian, val); err != nil {
					return 0, err
				}
			}
		}

	case TypeInt64:
		for _, v := range data {
			if v != nil {
				val := int64(0)
				switch tv := v.(type) {
				case int64:
					val = tv
				case int:
					val = int64(tv)
				case int32:
					val = int64(tv)
				}
				if err := binary.Write(w.bufferedWriter, binary.LittleEndian, val); err != nil {
					return 0, err
				}
			}
		}

	case TypeFloat:
		for _, v := range data {
			if v != nil {
				val := float32(0)
				switch tv := v.(type) {
				case float32:
					val = tv
				case float64:
					val = float32(tv)
				case int:
					val = float32(tv)
				}
				if err := binary.Write(w.bufferedWriter, binary.LittleEndian, val); err != nil {
					return 0, err
				}
			}
		}

	case TypeDouble:
		for _, v := range data {
			if v != nil {
				val := float64(0)
				switch tv := v.(type) {
				case float64:
					val = tv
				case float32:
					val = float64(tv)
				case int:
					val = float64(tv)
				}
				if err := binary.Write(w.bufferedWriter, binary.LittleEndian, val); err != nil {
					return 0, err
				}
			}
		}

	case TypeByteArray, TypeFixedLenByteArray:
		// Write lengths and data
		for _, v := range data {
			if v != nil {
				str := fmt.Sprintf("%v", v)
				length := int32(len(str))
				if err := binary.Write(w.bufferedWriter, binary.LittleEndian, length); err != nil {
					return 0, err
				}
				if _, err := w.bufferedWriter.WriteString(str); err != nil {
					return 0, err
				}
			}
		}
	}

	endOffset, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	return endOffset, nil
}

// writeFooter writes the file footer
func (w *Writer) writeFooter() error {
	footer := FileFooter{
		RowGroups: w.rowGroups,
		Schema:    w.schema,
	}

	footerData, err := json.Marshal(footer)
	if err != nil {
		return fmt.Errorf("failed to marshal footer: %w", err)
	}

	// Write footer size
	footerSize := int64(len(footerData))
	if err := binary.Write(w.bufferedWriter, binary.LittleEndian, footerSize); err != nil {
		return err
	}

	// Write footer data
	if _, err := w.bufferedWriter.Write(footerData); err != nil {
		return err
	}

	return nil
}

// NumRows returns the total number of rows written
func (w *Writer) NumRows() int64 {
	return w.numRows
}

// Path returns the file path
func (w *Writer) Path() string {
	return w.path
}
