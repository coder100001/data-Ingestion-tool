package parquet

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/util"
)

// Reader reads data from columnar format files
type Reader struct {
	file           *os.File
	bufferedReader *bufio.Reader
	path           string
	logger         *logger.Logger

	header       FileHeader
	footer       FileFooter
	footerOffset int64

	// Current read position
	currentRowGroup int
	currentRow      int64
	rowGroupData    []map[string]interface{}

	closed bool
}

// NewReader creates a new columnar file reader
func NewReader(path string, logger *logger.Logger) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	reader := &Reader{
		file:           file,
		bufferedReader: bufio.NewReaderSize(file, 256*1024), // 256KB buffer
		path:           path,
		logger:         logger,
	}

	// Read and validate header
	if err := reader.readHeader(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Read footer
	if err := reader.readFooter(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}

	logger.WithFields(map[string]interface{}{
		"path":       path,
		"version":    reader.header.Version,
		"num_rows":   reader.header.NumRows,
		"row_groups": reader.header.NumRowGroups,
	}).Info("Columnar file reader opened")

	return reader, nil
}

// Schema returns the file schema
func (r *Reader) Schema() *Schema {
	return r.footer.Schema
}

// NumRows returns the total number of rows
func (r *Reader) NumRows() int64 {
	return r.header.NumRows
}

// NumRowGroups returns the number of row groups
func (r *Reader) NumRowGroups() int {
	return len(r.footer.RowGroups)
}

// Read reads the next row
func (r *Reader) Read() (map[string]interface{}, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	// Check if we need to load next row group
	if r.rowGroupData == nil || r.currentRow >= int64(len(r.rowGroupData)) {
		if r.currentRowGroup >= len(r.footer.RowGroups) {
			return nil, io.EOF
		}

		if err := r.loadRowGroup(r.currentRowGroup); err != nil {
			return nil, err
		}
		r.currentRowGroup++
		r.currentRow = 0
	}

	row := r.rowGroupData[r.currentRow]
	r.currentRow++
	return row, nil
}

// ReadRows reads multiple rows
func (r *Reader) ReadRows(count int) ([]map[string]interface{}, error) {
	rows := make([]map[string]interface{}, 0, count)

	for i := 0; i < count; i++ {
		row, err := r.Read()
		if err != nil {
			if err == io.EOF && len(rows) > 0 {
				return rows, nil
			}
			return rows, err
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// ReadAll reads all remaining rows
func (r *Reader) ReadAll() ([]map[string]interface{}, error) {
	var rows []map[string]interface{}

	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return rows, err
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// Seek seeks to a specific row. Implements io.Seeker interface.
// whence: 0 = offset from start, 1 = offset from current, 2 = offset from end
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	if r.closed {
		return 0, fmt.Errorf("reader is closed")
	}

	var rowNum int64
	switch whence {
	case 0:
		rowNum = offset
	case 1:
		rowNum = r.currentRow + offset
	case 2:
		rowNum = r.header.NumRows + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if rowNum < 0 || rowNum >= r.header.NumRows {
		return 0, fmt.Errorf("invalid row number: %d", rowNum)
	}

	var currentRow int64
	for i, rg := range r.footer.RowGroups {
		if rowNum < currentRow+rg.NumRows {
			r.currentRowGroup = i
			r.currentRow = rowNum - currentRow
			r.rowGroupData = nil
			return rowNum, nil
		}
		currentRow += rg.NumRows
	}

	return 0, fmt.Errorf("row %d not found", rowNum)
}

// GetRowGroupStatistics returns statistics for a row group
func (r *Reader) GetRowGroupStatistics(rowGroupIdx int) (*RowGroupStatistics, error) {
	if rowGroupIdx < 0 || rowGroupIdx >= len(r.footer.RowGroups) {
		return nil, fmt.Errorf("invalid row group index: %d", rowGroupIdx)
	}

	rg := r.footer.RowGroups[rowGroupIdx]
	stats := &RowGroupStatistics{
		NumRows:   rg.NumRows,
		TotalSize: rg.TotalSize,
		Columns:   make(map[string]ColumnStatistics),
	}

	for _, col := range rg.Columns {
		stats.Columns[col.ColumnName] = ColumnStatistics{
			NumValues: col.NumValues,
			NullCount: col.NullCount,
			MinValue:  bytesToValue(col.MinValue, Type(col.ColumnType)),
			MaxValue:  bytesToValue(col.MaxValue, Type(col.ColumnType)),
		}
	}

	return stats, nil
}

// GetColumnStatistics returns statistics for a column across all row groups
func (r *Reader) GetColumnStatistics(columnName string) (*ColumnStatistics, error) {
	var totalStats ColumnStatistics

	for i := range r.footer.RowGroups {
		rgStats, err := r.GetRowGroupStatistics(i)
		if err != nil {
			return nil, err
		}

		colStats, exists := rgStats.Columns[columnName]
		if !exists {
			continue
		}

		totalStats.NumValues += colStats.NumValues
		totalStats.NullCount += colStats.NullCount

		// Update min/max
		if totalStats.MinValue == nil || util.CompareValues(colStats.MinValue, totalStats.MinValue) < 0 {
			totalStats.MinValue = colStats.MinValue
		}
		if totalStats.MaxValue == nil || util.CompareValues(colStats.MaxValue, totalStats.MaxValue) > 0 {
			totalStats.MaxValue = colStats.MaxValue
		}
	}

	if totalStats.NumValues == 0 {
		return nil, fmt.Errorf("column not found: %s", columnName)
	}

	return &totalStats, nil
}

// Close closes the reader
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}

	if err := r.file.Close(); err != nil {
		return err
	}

	r.closed = true
	r.logger.WithField("path", r.path).Info("Columnar file reader closed")
	return nil
}

// readHeader reads and validates the file header
func (r *Reader) readHeader() error {
	if err := binary.Read(r.bufferedReader, binary.LittleEndian, &r.header); err != nil {
		return err
	}

	// Validate magic
	if string(r.header.Magic[:]) != FileMagic {
		return fmt.Errorf("invalid file magic: %s", string(r.header.Magic[:]))
	}

	// Validate version
	if r.header.Version != CurrentVersion {
		return fmt.Errorf("unsupported file version: %d", r.header.Version)
	}

	return nil
}

// readFooter reads the file footer
func (r *Reader) readFooter() error {
	// Seek to footer size (8 bytes before end)
	if _, err := r.file.Seek(-8, io.SeekEnd); err != nil {
		return err
	}

	// Read footer size
	var footerSize int64
	if err := binary.Read(r.file, binary.LittleEndian, &footerSize); err != nil {
		return err
	}

	// Seek to footer data
	r.footerOffset, _ = r.file.Seek(-8-footerSize, io.SeekEnd)

	// Read footer data
	footerData := make([]byte, footerSize)
	if _, err := r.file.Read(footerData); err != nil {
		return err
	}

	// Parse footer
	if err := json.Unmarshal(footerData, &r.footer); err != nil {
		return fmt.Errorf("failed to parse footer: %w", err)
	}

	// Reset to position after header
	if _, err := r.file.Seek(int64(binary.Size(FileHeader{})), io.SeekStart); err != nil {
		return err
	}

	// Reset buffered reader
	r.bufferedReader.Reset(r.file)

	return nil
}

// loadRowGroup loads a row group into memory
func (r *Reader) loadRowGroup(idx int) error {
	if idx < 0 || idx >= len(r.footer.RowGroups) {
		return fmt.Errorf("invalid row group index: %d", idx)
	}

	rg := r.footer.RowGroups[idx]
	r.rowGroupData = make([]map[string]interface{}, rg.NumRows)

	// Initialize rows
	for i := range r.rowGroupData {
		r.rowGroupData[i] = make(map[string]interface{})
	}

	// Read each column
	for _, col := range rg.Columns {
		if err := r.readColumn(col); err != nil {
			return fmt.Errorf("failed to read column %s: %w", col.ColumnName, err)
		}
	}

	r.logger.WithFields(map[string]interface{}{
		"row_group": idx,
		"rows":      rg.NumRows,
		"columns":   len(rg.Columns),
	}).Debug("Loaded row group")

	return nil
}

// readColumn reads a column's data
func (r *Reader) readColumn(chunk ColumnChunk) error {
	// Seek to column data
	if _, err := r.file.Seek(chunk.DataOffset, io.SeekStart); err != nil {
		return err
	}

	// Reset buffered reader
	r.bufferedReader.Reset(r.file)

	// Read null bitmap
	nullBitmapSize := (chunk.NumValues + 7) / 8
	nullBitmap := make([]byte, nullBitmapSize)
	if err := binary.Read(r.bufferedReader, binary.LittleEndian, nullBitmap); err != nil {
		return err
	}

	// Read values based on type
	dataType := Type(chunk.ColumnType)
	for i := int64(0); i < chunk.NumValues; i++ {
		// Check if null
		isNull := nullBitmap[i/8]&(1<<(i%8)) != 0
		if isNull {
			r.rowGroupData[i][chunk.ColumnName] = nil
			continue
		}

		// Read value
		value, err := r.readValue(dataType)
		if err != nil {
			return err
		}
		r.rowGroupData[i][chunk.ColumnName] = value
	}

	return nil
}

// readValue reads a single value based on type
func (r *Reader) readValue(dataType Type) (interface{}, error) {
	switch dataType {
	case TypeBoolean:
		var val bool
		if err := binary.Read(r.bufferedReader, binary.LittleEndian, &val); err != nil {
			return nil, err
		}
		return val, nil

	case TypeInt32:
		var val int32
		if err := binary.Read(r.bufferedReader, binary.LittleEndian, &val); err != nil {
			return nil, err
		}
		return val, nil

	case TypeInt64:
		var val int64
		if err := binary.Read(r.bufferedReader, binary.LittleEndian, &val); err != nil {
			return nil, err
		}
		return val, nil

	case TypeFloat:
		var val float32
		if err := binary.Read(r.bufferedReader, binary.LittleEndian, &val); err != nil {
			return nil, err
		}
		return val, nil

	case TypeDouble:
		var val float64
		if err := binary.Read(r.bufferedReader, binary.LittleEndian, &val); err != nil {
			return nil, err
		}
		return val, nil

	case TypeByteArray, TypeFixedLenByteArray:
		var length int32
		if err := binary.Read(r.bufferedReader, binary.LittleEndian, &length); err != nil {
			return nil, err
		}
		data := make([]byte, length)
		if _, err := r.bufferedReader.Read(data); err != nil {
			return nil, err
		}
		return string(data), nil

	default:
		return nil, fmt.Errorf("unsupported type: %v", dataType)
	}
}

// RowGroupStatistics holds statistics for a row group
type RowGroupStatistics struct {
	NumRows   int64
	TotalSize int64
	Columns   map[string]ColumnStatistics
}

// ColumnStatistics holds statistics for a column
type ColumnStatistics struct {
	NumValues int64
	NullCount int64
	MinValue  interface{}
	MaxValue  interface{}
}
