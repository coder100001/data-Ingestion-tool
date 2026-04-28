package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"
)

type MockStorage struct {
	mu          sync.Mutex
	writes      []*models.DataChange
	flushCount  int
	closeCount  int
	writeError  error
	flushError  error
	closeError  error
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		writes: make([]*models.DataChange, 0),
	}
}

func (m *MockStorage) Write(change *models.DataChange) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeError != nil {
		return m.writeError
	}
	m.writes = append(m.writes, change)
	return nil
}

func (m *MockStorage) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushCount++
	return m.flushError
}

func (m *MockStorage) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCount++
	return m.closeError
}

func (m *MockStorage) GetWrites() []*models.DataChange {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writes
}

func (m *MockStorage) GetWriteCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.writes)
}

type MockFilter struct {
	mu       sync.Mutex
	shouldPass bool
	callCount int
	applyFunc func(change *models.DataChange) bool
}

func NewMockFilter(shouldPass bool) *MockFilter {
	return &MockFilter{
		shouldPass: shouldPass,
	}
}

func (m *MockFilter) Apply(change *models.DataChange) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount++
	if m.applyFunc != nil {
		return m.applyFunc(change)
	}
	return m.shouldPass
}

func (m *MockFilter) GetCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

type MockTransformer struct {
	mu        sync.Mutex
	callCount int
	transformFunc func(change *models.DataChange) error
}

func NewMockTransformer() *MockTransformer {
	return &MockTransformer{}
}

func (m *MockTransformer) Transform(change *models.DataChange) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount++
	if m.transformFunc != nil {
		return m.transformFunc(change)
	}
	return nil
}

func (m *MockTransformer) GetCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

func newTestConfig() *config.Config {
	return &config.Config{
		App: config.AppConfig{
			Name:     "test-app",
			LogLevel: "debug",
		},
		Processing: config.ProcessingConfig{
			BatchSize:   10,
			WorkerCount: 2,
			Filters:     []string{},
			Transforms:  []string{},
		},
	}
}

func newTestLogger() *logger.Logger {
	log, _ := logger.New("debug", "")
	return log
}

func TestNewPipeline(t *testing.T) {
	cfg := newTestConfig()
	log := newTestLogger()
	storage := NewMockStorage()

	p := NewPipeline(cfg, log, storage)

	if p == nil {
		t.Fatal("NewPipeline returned nil")
	}

	if p.cfg != cfg {
		t.Error("Pipeline config not set correctly")
	}

	if p.logger != log {
		t.Error("Pipeline logger not set correctly")
	}

	if p.storage != storage {
		t.Error("Pipeline storage not set correctly")
	}

	if p.changeChan == nil {
		t.Error("Pipeline changeChan is nil")
	}

	if cap(p.changeChan) != cfg.Processing.BatchSize*2 {
		t.Errorf("Expected changeChan capacity %d, got %d", cfg.Processing.BatchSize*2, cap(p.changeChan))
	}

	if p.workers != cfg.Processing.WorkerCount {
		t.Errorf("Expected workers %d, got %d", cfg.Processing.WorkerCount, p.workers)
	}

	if p.ctx == nil {
		t.Error("Pipeline context is nil")
	}

	if p.cancel == nil {
		t.Error("Pipeline cancel function is nil")
	}

	if len(p.filters) != 0 {
		t.Errorf("Expected 0 filters, got %d", len(p.filters))
	}

	if len(p.transformers) != 0 {
		t.Errorf("Expected 0 transformers, got %d", len(p.transformers))
	}
}

func TestAddFilter(t *testing.T) {
	p := NewPipeline(newTestConfig(), newTestLogger(), NewMockStorage())

	filter1 := NewMockFilter(true)
	filter2 := NewMockFilter(false)

	p.AddFilter(filter1)
	if len(p.filters) != 1 {
		t.Errorf("Expected 1 filter, got %d", len(p.filters))
	}

	p.AddFilter(filter2)
	if len(p.filters) != 2 {
		t.Errorf("Expected 2 filters, got %d", len(p.filters))
	}
}

func TestAddTransformer(t *testing.T) {
	p := NewPipeline(newTestConfig(), newTestLogger(), NewMockStorage())

	transformer1 := NewMockTransformer()
	transformer2 := NewMockTransformer()

	p.AddTransformer(transformer1)
	if len(p.transformers) != 1 {
		t.Errorf("Expected 1 transformer, got %d", len(p.transformers))
	}

	p.AddTransformer(transformer2)
	if len(p.transformers) != 2 {
		t.Errorf("Expected 2 transformers, got %d", len(p.transformers))
	}
}

func TestGetChangeChannel(t *testing.T) {
	p := NewPipeline(newTestConfig(), newTestLogger(), NewMockStorage())

	ch := p.GetChangeChannel()
	if ch == nil {
		t.Error("GetChangeChannel returned nil")
	}

	change := models.NewDataChange(models.Insert, "test_db", "test_table")
	select {
	case ch <- change:
	default:
		t.Error("Failed to send to change channel")
	}
}

func TestStartStop(t *testing.T) {
	cfg := newTestConfig()
	cfg.Processing.WorkerCount = 2

	p := NewPipeline(cfg, newTestLogger(), NewMockStorage())

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := p.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	select {
	case <-p.ctx.Done():
	default:
		t.Error("Context should be cancelled after Stop")
	}
}

func TestStartWithFiltersAndTransforms(t *testing.T) {
	cfg := newTestConfig()
	cfg.Processing.Filters = []string{"age > 18"}
	cfg.Processing.Transforms = []string{"add_timestamp"}

	p := NewPipeline(cfg, newTestLogger(), NewMockStorage())

	// Backward compatibility: unsupported filter/transform rules should not fail startup
	// They fall back to NoOp with a warning log
	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if len(p.filters) != 1 {
		t.Errorf("Expected 1 filter, got %d", len(p.filters))
	}

	if len(p.transformers) != 1 {
		t.Errorf("Expected 1 transformer, got %d", len(p.transformers))
	}

	_ = p.Stop()
}

func TestProcessChange(t *testing.T) {
	tests := []struct {
		name          string
		filters       []Filter
		transformers  []Transformer
		change        *models.DataChange
		expectWrite   bool
		expectError   bool
	}{
		{
			name:        "no filters or transformers",
			filters:     nil,
			transformers: nil,
			change: &models.DataChange{
				Type:     models.Insert,
				Database: "test_db",
				Table:    "test_table",
				After:    map[string]interface{}{"id": 1, "name": "test"},
			},
			expectWrite: true,
			expectError: false,
		},
		{
			name: "filter passes",
			filters: []Filter{NewMockFilter(true)},
			transformers: nil,
			change: &models.DataChange{
				Type:     models.Insert,
				Database: "test_db",
				Table:    "test_table",
				After:    map[string]interface{}{"id": 1},
			},
			expectWrite: true,
			expectError: false,
		},
		{
			name: "filter blocks",
			filters: []Filter{NewMockFilter(false)},
			transformers: nil,
			change: &models.DataChange{
				Type:     models.Insert,
				Database: "test_db",
				Table:    "test_table",
				After:    map[string]interface{}{"id": 1},
			},
			expectWrite: false,
			expectError: false,
		},
		{
			name:    "multiple filters all pass",
			filters: []Filter{NewMockFilter(true), NewMockFilter(true)},
			transformers: nil,
			change: &models.DataChange{
				Type:     models.Insert,
				Database: "test_db",
				Table:    "test_table",
				After:    map[string]interface{}{"id": 1},
			},
			expectWrite: true,
			expectError: false,
		},
		{
			name:    "multiple filters one blocks",
			filters: []Filter{NewMockFilter(true), NewMockFilter(false)},
			transformers: nil,
			change: &models.DataChange{
				Type:     models.Insert,
				Database: "test_db",
				Table:    "test_table",
				After:    map[string]interface{}{"id": 1},
			},
			expectWrite: false,
			expectError: false,
		},
		{
			name:        "transformer modifies data",
			filters:     nil,
			transformers: []Transformer{&AddTimestampTransformer{FieldName: "processed_at"}},
			change: &models.DataChange{
				Type:     models.Insert,
				Database: "test_db",
				Table:    "test_table",
				After:    map[string]interface{}{"id": 1},
			},
			expectWrite: true,
			expectError: false,
		},
		{
			name:        "filter and transformer",
			filters:     []Filter{NewMockFilter(true)},
			transformers: []Transformer{NewMockTransformer()},
			change: &models.DataChange{
				Type:     models.Insert,
				Database: "test_db",
				Table:    "test_table",
				After:    map[string]interface{}{"id": 1},
			},
			expectWrite: true,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := NewMockStorage()
			p := NewPipeline(newTestConfig(), newTestLogger(), storage)

			for _, f := range tt.filters {
				p.AddFilter(f)
			}
			for _, tr := range tt.transformers {
				p.AddTransformer(tr)
			}

			err := p.processChange(tt.change)

			if tt.expectError && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			writeCount := storage.GetWriteCount()
			if tt.expectWrite && writeCount != 1 {
				t.Errorf("Expected 1 write, got %d", writeCount)
			}
			if !tt.expectWrite && writeCount != 0 {
				t.Errorf("Expected 0 writes, got %d", writeCount)
			}
		})
	}
}

func TestColumnFilter(t *testing.T) {
	tests := []struct {
		name       string
		filter     *ColumnFilter
		change     *models.DataChange
		shouldPass bool
	}{
		{
			name: "equals operator - pass",
			filter: &ColumnFilter{Column: "status", Operator: "=", Value: "active"},
			change: &models.DataChange{
				Type:  models.Insert,
				After: map[string]interface{}{"status": "active"},
			},
			shouldPass: true,
		},
		{
			name: "equals operator - fail",
			filter: &ColumnFilter{Column: "status", Operator: "=", Value: "active"},
			change: &models.DataChange{
				Type:  models.Insert,
				After: map[string]interface{}{"status": "inactive"},
			},
			shouldPass: false,
		},
		{
			name: "not equals operator - pass",
			filter: &ColumnFilter{Column: "status", Operator: "!=", Value: "active"},
			change: &models.DataChange{
				Type:  models.Insert,
				After: map[string]interface{}{"status": "inactive"},
			},
			shouldPass: true,
		},
		{
			name: "greater than operator - pass",
			filter: &ColumnFilter{Column: "age", Operator: ">", Value: 18},
			change: &models.DataChange{
				Type:  models.Insert,
				After: map[string]interface{}{"age": 25},
			},
			shouldPass: true,
		},
		{
			name: "greater than operator - fail",
			filter: &ColumnFilter{Column: "age", Operator: ">", Value: 18},
			change: &models.DataChange{
				Type:  models.Insert,
				After: map[string]interface{}{"age": 15},
			},
			shouldPass: false,
		},
		{
			name: "less than operator - pass",
			filter: &ColumnFilter{Column: "age", Operator: "<", Value: 100},
			change: &models.DataChange{
				Type:  models.Insert,
				After: map[string]interface{}{"age": 50},
			},
			shouldPass: true,
		},
		{
			name: "greater than or equal - pass",
			filter: &ColumnFilter{Column: "age", Operator: ">=", Value: 18},
			change: &models.DataChange{
				Type:  models.Insert,
				After: map[string]interface{}{"age": 18},
			},
			shouldPass: true,
		},
		{
			name: "less than or equal - pass",
			filter: &ColumnFilter{Column: "age", Operator: "<=", Value: 18},
			change: &models.DataChange{
				Type:  models.Insert,
				After: map[string]interface{}{"age": 18},
			},
			shouldPass: true,
		},
		{
			name: "column not exists",
			filter: &ColumnFilter{Column: "nonexistent", Operator: "=", Value: "value"},
			change: &models.DataChange{
				Type:  models.Insert,
				After: map[string]interface{}{"other": "value"},
			},
			shouldPass: false,
		},
		{
			name: "delete operation uses before",
			filter: &ColumnFilter{Column: "status", Operator: "=", Value: "deleted"},
			change: &models.DataChange{
				Type:   models.Delete,
				Before: map[string]interface{}{"status": "deleted"},
			},
			shouldPass: true,
		},
		{
			name: "unknown operator defaults to pass",
			filter: &ColumnFilter{Column: "status", Operator: "LIKE", Value: "%active%"},
			change: &models.DataChange{
				Type:  models.Insert,
				After: map[string]interface{}{"status": "active"},
			},
			shouldPass: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.filter.Apply(tt.change)
			if result != tt.shouldPass {
				t.Errorf("Expected %v, got %v", tt.shouldPass, result)
			}
		})
	}
}

func TestAddTimestampTransformer(t *testing.T) {
	transformer := &AddTimestampTransformer{FieldName: "created_at"}

	change := &models.DataChange{
		Type:  models.Insert,
		After: map[string]interface{}{"id": 1},
	}

	err := transformer.Transform(change)
	if err != nil {
		t.Errorf("Transform failed: %v", err)
	}

	if _, exists := change.After["created_at"]; !exists {
		t.Error("Timestamp field not added to After")
	}
}

func TestAddTimestampTransformerWithBefore(t *testing.T) {
	transformer := &AddTimestampTransformer{FieldName: "updated_at"}

	change := &models.DataChange{
		Type:   models.Update,
		Before: map[string]interface{}{"id": 1},
		After:  map[string]interface{}{"id": 1, "name": "updated"},
	}

	err := transformer.Transform(change)
	if err != nil {
		t.Errorf("Transform failed: %v", err)
	}

	if _, exists := change.Before["updated_at"]; !exists {
		t.Error("Timestamp field not added to Before")
	}
	if _, exists := change.After["updated_at"]; !exists {
		t.Error("Timestamp field not added to After")
	}
}

func TestMaskFieldTransformer(t *testing.T) {
	tests := []struct {
		name      string
		transformer *MaskFieldTransformer
		change    *models.DataChange
		checkFunc func(t *testing.T, change *models.DataChange)
	}{
		{
			name: "mask email field",
			transformer: &MaskFieldTransformer{Field: "email", Mask: "***", Length: 3},
			change: &models.DataChange{
				Type:  models.Insert,
				After: map[string]interface{}{"email": "test@example.com"},
			},
			checkFunc: func(t *testing.T, change *models.DataChange) {
				if change.After["email"] != "tes***" {
					t.Errorf("Expected 'tes***', got '%v'", change.After["email"])
				}
			},
		},
		{
			name: "mask short value",
			transformer: &MaskFieldTransformer{Field: "code", Mask: "***", Length: 3},
			change: &models.DataChange{
				Type:  models.Insert,
				After: map[string]interface{}{"code": "ab"},
			},
			checkFunc: func(t *testing.T, change *models.DataChange) {
				if change.After["code"] != "***" {
					t.Errorf("Expected '***', got '%v'", change.After["code"])
				}
			},
		},
		{
			name: "mask non-existent field",
			transformer: &MaskFieldTransformer{Field: "nonexistent", Mask: "***", Length: 3},
			change: &models.DataChange{
				Type:  models.Insert,
				After: map[string]interface{}{"email": "test@example.com"},
			},
			checkFunc: func(t *testing.T, change *models.DataChange) {
				if change.After["email"] != "test@example.com" {
					t.Error("Email should not be modified")
				}
			},
		},
		{
			name: "mask nil value",
			transformer: &MaskFieldTransformer{Field: "email", Mask: "***", Length: 3},
			change: &models.DataChange{
				Type:  models.Insert,
				After: map[string]interface{}{"email": nil},
			},
			checkFunc: func(t *testing.T, change *models.DataChange) {
				if change.After["email"] != nil {
					t.Error("Nil value should remain nil")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.transformer.Transform(tt.change)
			if err != nil {
				t.Errorf("Transform failed: %v", err)
			}
			tt.checkFunc(t, tt.change)
		})
	}
}

func TestNoOpFilter(t *testing.T) {
	filter := &NoOpFilter{}

	change := &models.DataChange{
		Type:  models.Insert,
		After: map[string]interface{}{"id": 1},
	}

	if !filter.Apply(change) {
		t.Error("NoOpFilter should always return true")
	}
}

func TestNoOpTransformer(t *testing.T) {
	transformer := &NoOpTransformer{}

	change := &models.DataChange{
		Type:  models.Insert,
		After: map[string]interface{}{"id": 1},
	}

	err := transformer.Transform(change)
	if err != nil {
		t.Errorf("NoOpTransformer should not return error: %v", err)
	}
}

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
	}{
		{"int less", 5, 10, -1},
		{"int equal", 10, 10, 0},
		{"int greater", 15, 10, 1},
		{"float less", 5.5, 10.5, -1},
		{"float equal", 10.5, 10.5, 0},
		{"string less", "a", "b", -1},
		{"string equal", "test", "test", 0},
		{"string greater", "z", "a", 1},
		{"mixed numeric", 10, 10.0, 0},
		{"string number", "10", 10, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareValues(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("compareValues(%v, %v) = %d, expected %d", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected float64
		ok       bool
	}{
		{"float64", float64(10.5), 10.5, true},
		{"float32", float32(10.5), 10.5, true},
		{"int", int(10), 10.0, true},
		{"int32", int32(10), 10.0, true},
		{"int64", int64(10), 10.0, true},
		{"string number", "10.5", 10.5, true},
		{"string invalid", "not a number", 0, false},
		{"bool", true, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := toFloat64(tt.input)
			if ok != tt.ok {
				t.Errorf("toFloat64(%v) ok = %v, expected %v", tt.input, ok, tt.ok)
			}
			if ok && result != tt.expected {
				t.Errorf("toFloat64(%v) = %f, expected %f", tt.input, result, tt.expected)
			}
		})
	}
}

func TestConcurrentProcessing(t *testing.T) {
	cfg := newTestConfig()
	cfg.Processing.WorkerCount = 4
	cfg.Processing.BatchSize = 100

	storage := NewMockStorage()
	p := NewPipeline(cfg, newTestLogger(), storage)

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	numChanges := 100
	var wg sync.WaitGroup
	wg.Add(numChanges)

	for i := 0; i < numChanges; i++ {
		go func(id int) {
			defer wg.Done()
			change := &models.DataChange{
				Type:     models.Insert,
				Database: "test_db",
				Table:    "test_table",
				After:    map[string]interface{}{"id": id},
			}
			p.GetChangeChannel() <- change
		}(i)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	_ = p.Stop()

	writeCount := storage.GetWriteCount()
	if writeCount != numChanges {
		t.Errorf("Expected %d writes, got %d", numChanges, writeCount)
	}
}

func TestConcurrentFilterAccess(t *testing.T) {
	p := NewPipeline(newTestConfig(), newTestLogger(), NewMockStorage())

	var wg sync.WaitGroup
	var mu sync.Mutex
	numGoroutines := 100
	filtersAdded := 0
	transformersAdded := 0

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()
			p.AddFilter(NewMockFilter(true))
			filtersAdded++
			mu.Unlock()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			mu.Lock()
			p.AddTransformer(NewMockTransformer())
			transformersAdded++
			mu.Unlock()
		}()
	}

	wg.Wait()

	if filtersAdded != numGoroutines {
		t.Errorf("Expected %d filters added, got %d", numGoroutines, filtersAdded)
	}
	if transformersAdded != numGoroutines {
		t.Errorf("Expected %d transformers added, got %d", numGoroutines, transformersAdded)
	}
}

func TestStorageError(t *testing.T) {
	storage := NewMockStorage()
	storage.writeError = context.DeadlineExceeded

	p := NewPipeline(newTestConfig(), newTestLogger(), storage)

	change := &models.DataChange{
		Type:     models.Insert,
		Database: "test_db",
		Table:    "test_table",
		After:    map[string]interface{}{"id": 1},
	}

	err := p.processChange(change)
	if err == nil {
		t.Error("Expected error from storage write failure")
	}
}

func TestTransformerError(t *testing.T) {
	transformer := NewMockTransformer()
	transformer.transformFunc = func(change *models.DataChange) error {
		return context.Canceled
	}

	p := NewPipeline(newTestConfig(), newTestLogger(), NewMockStorage())
	p.AddTransformer(transformer)

	change := &models.DataChange{
		Type:     models.Insert,
		Database: "test_db",
		Table:    "test_table",
		After:    map[string]interface{}{"id": 1},
	}

	err := p.processChange(change)
	if err == nil {
		t.Error("Expected error from transformer failure")
	}
}

func TestStopFlushesStorage(t *testing.T) {
	storage := NewMockStorage()
	p := NewPipeline(newTestConfig(), newTestLogger(), storage)

	_ = p.Start()
	time.Sleep(10 * time.Millisecond)
	_ = p.Stop()

	if storage.flushCount != 1 {
		t.Errorf("Expected 1 flush, got %d", storage.flushCount)
	}
	if storage.closeCount != 1 {
		t.Errorf("Expected 1 close, got %d", storage.closeCount)
	}
}

func TestStopClosesChannel(t *testing.T) {
	p := NewPipeline(newTestConfig(), newTestLogger(), NewMockStorage())
	_ = p.Start()
	_ = p.Stop()

	select {
	case _, ok := <-p.changeChan:
		if ok {
			t.Error("Channel should be closed")
		}
	default:
		t.Error("Channel should be closed and readable")
	}
}

func TestProcessChangeWithDelete(t *testing.T) {
	storage := NewMockStorage()
	p := NewPipeline(newTestConfig(), newTestLogger(), storage)

	filter := &ColumnFilter{Column: "status", Operator: "=", Value: "deleted"}
	p.AddFilter(filter)

	change := &models.DataChange{
		Type:   models.Delete,
		Before: map[string]interface{}{"id": 1, "status": "deleted"},
	}

	err := p.processChange(change)
	if err != nil {
		t.Errorf("processChange failed: %v", err)
	}

	if storage.GetWriteCount() != 1 {
		t.Errorf("Expected 1 write, got %d", storage.GetWriteCount())
	}
}

func TestMultipleWorkersProcessChanges(t *testing.T) {
	cfg := newTestConfig()
	cfg.Processing.WorkerCount = 3

	storage := NewMockStorage()
	p := NewPipeline(cfg, newTestLogger(), storage)

	if err := p.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		change := &models.DataChange{
			Type:     models.Insert,
			Database: "test_db",
			Table:    "test_table",
			After:    map[string]interface{}{"id": i},
		}
		p.GetChangeChannel() <- change
	}

	time.Sleep(100 * time.Millisecond)
	_ = p.Stop()

	if storage.GetWriteCount() != 10 {
		t.Errorf("Expected 10 writes, got %d", storage.GetWriteCount())
	}
}

func TestParseFilterRule(t *testing.T) {
	tests := []struct {
		name    string
		rule    string
		wantErr bool
	}{
		{"simple rule", "age > 18", true},
		{"empty rule", "", true},
		{"complex rule", "status = 'active'", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := parseFilterRule(tt.rule)
			if tt.wantErr && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !tt.wantErr && filter == nil {
				t.Error("Expected filter, got nil")
			}
		})
	}
}

func TestParseTransformRule(t *testing.T) {
	tests := []struct {
		name    string
		rule    string
		wantErr bool
	}{
		{"simple rule", "add_timestamp", true},
		{"empty rule", "", true},
		{"complex rule", "mask_field(email)", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transformer, err := parseTransformRule(tt.rule)
			if tt.wantErr && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !tt.wantErr && transformer == nil {
				t.Error("Expected transformer, got nil")
			}
		})
	}
}
