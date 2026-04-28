package pipeline

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"
	"data-ingestion-tool/pkg/storage"
)

// Pipeline handles data processing from source to storage
type Pipeline struct {
	cfg          *config.Config
	logger       *logger.Logger
	changeChan   chan *models.DataChange
	storage      Storage
	filters      []Filter
	transformers []Transformer
	workers      int
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

// Filter defines the interface for data filtering
type Filter interface {
	Apply(change *models.DataChange) bool
}

// Transformer defines the interface for data transformation
type Transformer interface {
	Transform(change *models.DataChange) error
}

// Storage defines the interface for data storage
type Storage interface {
	Write(change *models.DataChange) error
	Flush() error
	Close() error
}

// NewPipeline creates a new data processing pipeline
func NewPipeline(cfg *config.Config, logger *logger.Logger, storage Storage) *Pipeline {
	ctx, cancel := context.WithCancel(context.Background())

	return &Pipeline{
		cfg:          cfg,
		logger:       logger,
		changeChan:   make(chan *models.DataChange, cfg.Processing.BatchSize*2),
		storage:      storage,
		filters:      make([]Filter, 0),
		transformers: make([]Transformer, 0),
		workers:      cfg.Processing.WorkerCount,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// AddFilter adds a filter to the pipeline
func (p *Pipeline) AddFilter(filter Filter) {
	p.filters = append(p.filters, filter)
}

// AddTransformer adds a transformer to the pipeline
func (p *Pipeline) AddTransformer(transformer Transformer) {
	p.transformers = append(p.transformers, transformer)
}

// GetChangeChannel returns the channel for receiving data changes
func (p *Pipeline) GetChangeChannel() chan<- *models.DataChange {
	return p.changeChan
}

// Start starts the pipeline processing
func (p *Pipeline) Start() error {
	p.logger.Info("Starting data processing pipeline...")

	// Initialize filters from config
	if err := p.initFilters(); err != nil {
		return fmt.Errorf("failed to initialize filters: %w", err)
	}

	// Initialize transformers from config
	if err := p.initTransformers(); err != nil {
		return fmt.Errorf("failed to initialize transformers: %w", err)
	}

	// Start worker pool
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	p.logger.WithField("workers", p.workers).Info("Pipeline started")
	return nil
}

// Stop stops the pipeline processing
func (p *Pipeline) Stop() error {
	p.logger.Info("Stopping data processing pipeline...")

	p.cancel()
	close(p.changeChan)

	// Wait for workers to finish
	p.wg.Wait()

	// Flush and close storage
	if err := p.storage.Flush(); err != nil {
		p.logger.WithError(err).Error("Failed to flush storage")
	}

	if err := p.storage.Close(); err != nil {
		p.logger.WithError(err).Error("Failed to close storage")
	}

	p.logger.Info("Pipeline stopped")
	return nil
}

// worker processes data changes
func (p *Pipeline) worker(id int) {
	defer p.wg.Done()

	logger := p.logger.WithField("worker", id)
	logger.Debug("Worker started")

	for {
		select {
		case <-p.ctx.Done():
			logger.Debug("Worker stopping due to context cancellation")
			return

		case change, ok := <-p.changeChan:
			if !ok {
				logger.Debug("Worker stopping due to channel close")
				return
			}

			if err := p.processChange(change); err != nil {
				logger.WithError(err).Error("Failed to process change")
			}
		}
	}
}

// processChange processes a single data change
func (p *Pipeline) processChange(change *models.DataChange) error {
	// Apply filters
	for _, filter := range p.filters {
		if !filter.Apply(change) {
			p.logger.WithFields(map[string]interface{}{
				"database": change.Database,
				"table":    change.Table,
				"type":     change.Type,
			}).Debug("Change filtered out")
			return nil
		}
	}

	// Apply transformations
	for _, transformer := range p.transformers {
		if err := transformer.Transform(change); err != nil {
			return fmt.Errorf("transformation failed: %w", err)
		}
	}

	// Write to storage
	if err := p.storage.Write(change); err != nil {
		return fmt.Errorf("storage write failed: %w", err)
	}

	return nil
}

// initFilters initializes filters from configuration
func (p *Pipeline) initFilters() error {
	for _, rule := range p.cfg.Processing.Filters {
		filter, err := parseFilterRule(rule)
		if err != nil {
			return fmt.Errorf("failed to parse filter rule '%s': %w", rule, err)
		}
		p.AddFilter(filter)
	}
	return nil
}

// initTransformers initializes transformers from configuration
func (p *Pipeline) initTransformers() error {
	for _, rule := range p.cfg.Processing.Transforms {
		transformer, err := parseTransformRule(rule)
		if err != nil {
			return fmt.Errorf("failed to parse transform rule '%s': %w", rule, err)
		}
		p.AddTransformer(transformer)
	}
	return nil
}

// parseFilterRule parses a filter rule string
func parseFilterRule(rule string) (Filter, error) {
	// Simple filter parsing: column operator value
	// Example: "age > 18", "status = 'active'"
	// For now, return a no-op filter
	return &NoOpFilter{}, nil
}

// parseTransformRule parses a transform rule string
func parseTransformRule(rule string) (Transformer, error) {
	// Simple transform parsing
	// For now, return a no-op transformer
	return &NoOpTransformer{}, nil
}

// NoOpFilter is a filter that always returns true
type NoOpFilter struct{}

// Apply implements the Filter interface
func (f *NoOpFilter) Apply(change *models.DataChange) bool {
	return true
}

// NoOpTransformer is a transformer that does nothing
type NoOpTransformer struct{}

// Transform implements the Transformer interface
func (t *NoOpTransformer) Transform(change *models.DataChange) error {
	return nil
}

// ColumnFilter filters based on column values
type ColumnFilter struct {
	Column   string
	Operator string
	Value    interface{}
}

// Apply implements the Filter interface
func (f *ColumnFilter) Apply(change *models.DataChange) bool {
	// Check in After for inserts and updates, Before for deletes
	var data map[string]interface{}
	if change.Type == models.Delete {
		data = change.Before
	} else {
		data = change.After
	}

	value, exists := data[f.Column]
	if !exists {
		return false
	}

	switch f.Operator {
	case "=", "==":
		return fmt.Sprintf("%v", value) == fmt.Sprintf("%v", f.Value)
	case "!=":
		return fmt.Sprintf("%v", value) != fmt.Sprintf("%v", f.Value)
	case ">":
		return compareValues(value, f.Value) > 0
	case ">=":
		return compareValues(value, f.Value) >= 0
	case "<":
		return compareValues(value, f.Value) < 0
	case "<=":
		return compareValues(value, f.Value) <= 0
	default:
		return true
	}
}

// compareValues compares two values
func compareValues(a, b interface{}) int {
	// Try numeric comparison
	aFloat, aOk := toFloat64(a)
	bFloat, bOk := toFloat64(b)

	if aOk && bOk {
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}

	// String comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return strings.Compare(aStr, bStr)
}

// toFloat64 converts a value to float64
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// AddTimestampTransformer adds a timestamp to the change
type AddTimestampTransformer struct {
	FieldName string
}

// Transform implements the Transformer interface
func (t *AddTimestampTransformer) Transform(change *models.DataChange) error {
	if change.After != nil {
		change.After[t.FieldName] = time.Now().UTC().Format(time.RFC3339)
	}
	if change.Before != nil {
		change.Before[t.FieldName] = time.Now().UTC().Format(time.RFC3339)
	}
	return nil
}

// MaskFieldTransformer masks sensitive fields
type MaskFieldTransformer struct {
	Field  string
	Mask   string
	Length int
}

// Transform implements the Transformer interface
func (t *MaskFieldTransformer) Transform(change *models.DataChange) error {
	maskValue := func(data map[string]interface{}) {
		if val, exists := data[t.Field]; exists && val != nil {
			strVal := fmt.Sprintf("%v", val)
			if len(strVal) > t.Length {
				data[t.Field] = strVal[:t.Length] + t.Mask
			} else {
				data[t.Field] = t.Mask
			}
		}
	}

	if change.After != nil {
		maskValue(change.After)
	}
	if change.Before != nil {
		maskValue(change.Before)
	}
	return nil
}

// NewLocalStorage creates a new local storage instance (delegates to storage package)
func NewLocalStorage(cfg *config.LocalConfig, logger *logger.Logger) (*storage.LocalStorage, error) {
	return storage.NewLocalStorage(cfg, logger)
}
