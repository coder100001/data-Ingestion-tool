package connector

import (
	"context"
	"fmt"

	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"
)

// Connector defines the interface for data source connectors
type Connector interface {
	// Connect establishes connection to the data source
	Connect(ctx context.Context) error
	
	// Disconnect closes the connection
	Disconnect() error
	
	// Start begins capturing data changes
	Start(ctx context.Context, changeChan chan<- *models.DataChange) error
	
	// Stop stops capturing data changes
	Stop() error
	
	// GetPosition returns the current position in the data source
	GetPosition() models.Position
	
	// SetPosition sets the starting position for data capture
	SetPosition(position models.Position) error
	
	// IsConnected returns true if the connector is connected
	IsConnected() bool
	
	// GetTableInfo returns metadata about tables
	GetTableInfo(database, table string) (*models.TableInfo, error)
}

// Factory creates connectors based on configuration
type Factory struct {
	cfg    *config.Config
	logger *logger.Logger
}

// NewFactory creates a new connector factory
func NewFactory(cfg *config.Config, log *logger.Logger) *Factory {
	return &Factory{
		cfg:    cfg,
		logger: log,
	}
}

// CreateConnector creates a connector based on the configuration
func (f *Factory) CreateConnector() (Connector, error) {
	switch f.cfg.Source.Type {
	case "mysql":
		return NewMySQLConnector(f.cfg, f.logger), nil
	case "kafka":
		return NewKafkaConnector(f.cfg, f.logger), nil
	case "postgresql":
		return NewPostgresConnector(f.cfg, f.logger), nil
	case "rest":
		return NewRESTConnector(f.cfg, f.logger), nil
	default:
		return nil, fmt.Errorf("unsupported source type: %s", f.cfg.Source.Type)
	}
}

// BaseConnector provides common functionality for all connectors
type BaseConnector struct {
	cfg       *config.Config
	logger    *logger.Logger
	connected bool
	position  models.Position
	stopChan  chan struct{}
}

// NewBaseConnector creates a new base connector
func NewBaseConnector(cfg *config.Config, logger *logger.Logger) BaseConnector {
	return BaseConnector{
		cfg:      cfg,
		logger:   logger,
		stopChan: make(chan struct{}),
	}
}

// IsConnected returns true if the connector is connected
func (bc *BaseConnector) IsConnected() bool {
	return bc.connected
}

// GetPosition returns the current position
func (bc *BaseConnector) GetPosition() models.Position {
	return bc.position
}

// SetPosition sets the starting position
func (bc *BaseConnector) SetPosition(position models.Position) error {
	bc.position = position
	return nil
}
