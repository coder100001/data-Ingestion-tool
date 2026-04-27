package connector

import (
	"context"
	"fmt"

	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"
)

// RESTConnector implements the Connector interface for REST APIs (placeholder for future implementation)
type RESTConnector struct {
	BaseConnector
}

// NewRESTConnector creates a new REST connector
func NewRESTConnector(cfg *config.Config, logger *logger.Logger) *RESTConnector {
	return &RESTConnector{
		BaseConnector: NewBaseConnector(cfg, logger),
	}
}

// Connect establishes connection to REST API
func (r *RESTConnector) Connect(ctx context.Context) error {
	r.logger.Info("Connecting to REST API...")
	// TODO: Implement REST API connection
	return fmt.Errorf("REST connector not yet implemented")
}

// Disconnect closes the REST API connection
func (r *RESTConnector) Disconnect() error {
	r.logger.Info("Disconnecting from REST API...")
	// TODO: Implement REST API disconnection
	return nil
}

// Start begins polling REST API
func (r *RESTConnector) Start(ctx context.Context, changeChan chan<- *models.DataChange) error {
	r.logger.Info("Starting REST API polling...")
	// TODO: Implement REST API polling
	return fmt.Errorf("REST connector not yet implemented")
}

// Stop stops polling REST API
func (r *RESTConnector) Stop() error {
	r.logger.Info("Stopping REST API polling...")
	// TODO: Implement REST API stop
	return nil
}

// GetTableInfo returns metadata (not applicable for REST)
func (r *RESTConnector) GetTableInfo(database, table string) (*models.TableInfo, error) {
	return nil, fmt.Errorf("GetTableInfo not applicable for REST source")
}
