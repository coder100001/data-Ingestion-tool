package connector

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// MySQLConnector implements the Connector interface for MySQL binlog
type MySQLConnector struct {
	BaseConnector
	canal      *canal.Canal
	handler    *binlogHandler
	config     *canal.Config
	syncer     *replication.BinlogSyncer
	streamer   *replication.BinlogStreamer
	mu         sync.RWMutex
	tableCache map[string]*models.TableInfo
}

// binlogHandler handles binlog events
type binlogHandler struct {
	connector *MySQLConnector
	changeChan chan<- *models.DataChange
}

// NewMySQLConnector creates a new MySQL connector
func NewMySQLConnector(cfg *config.Config, logger *logger.Logger) *MySQLConnector {
	return &MySQLConnector{
		BaseConnector: NewBaseConnector(cfg, logger),
		tableCache:    make(map[string]*models.TableInfo),
	}
}

// Connect establishes connection to MySQL
func (m *MySQLConnector) Connect(ctx context.Context) error {
	m.logger.Info("Connecting to MySQL...")

	mysqlCfg := m.cfg.Source.MySQL

	// Create canal config
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", mysqlCfg.Host, mysqlCfg.Port)
	cfg.User = mysqlCfg.User
	cfg.Password = mysqlCfg.Password
	cfg.ServerID = mysqlCfg.ServerID
	cfg.Flavor = "mysql"
	cfg.Dump.ExecutionPath = "" // Disable mysqldump

	// Validate required fields
	if mysqlCfg.Host == "" {
		return fmt.Errorf("MySQL host is required")
	}
	if mysqlCfg.Port == 0 {
		mysqlCfg.Port = 3306
	}
	if mysqlCfg.User == "" {
		return fmt.Errorf("MySQL user is required")
	}
	if mysqlCfg.ServerID == 0 {
		return fmt.Errorf("MySQL server_id is required (must be unique for replication)")
	}

	// Create canal instance
	c, err := canal.NewCanal(cfg)
	if err != nil {
		return fmt.Errorf("failed to create canal: %w", err)
	}

	m.canal = c
	m.config = cfg

	// Set up binlog syncer for direct binlog reading
	binlogCfg := replication.BinlogSyncerConfig{
		ServerID: mysqlCfg.ServerID,
		Flavor:   "mysql",
		Host:     mysqlCfg.Host,
		Port:     uint16(mysqlCfg.Port),
		User:     mysqlCfg.User,
		Password: mysqlCfg.Password,
	}

	m.syncer = replication.NewBinlogSyncer(binlogCfg)

	m.connected = true
	m.logger.Info("Successfully connected to MySQL")
	return nil
}

// Disconnect closes the MySQL connection
func (m *MySQLConnector) Disconnect() error {
	m.logger.Info("Disconnecting from MySQL...")

	if m.syncer != nil {
		m.syncer.Close()
	}

	if m.canal != nil {
		m.canal.Close()
	}

	m.connected = false
	m.logger.Info("Successfully disconnected from MySQL")
	return nil
}

// Start begins capturing binlog events
func (m *MySQLConnector) Start(ctx context.Context, changeChan chan<- *models.DataChange) error {
	m.logger.Info("Starting MySQL binlog capture...")

	if !m.connected {
		return fmt.Errorf("not connected to MySQL")
	}

	// Create handler
	m.handler = &binlogHandler{
		connector:  m,
		changeChan: changeChan,
	}

	// Start binlog streaming
	go m.streamBinlog(ctx)

	m.logger.Info("MySQL binlog capture started")
	return nil
}

// streamBinlog streams binlog events
func (m *MySQLConnector) streamBinlog(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.WithField("panic", r).Error("Panic in binlog streaming")
		}
	}()

	var pos mysql.Position
	
	// Use checkpoint position if available
	if m.position.BinlogFile != "" {
		pos = mysql.Position{
			Name: m.position.BinlogFile,
			Pos:  m.position.BinlogPos,
		}
		m.logger.WithFields(map[string]interface{}{
			"file": pos.Name,
			"pos":  pos.Pos,
		}).Info("Starting from checkpoint position")
	} else {
		// Get current master position
		pos = m.syncer.GetNextPosition()
		m.logger.WithFields(map[string]interface{}{
			"file": pos.Name,
			"pos":  pos.Pos,
		}).Info("Starting from current master position")
	}

	// Start syncing
	streamer, err := m.syncer.StartSync(pos)
	if err != nil {
		m.logger.WithError(err).Error("Failed to start binlog sync")
		return
	}
	m.streamer = streamer

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("Binlog streaming stopped due to context cancellation")
			return
		case <-m.stopChan:
			m.logger.Info("Binlog streaming stopped")
			return
		default:
			event, err := streamer.GetEvent(ctx)
			if err != nil {
				m.logger.WithError(err).Error("Failed to get binlog event")
				time.Sleep(time.Second)
				continue
			}

			// Update position
			m.mu.Lock()
			m.position.BinlogFile = pos.Name
			m.position.BinlogPos = uint32(event.Header.LogPos)
			m.mu.Unlock()

			// Process event
			if err := m.processEvent(event); err != nil {
				m.logger.WithError(err).Error("Failed to process binlog event")
			}
		}
	}
}

// processEvent processes a single binlog event
func (m *MySQLConnector) processEvent(event *replication.BinlogEvent) error {
	switch e := event.Event.(type) {
	case *replication.RowsEvent:
		return m.processRowsEvent(event.Header, e)
	case *replication.TableMapEvent:
		return m.processTableMapEvent(e)
	case *replication.QueryEvent:
		// Handle DDL events if needed
		m.logger.WithField("query", string(e.Query)).Debug("DDL event received")
	}
	return nil
}

// processTableMapEvent processes table map events to cache schema info
func (m *MySQLConnector) processTableMapEvent(e *replication.TableMapEvent) error {
	database := string(e.Schema)
	table := string(e.Table)
	
	if !m.shouldProcessTable(database, table) {
		return nil
	}

	key := fmt.Sprintf("%s.%s", database, table)
	
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if we already have this table cached
	if _, exists := m.tableCache[key]; exists {
		return nil
	}

	// Get table info from canal
	tableInfo, err := m.canal.GetTable(database, table)
	if err != nil {
		return fmt.Errorf("failed to get table info: %w", err)
	}

	// Convert to our model
	columns := make([]models.ColumnInfo, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		columns = append(columns, models.ColumnInfo{
			Name: col.Name,
			Type: col.RawType,
		})
	}

	// Convert PKColumns from []int to []string
	pkColumns := make([]string, 0, len(tableInfo.PKColumns))
	for _, pkIdx := range tableInfo.PKColumns {
		if pkIdx >= 0 && pkIdx < len(tableInfo.Columns) {
			pkColumns = append(pkColumns, tableInfo.Columns[pkIdx].Name)
		}
	}

	m.tableCache[key] = &models.TableInfo{
		Database:   database,
		Table:      table,
		Columns:    columns,
		PrimaryKey: pkColumns,
	}

	return nil
}

// processRowsEvent processes row change events
func (m *MySQLConnector) processRowsEvent(header *replication.EventHeader, e *replication.RowsEvent) error {
	database := string(e.Table.Schema)
	table := string(e.Table.Table)
	
	if !m.shouldProcessTable(database, table) {
		return nil
	}

	key := fmt.Sprintf("%s.%s", database, table)
	
	m.mu.RLock()
	tableInfo, exists := m.tableCache[key]
	m.mu.RUnlock()

	if !exists {
		// Try to get table info
		if err := m.processTableMapEvent(&replication.TableMapEvent{
			Schema: []byte(database),
			Table:  []byte(table),
		}); err != nil {
			m.logger.WithError(err).Warn("Failed to get table info")
		}
		
		m.mu.RLock()
		tableInfo = m.tableCache[key]
		m.mu.RUnlock()
	}

	var changeType models.ChangeType
	switch header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		changeType = models.Insert
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		changeType = models.Update
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		changeType = models.Delete
	default:
		return fmt.Errorf("unknown event type: %v", header.EventType)
	}

	// Process rows
	for i, row := range e.Rows {
		// For update events, rows come in pairs (before, after)
		if changeType == models.Update && i%2 == 0 {
			continue
		}

		change := models.NewDataChange(changeType, database, table)
		change.BinlogFile = m.position.BinlogFile
		change.BinlogPos = m.position.BinlogPos
		change.Source = fmt.Sprintf("mysql://%s@%s:%d", m.cfg.Source.MySQL.User, m.cfg.Source.MySQL.Host, m.cfg.Source.MySQL.Port)

		// Add schema info
		if tableInfo != nil {
			schema := make(map[string]interface{})
			for _, col := range tableInfo.Columns {
				schema[col.Name] = map[string]interface{}{
					"type":     col.Type,
					"nullable": col.Nullable,
				}
			}
			change.Schema = schema
		}

		// Convert row data
		if changeType == models.Update {
			// Get before image (previous row)
			beforeRow := e.Rows[i-1]
			change.Before = m.rowToMap(beforeRow, tableInfo)
			change.After = m.rowToMap(row, tableInfo)
		} else if changeType == models.Delete {
			change.Before = m.rowToMap(row, tableInfo)
		} else {
			change.After = m.rowToMap(row, tableInfo)
		}

		// Send to channel
		select {
		case m.handler.changeChan <- change:
			m.logger.WithFields(map[string]interface{}{
				"database": database,
				"table":    table,
				"type":     changeType,
			}).Debug("Change event captured")
		default:
			m.logger.Warn("Change channel is full, dropping event")
		}
	}

	return nil
}

// rowToMap converts a row to a map
func (m *MySQLConnector) rowToMap(row []interface{}, tableInfo *models.TableInfo) map[string]interface{} {
	result := make(map[string]interface{})
	
	if tableInfo == nil {
		// Fallback: use column indices as keys
		for i, val := range row {
			result[fmt.Sprintf("col_%d", i)] = val
		}
		return result
	}

	for i, val := range row {
		if i < len(tableInfo.Columns) {
			colName := tableInfo.Columns[i].Name
			result[colName] = m.convertValue(val)
		}
	}
	
	return result
}

// convertValue converts MySQL values to appropriate Go types
func (m *MySQLConnector) convertValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case []byte:
		// Try to convert to string
		return string(v)
	default:
		return v
	}
}

// Stop stops the binlog capture
func (m *MySQLConnector) Stop() error {
	m.logger.Info("Stopping MySQL binlog capture...")
	close(m.stopChan)
	return nil
}

// GetTableInfo returns metadata about a table
func (m *MySQLConnector) GetTableInfo(database, table string) (*models.TableInfo, error) {
	key := fmt.Sprintf("%s.%s", database, table)
	
	m.mu.RLock()
	if info, exists := m.tableCache[key]; exists {
		m.mu.RUnlock()
		return info, nil
	}
	m.mu.RUnlock()

	// Try to fetch from MySQL
	if m.canal == nil {
		return nil, fmt.Errorf("canal not initialized")
	}

	tableInfo, err := m.canal.GetTable(database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}

	columns := make([]models.ColumnInfo, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		columns = append(columns, models.ColumnInfo{
			Name: col.Name,
			Type: col.RawType,
		})
	}

	// Convert PKColumns from []int to []string
	pkColumns := make([]string, 0, len(tableInfo.PKColumns))
	for _, pkIdx := range tableInfo.PKColumns {
		if pkIdx >= 0 && pkIdx < len(tableInfo.Columns) {
			pkColumns = append(pkColumns, tableInfo.Columns[pkIdx].Name)
		}
	}

	info := &models.TableInfo{
		Database:   database,
		Table:      table,
		Columns:    columns,
		PrimaryKey: pkColumns,
	}

	m.mu.Lock()
	m.tableCache[key] = info
	m.mu.Unlock()

	return info, nil
}

// OnRow implements canal.EventHandler (for future use with canal)
func (h *binlogHandler) OnRow(e *canal.RowsEvent) error {
	// This method is used when using canal's high-level API
	// Currently we use the low-level replication API
	return nil
}

// OnTableChanged implements canal.EventHandler
func (h *binlogHandler) OnTableChanged(schema string, table string) error {
	h.connector.logger.WithFields(map[string]interface{}{
		"schema": schema,
		"table":  table,
	}).Debug("Table changed")
	return nil
}

// OnPosSynced implements canal.EventHandler
func (h *binlogHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	h.connector.mu.Lock()
	h.connector.position.BinlogFile = pos.Name
	h.connector.position.BinlogPos = pos.Pos
	h.connector.mu.Unlock()
	return nil
}

// String implements canal.EventHandler
func (h *binlogHandler) String() string {
	return "binlogHandler"

}

// OnRotate implements canal.EventHandler
func (h *binlogHandler) OnRotate(e *replication.RotateEvent) error {
	h.connector.logger.WithFields(map[string]interface{}{
		"file": string(e.NextLogName),
		"pos":  e.Position,
	}).Debug("Binlog rotated")
	return nil
}

// OnDDL implements canal.EventHandler  
func (h *binlogHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	query := string(queryEvent.Query)
	
	// Log DDL events
	h.connector.logger.WithFields(map[string]interface{}{
		"query": query,
		"file":  nextPos.Name,
		"pos":   nextPos.Pos,
	}).Info("DDL event")

	// Parse CREATE TABLE and ALTER TABLE to update schema cache
	upperQuery := strings.ToUpper(query)
	if strings.Contains(upperQuery, "CREATE TABLE") || strings.Contains(upperQuery, "ALTER TABLE") {
		// Extract table name and refresh cache
		h.connector.invalidateTableCache()
	}

	return nil
}

// OnXID implements canal.EventHandler
func (h *binlogHandler) OnXID(pos mysql.Position) error {
	return nil
}

// OnGTID implements canal.EventHandler
func (h *binlogHandler) OnGTID(gtid mysql.GTIDSet) error {
	h.connector.logger.WithField("gtid", gtid.String()).Debug("GTID event")
	return nil
}

// invalidateTableCache clears the table cache
func (m *MySQLConnector) invalidateTableCache() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.tableCache = make(map[string]*models.TableInfo)
	m.logger.Debug("Table cache invalidated")
}
