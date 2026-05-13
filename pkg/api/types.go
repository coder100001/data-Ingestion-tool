package api

import "time"

type ApiResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   *ApiError   `json:"error,omitempty"`
}

type ApiError struct {
	Code    string       `json:"code"`
	Message string       `json:"message"`
	Details []FieldError `json:"details,omitempty"`
}

type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

type SystemStatus struct {
	Version       string          `json:"version"`
	UptimeSeconds int64           `json:"uptime_seconds"`
	State         string          `json:"state"`
	StartTime     time.Time       `json:"start_time"`
	Connector     ConnectorStatus `json:"connector"`
	Pipeline      PipelineStatus  `json:"pipeline"`
	Storage       StorageStatus   `json:"storage"`
	Sync          SyncStatus      `json:"sync"`
}

type ConnectorStatus struct {
	ID           string                 `json:"id"`
	Type         string                 `json:"type"`
	Name         string                 `json:"name"`
	State        string                 `json:"state"`
	Config       map[string]interface{} `json:"config,omitempty"`
	Metrics      ConnectorMetrics       `json:"metrics"`
	ErrorMessage string                 `json:"error_message,omitempty"`
}

type ConnectorMetrics struct {
	EventsProcessed int64     `json:"events_processed"`
	EventsPerSecond float64   `json:"events_per_second"`
	LagMs           int64     `json:"lag_ms"`
	LastEvent       time.Time `json:"last_event"`
	ReconnectCount  int       `json:"reconnect_count"`
}

type PipelineStatus struct {
	State         string `json:"state"`
	QueueSize     int    `json:"queue_size"`
	WorkersActive int    `json:"workers_active"`
}

type StorageStatus struct {
	Type        string  `json:"type"`
	BasePath    string  `json:"base_path"`
	TotalSizeMB float64 `json:"total_size_mb"`
	FileCount   int     `json:"file_count"`
}

type SyncStatus struct {
	Mode     string `json:"mode"`
	Position string `json:"position"`
	LagMs    int64  `json:"lag_ms"`
}

type SyncProgress struct {
	IsRunning       bool          `json:"is_running"`
	Mode            string        `json:"mode"`
	StartedAt       time.Time     `json:"started_at"`
	CurrentPosition BinlogPosition `json:"current_position"`
	Progress        ProgressDetail `json:"progress"`
	Throughput      Throughput    `json:"throughput"`
	Tables          []TableProgress `json:"tables"`
}

type BinlogPosition struct {
	BinlogFile string `json:"binlog_file"`
	BinlogPos  uint32 `json:"binlog_pos"`
	GTID       string `json:"gtid,omitempty"`
}

type ProgressDetail struct {
	TotalEvents              int64   `json:"total_events"`
	ProcessedEvents          int64   `json:"processed_events"`
	Percentage               float64 `json:"percentage"`
	EstimatedRemainingSeconds int64  `json:"estimated_remaining_seconds"`
}

type Throughput struct {
	EventsPerSecond float64 `json:"events_per_second"`
	BytesPerSecond  int64   `json:"bytes_per_second"`
}

type TableProgress struct {
	Database        string    `json:"database"`
	Table           string    `json:"table"`
	EventsProcessed int64     `json:"events_processed"`
	LastEventTime   time.Time `json:"last_event_time"`
}

type DataChange struct {
	ID            string                 `json:"id"`
	Timestamp     time.Time              `json:"timestamp"`
	Type          string                 `json:"type"`
	Database      string                 `json:"database"`
	Table         string                 `json:"table"`
	SchemaVersion string                 `json:"schema_version,omitempty"`
	Before        map[string]interface{} `json:"before,omitempty"`
	After         map[string]interface{} `json:"after,omitempty"`
	Metadata      ChangeMetadata         `json:"metadata"`
}

type ChangeMetadata struct {
	BinlogFile string `json:"binlog_file"`
	BinlogPos  uint32 `json:"binlog_pos"`
	ServerID   uint32 `json:"server_id"`
	ThreadID   uint32 `json:"thread_id"`
}

type ChangeStats struct {
	ByTable map[string]TableChangeStats `json:"by_table"`
	ByHour  []HourlyStats              `json:"by_hour"`
}

type TableChangeStats struct {
	Inserts int64 `json:"inserts"`
	Updates int64 `json:"updates"`
	Deletes int64 `json:"deletes"`
}

type HourlyStats struct {
	Hour    string `json:"hour"`
	Inserts int64  `json:"inserts"`
	Updates int64  `json:"updates"`
	Deletes int64  `json:"deletes"`
}

type DataPreview struct {
	Schema     DataSchema              `json:"schema"`
	Records    []map[string]interface{} `json:"records"`
	Pagination Pagination              `json:"pagination"`
	Partition  PartitionInfo           `json:"partition"`
}

type DataSchema struct {
	Columns []SchemaColumn `json:"columns"`
}

type SchemaColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

type Pagination struct {
	TotalRecords int64  `json:"total_records"`
	Limit        int    `json:"limit"`
	Offset       int    `json:"offset"`
	HasMore      bool   `json:"has_more"`
}

type PartitionInfo struct {
	Path           string `json:"path"`
	FileCount      int    `json:"file_count"`
	TotalSizeBytes int64  `json:"total_size_bytes"`
}

type DataQualityReport struct {
	Completeness float64           `json:"completeness"`
	Validity     float64           `json:"validity"`
	Uniqueness   float64           `json:"uniqueness"`
	Consistency  float64           `json:"consistency"`
	Issues       []DataQualityIssue `json:"issues"`
}

type DataQualityIssue struct {
	Column       string   `json:"column"`
	Type         string   `json:"type"`
	Count        int64    `json:"count"`
	SampleValues []string `json:"sample_values"`
}

type ConfigUpdateRequest struct {
	Processing *ProcessingConfigUpdate `json:"processing,omitempty"`
	Filters    []FilterConfig          `json:"filters,omitempty"`
}

type ProcessingConfigUpdate struct {
	BatchSize   *int `json:"batch_size,omitempty"`
	WorkerCount *int `json:"worker_count,omitempty"`
}

type FilterConfig struct {
	Table     string `json:"table"`
	Condition string `json:"condition"`
}

type ConfigUpdateResponse struct {
	Message         string         `json:"message"`
	RestartRequired bool           `json:"restart_required"`
	Changes         []ConfigChange `json:"changes"`
}

type ConfigChange struct {
	Path     string      `json:"path"`
	OldValue interface{} `json:"old_value"`
	NewValue interface{} `json:"new_value"`
}
