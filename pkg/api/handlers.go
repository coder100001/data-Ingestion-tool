package api

import (
	"net/http"
	"runtime"
	"time"

	"github.com/gin-gonic/gin"

	"data-ingestion-tool/pkg/config"
)

type Handlers struct {
	config    *config.Config
	startTime time.Time
	version   string
}

func NewHandlers(cfg *config.Config, version string) *Handlers {
	return &Handlers{
		config:    cfg,
		startTime: time.Now(),
		version:   version,
	}
}

func (h *Handlers) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
	})
}

func (h *Handlers) ReadyCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"ready": true,
	})
}

func (h *Handlers) GetStatus(c *gin.Context) {
	uptime := time.Since(h.startTime)

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	status := SystemStatus{
		Version:       h.version,
		UptimeSeconds: int64(uptime.Seconds()),
		State:         "running",
		StartTime:     h.startTime,
		Connector: ConnectorStatus{
			ID:    "mysql-main",
			Type:  h.config.Source.Type,
			Name:  "Main Connector",
			State: "connected",
			Metrics: ConnectorMetrics{
				EventsProcessed: 0,
				EventsPerSecond: 0,
				LagMs:           0,
			},
		},
		Pipeline: PipelineStatus{
			State:         "running",
			QueueSize:     0,
			WorkersActive: h.config.Processing.WorkerCount,
		},
		Storage: StorageStatus{
			Type:        h.config.Storage.Type,
			BasePath:    h.config.Storage.Local.BasePath,
			TotalSizeMB: 0,
			FileCount:   0,
		},
		Sync: SyncStatus{
			Mode:     "incremental",
			Position: "",
			LagMs:    0,
		},
	}

	c.JSON(http.StatusOK, ApiResponse{
		Success: true,
		Data:    status,
	})
}
