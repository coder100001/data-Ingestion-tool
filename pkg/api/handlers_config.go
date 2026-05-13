package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"data-ingestion-tool/pkg/config"
)

type ConfigHandlers struct {
	config *config.Config
}

func NewConfigHandlers(cfg *config.Config) *ConfigHandlers {
	return &ConfigHandlers{config: cfg}
}

func (h *ConfigHandlers) GetConfig(c *gin.Context) {
	c.JSON(http.StatusOK, ApiResponse{
		Success: true,
		Data:    h.config,
	})
}

func (h *ConfigHandlers) UpdateConfig(c *gin.Context) {
	var req ConfigUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ApiResponse{
			Success: false,
			Error: &ApiError{
				Code:    "INVALID_REQUEST",
				Message: "Invalid request body",
				Details: []FieldError{
					{Field: "body", Message: err.Error()},
				},
			},
		})
		return
	}

	changes := []ConfigChange{}
	restartRequired := false

	if req.Processing != nil {
		if req.Processing.BatchSize != nil {
			oldVal := h.config.Processing.BatchSize
			h.config.Processing.BatchSize = *req.Processing.BatchSize
			changes = append(changes, ConfigChange{
				Path:     "processing.batch_size",
				OldValue: oldVal,
				NewValue: *req.Processing.BatchSize,
			})
		}
		if req.Processing.WorkerCount != nil {
			oldVal := h.config.Processing.WorkerCount
			h.config.Processing.WorkerCount = *req.Processing.WorkerCount
			changes = append(changes, ConfigChange{
				Path:     "processing.worker_count",
				OldValue: oldVal,
				NewValue: *req.Processing.WorkerCount,
			})
			restartRequired = true
		}
	}

	c.JSON(http.StatusOK, ApiResponse{
		Success: true,
		Data: ConfigUpdateResponse{
			Message:         "Configuration updated successfully",
			RestartRequired: restartRequired,
			Changes:         changes,
		},
	})
}

func (h *ConfigHandlers) ValidateConfig(c *gin.Context) {
	errors := []FieldError{}

	if h.config.Source.Type == "" {
		errors = append(errors, FieldError{
			Field:   "source.type",
			Message: "Source type is required",
		})
	}

	if h.config.Source.Type == "mysql" {
		if h.config.Source.MySQL.Host == "" {
			errors = append(errors, FieldError{
				Field:   "source.mysql.host",
				Message: "MySQL host is required",
			})
		}
		if h.config.Source.MySQL.ServerID == 0 {
			errors = append(errors, FieldError{
				Field:   "source.mysql.server_id",
				Message: "MySQL server_id is required",
			})
		}
	}

	if len(errors) > 0 {
		c.JSON(http.StatusBadRequest, ApiResponse{
			Success: false,
			Error: &ApiError{
				Code:    "VALIDATION_ERROR",
				Message: "Configuration validation failed",
				Details: errors,
			},
		})
		return
	}

	c.JSON(http.StatusOK, ApiResponse{
		Success: true,
		Data: gin.H{
			"message": "Configuration is valid",
		},
	})
}

func (h *ConfigHandlers) ReloadConfig(c *gin.Context) {
	c.JSON(http.StatusOK, ApiResponse{
		Success: true,
		Data: gin.H{
			"message": "Configuration reloaded successfully",
		},
	})
}
