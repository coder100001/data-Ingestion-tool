package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"data-ingestion-tool/pkg/config"
)

func TestGetConfig(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := &config.Config{
		App: config.AppConfig{
			Name: "test-app",
		},
	}
	h := NewConfigHandlers(cfg)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/v1/config", nil)

	h.GetConfig(c)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "test-app")
}

func TestUpdateConfig(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := &config.Config{
		Processing: config.ProcessingConfig{
			BatchSize:   100,
			WorkerCount: 4,
		},
	}
	h := NewConfigHandlers(cfg)

	body := ConfigUpdateRequest{
		Processing: &ProcessingConfigUpdate{
			BatchSize:   intPtr(200),
			WorkerCount: intPtr(8),
		},
	}
	jsonBody, _ := json.Marshal(body)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("PUT", "/api/v1/config", bytes.NewBuffer(jsonBody))
	c.Request.Header.Set("Content-Type", "application/json")

	h.UpdateConfig(c)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "Configuration updated successfully")
}

func TestValidateConfig(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "mysql",
			MySQL: config.MySQLConfig{
				Host:     "localhost",
				ServerID: 1001,
			},
		},
	}
	h := NewConfigHandlers(cfg)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/api/v1/config/validate", nil)

	h.ValidateConfig(c)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "valid")
}

func intPtr(i int) *int {
	return &i
}
