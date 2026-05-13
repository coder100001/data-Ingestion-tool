package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"data-ingestion-tool/pkg/config"
)

func TestHealthCheck(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := &config.Config{}
	h := NewHandlers(cfg, "test-version")

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/health", nil)

	h.HealthCheck(c)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "ok")
}

func TestReadyCheck(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := &config.Config{}
	h := NewHandlers(cfg, "test-version")

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/ready", nil)

	h.ReadyCheck(c)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "ready")
}

func TestGetStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cfg := &config.Config{
		Source: config.SourceConfig{
			Type: "mysql",
		},
		Processing: config.ProcessingConfig{
			WorkerCount: 4,
		},
		Storage: config.StorageConfig{
			Type: "local",
		},
	}
	h := NewHandlers(cfg, "1.0.0")

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/api/v1/status", nil)

	h.GetStatus(c)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "success")
	assert.Contains(t, w.Body.String(), "1.0.0")
}
