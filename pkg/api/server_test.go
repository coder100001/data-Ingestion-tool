package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	
	"github.com/stretchr/testify/assert"
	
	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
)

func TestNewServer(t *testing.T) {
	cfg := &config.Config{
		App: config.AppConfig{
			LogLevel: "info",
		},
		API: config.APIConfig{
			Enabled: true,
			Host:    "0.0.0.0",
			Port:    8080,
			CORS: config.CORSConfig{
				Enabled:        true,
				AllowedOrigins: []string{"*"},
			},
		},
	}
	
	log, _ := logger.New("info", "")
	srv := NewServer(cfg, log, "1.0.0")
	
	assert.NotNil(t, srv)
	assert.NotNil(t, srv.Engine())
}

func TestServerRoutes(t *testing.T) {
	cfg := &config.Config{
		App: config.AppConfig{},
		API: config.APIConfig{
			Enabled: true,
		},
	}
	
	log, _ := logger.New("info", "")
	srv := NewServer(cfg, log, "1.0.0")
	
	tests := []struct {
		path       string
		method     string
		expectCode int
	}{
		{"/health", "GET", http.StatusOK},
		{"/ready", "GET", http.StatusOK},
		{"/api/v1/status", "GET", http.StatusOK},
		{"/api/v1/config", "GET", http.StatusOK},
	}
	
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			w := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			srv.Engine().ServeHTTP(w, req)
			
			assert.Equal(t, tt.expectCode, w.Code)
		})
	}
}

func TestServerShutdown(t *testing.T) {
	cfg := &config.Config{
		API: config.APIConfig{
			Enabled: true,
		},
	}
	
	log, _ := logger.New("info", "")
	srv := NewServer(cfg, log, "1.0.0")
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	err := srv.Shutdown(ctx)
	assert.NoError(t, err)
}
