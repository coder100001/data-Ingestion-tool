package api

import (
	"context"
	"fmt"
	"net/http"
	"time"
	
	"github.com/gin-gonic/gin"
	
	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
)

type Server struct {
	engine    *gin.Engine
	httpSrv   *http.Server
	config    *config.Config
	handlers  *Handlers
	logger    *logger.Logger
	version   string
}

func NewServer(cfg *config.Config, log *logger.Logger, version string) *Server {
	gin.SetMode(gin.ReleaseMode)
	if cfg.App.LogLevel == "debug" {
		gin.SetMode(gin.DebugMode)
	}
	
	r := gin.New()
	r.Use(RecoveryMiddleware())
	r.Use(LoggerMiddleware())
	
	if cfg.API.CORS.Enabled {
		r.Use(CORSMiddleware(
			cfg.API.CORS.AllowedOrigins,
			cfg.API.CORS.AllowedMethods,
		))
	}
	
	if cfg.API.RateLimit.Enabled {
		r.Use(RateLimitMiddleware(
			cfg.API.RateLimit.RequestsPerSecond,
			cfg.API.RateLimit.Burst,
		))
	}
	
	s := &Server{
		engine:  r,
		config:  cfg,
		logger:  log,
		version: version,
	}
	
	s.handlers = NewHandlers(cfg, version)
	configHandlers := NewConfigHandlers(cfg)
	
	setupRoutes(r, s.handlers, configHandlers, cfg.API)
	
	return s
}

func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.config.API.Host, s.config.API.Port)
	
	s.httpSrv = &http.Server{
		Addr:         addr,
		Handler:      s.engine,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	
	s.logger.WithField("address", addr).Info("Starting API server")
	
	if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start API server: %w", err)
	}
	
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpSrv == nil {
		return nil
	}
	
	s.logger.Info("Shutting down API server")
	return s.httpSrv.Shutdown(ctx)
}

func (s *Server) Engine() *gin.Engine {
	return s.engine
}
