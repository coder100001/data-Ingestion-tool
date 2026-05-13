package api

import (
	"github.com/gin-gonic/gin"

	"data-ingestion-tool/pkg/config"
)

func setupRoutes(
	r *gin.Engine,
	handlers *Handlers,
	configHandlers *ConfigHandlers,
	apiCfg config.APIConfig,
) {
	r.GET("/health", handlers.HealthCheck)
	r.GET("/ready", handlers.ReadyCheck)
	
	v1 := r.Group("/api/v1")
	
	if apiCfg.Auth.Enabled {
		v1.Use(AuthMiddleware(apiCfg.Auth.APIKey))
	}
	
	v1.GET("/status", handlers.GetStatus)
	
	v1.GET("/config", configHandlers.GetConfig)
	v1.PUT("/config", configHandlers.UpdateConfig)
	v1.POST("/config/validate", configHandlers.ValidateConfig)
	v1.POST("/config/reload", configHandlers.ReloadConfig)
}
