package api

import (
	"fmt"
	"net/http"
	"os"
	"runtime/debug"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ulule/limiter/v3"
	mgin "github.com/ulule/limiter/v3/drivers/middleware/gin"
	"github.com/ulule/limiter/v3/drivers/store/memory"
)

func LoggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()

		if status >= 400 {
			_ = c.Error(fmt.Errorf("HTTP %d %s %s latency=%s clientIP=%s", status, c.Request.Method, path, latency.String(), c.ClientIP()))
		}
	}
}

func CORSMiddleware(allowedOrigins, allowedMethods []string) gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")
		
		allowed := false
		for _, o := range allowedOrigins {
			if o == "*" || o == origin {
				allowed = true
				break
			}
		}
		
		if allowed {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Allow-Methods", joinMethods(allowedMethods))
			c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
			c.Header("Access-Control-Expose-Headers", "Content-Length, Content-Type")
		}
		
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		
		c.Next()
	}
}

func RateLimitMiddleware(requestsPerSecond int, burst int) gin.HandlerFunc {
	rate := limiter.Rate{
		Period: 1 * time.Second,
		Limit:  int64(requestsPerSecond),
   }
	store := memory.NewStore()
	instance := limiter.New(store, rate, limiter.WithTrustForwardHeader(true))
	middleware := mgin.NewMiddleware(instance)
	
	return middleware
}

func AuthMiddleware(apiKey string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if apiKey == "" {
			c.Next()
			return
		}
		
		providedKey := c.GetHeader("X-API-Key")
		if providedKey == "" {
			providedKey = c.Query("api_key")
		}
		
		if providedKey != apiKey {
			c.JSON(http.StatusUnauthorized, ApiResponse{
				Success: false,
				Error: &ApiError{
					Code:    "UNAUTHORIZED",
					Message: "Invalid or missing API key",
				},
			})
			c.Abort()
			return
		}
		
		c.Next()
	}
}

func RecoveryMiddleware() gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered interface{}) {
		stack := string(debug.Stack())
		_ = c.Error(fmt.Errorf("panic recovered: %v", recovered))
		// Write stack trace to stderr for debugging — stderr is always available
		// even if the logging infrastructure is broken
		fmt.Fprintf(os.Stderr, "[PANIC RECOVERED] %v\n%s\n", recovered, stack)
		c.JSON(http.StatusInternalServerError, ApiResponse{
			Success: false,
			Error: &ApiError{
				Code:    "INTERNAL_ERROR",
				Message: "An unexpected error occurred",
			},
		})
	})
}

func joinMethods(methods []string) string {
	result := ""
	for i, m := range methods {
		if i > 0 {
			result += ", "
		}
		result += m
	}
	return result
}
