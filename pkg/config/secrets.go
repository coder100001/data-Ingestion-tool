package config

import (
	"fmt"
	"os"
	"strings"
	"sync"
)

const (
	envVarPrefix    = "${"
	envVarSuffix    = "}"
	envVarSeparator = ":-"
)

var (
	unresolvedEnvVars []string
	envVarsMu         sync.Mutex
)

func parseEnvVar(value string) (string, bool, bool) {
	if value == "" {
		return "", false, false
	}

	if !strings.HasPrefix(value, envVarPrefix) || !strings.HasSuffix(value, envVarSuffix) {
		return value, false, false
	}

	inner := value[2 : len(value)-1]

	if idx := strings.Index(inner, envVarSeparator); idx != -1 {
		envName := inner[:idx]
		defaultValue := inner[idx+2:]

		if envVal := os.Getenv(envName); envVal != "" {
			return envVal, true, true
		}
		return defaultValue, true, true
	}

	if envVal := os.Getenv(inner); envVal != "" {
		return envVal, true, true
	}

	envVarsMu.Lock()
	unresolvedEnvVars = append(unresolvedEnvVars, inner)
	envVarsMu.Unlock()

	return "", true, false
}

func resolveValue(value string, envName string) string {
	// Environment variable value always takes highest priority
	if envVal := os.Getenv(envName); envVal != "" {
		return envVal
	}

	// Then check for ${ENV_VAR} or ${ENV_VAR:-default} syntax in config value
	if resolved, ok, success := parseEnvVar(value); ok {
		if !success {
			fmt.Printf("Warning: Environment variable not set and no default value provided for field '%s'\n", envName)
		}
		return resolved
	}

	return value
}

func GetUnresolvedEnvVars() []string {
	return unresolvedEnvVars
}

func ClearUnresolvedEnvVars() {
	unresolvedEnvVars = nil
}

func ResolveSecrets(cfg *Config) {
	ClearUnresolvedEnvVars()

	if cfg == nil {
		return
	}

	cfg.Source.MySQL.Password = resolveValue(cfg.Source.MySQL.Password, "MYSQL_PASSWORD")

	cfg.Source.PostgreSQL.Password = resolveValue(cfg.Source.PostgreSQL.Password, "POSTGRESQL_PASSWORD")

	if cfg.Source.REST.Headers != nil {
		if auth, exists := cfg.Source.REST.Headers["Authorization"]; exists {
			cfg.Source.REST.Headers["Authorization"] = resolveValue(auth, "REST_AUTHORIZATION")
		}
	}
}
