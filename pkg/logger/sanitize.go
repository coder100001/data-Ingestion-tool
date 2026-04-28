package logger

import (
	"reflect"
	"regexp"
	"strings"

	"github.com/sirupsen/logrus"
)

var sensitiveFields = []string{
	"password",
	"secret",
	"token",
	"key",
	"credential",
	"auth",
	"passwd",
	"private_key",
	"api_key",
	"access_key",
	"secret_key",
}

var sensitiveValuePatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(password|passwd|pwd)\s*[=:]\s*\S+`),
	regexp.MustCompile(`(?i)(secret|token|key)\s*[=:]\s*\S+`),
	regexp.MustCompile(`(?i)bearer\s+[a-zA-Z0-9\-._~+/]+=*`),
	regexp.MustCompile(`(?i)basic\s+[a-zA-Z0-9+/]+=*`),
}

func isSensitiveField(field string) bool {
	fieldLower := strings.ToLower(field)
	for _, sensitive := range sensitiveFields {
		if strings.Contains(fieldLower, sensitive) {
			return true
		}
	}
	return false
}

func SanitizeFields(fields logrus.Fields) logrus.Fields {
	sanitized := make(logrus.Fields, len(fields))
	for key, value := range fields {
		if isSensitiveField(key) {
			sanitized[key] = "******"
		} else {
			sanitized[key] = value
		}
	}
	return sanitized
}

func SanitizeStringValue(value string) string {
	sanitized := value
	for _, pattern := range sensitiveValuePatterns {
		sanitized = pattern.ReplaceAllString(sanitized, "[REDACTED]")
	}
	return sanitized
}

type SanitizeHook struct{}

func NewSanitizeHook() *SanitizeHook {
	return &SanitizeHook{}
}

func (h *SanitizeHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h *SanitizeHook) Fire(entry *logrus.Entry) error {
	if entry.Data == nil {
		return nil
	}

	for key := range entry.Data {
		if isSensitiveField(key) {
			entry.Data[key] = "******"
		}
	}

	return nil
}

func WarnPlaintextPasswords(logger *Logger, config interface{}) {
	if logger == nil || config == nil {
		return
	}

	checkStructForPasswords(logger, reflect.ValueOf(config))
}

func checkStructForPasswords(logger *Logger, v reflect.Value) {
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}

	if v.Kind() == reflect.Slice {
		for i := 0; i < v.Len(); i++ {
			checkStructForPasswords(logger, v.Index(i))
		}
		return
	}

	if v.Kind() == reflect.Map {
		for _, key := range v.MapKeys() {
			if key.Kind() == reflect.String && isSensitiveField(key.String()) {
				val := v.MapIndex(key)
				if val.Kind() == reflect.String && val.String() != "" {
					logger.WithField("field", key.String()).Warn("Security Warning: Plaintext password detected in configuration. Consider using environment variables or secret management systems.")
				}
			}
			checkStructForPasswords(logger, v.MapIndex(key))
		}
		return
	}

	if v.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		fieldValue := v.Field(i)

		if isSensitiveField(field.Name) {
			if fieldValue.Kind() == reflect.String && fieldValue.String() != "" {
				logger.WithField("field", field.Name).Warn("Security Warning: Plaintext password detected in configuration. Consider using environment variables or secret management systems.")
			}
		}

		checkStructForPasswords(logger, fieldValue)
	}
}
