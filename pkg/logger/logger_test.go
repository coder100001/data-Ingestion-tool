package logger

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_ValidLogLevel(t *testing.T) {
	tests := []struct {
		name  string
		level string
		want  logrus.Level
	}{
		{"debug level", "debug", logrus.DebugLevel},
		{"info level", "info", logrus.InfoLevel},
		{"warn level", "warn", logrus.WarnLevel},
		{"error level", "error", logrus.ErrorLevel},
		{"trace level", "trace", logrus.TraceLevel},
		{"fatal level", "fatal", logrus.FatalLevel},
		{"panic level", "panic", logrus.PanicLevel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, err := New(tt.level, "")
			require.NoError(t, err)
			require.NotNil(t, logger)
			assert.Equal(t, tt.want, logger.GetLevel())
		})
	}
}

func TestNew_InvalidLogLevel(t *testing.T) {
	logger, err := New("invalid", "")
	assert.Nil(t, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid log level")
}

func TestNew_WithLogFile(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	logger, err := New("info", logFile)
	require.NoError(t, err)
	require.NotNil(t, logger)
	assert.NotNil(t, logger.fileHook)

	logger.Info("test message")

	err = logger.Close()
	assert.NoError(t, err)

	_, err = os.Stat(logFile)
	assert.NoError(t, err)

	content, err := os.ReadFile(logFile)
	require.NoError(t, err)
	assert.Contains(t, string(content), "test message")
}

func TestNew_LogDirectoryCreation(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "subdir", "deep", "test.log")

	logger, err := New("info", logFile)
	require.NoError(t, err)
	require.NotNil(t, logger)

	err = logger.Close()
	assert.NoError(t, err)

	_, err = os.Stat(logFile)
	assert.NoError(t, err)
}

func TestNew_EmptyLogFile(t *testing.T) {
	logger, err := New("info", "")
	require.NoError(t, err)
	require.NotNil(t, logger)
	assert.Nil(t, logger.fileHook)
}

func TestLogger_Close(t *testing.T) {
	t.Run("close with file hook", func(t *testing.T) {
		tmpDir := t.TempDir()
		logFile := filepath.Join(tmpDir, "test.log")

		logger, err := New("info", logFile)
		require.NoError(t, err)

		err = logger.Close()
		assert.NoError(t, err)

		err = logger.Close()
		assert.NoError(t, err)
	})

	t.Run("close without file hook", func(t *testing.T) {
		logger, err := New("info", "")
		require.NoError(t, err)

		err = logger.Close()
		assert.NoError(t, err)
	})
}

func TestLogger_CloseNoPanic(t *testing.T) {
	logger, err := New("info", "")
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		logger.Close()
	})
}

func TestSanitizeFields_SensitiveFields(t *testing.T) {
	tests := []struct {
		name     string
		fields   logrus.Fields
		expected logrus.Fields
	}{
		{
			name:     "password field",
			fields:   logrus.Fields{"password": "secret123"},
			expected: logrus.Fields{"password": "******"},
		},
		{
			name:     "token field",
			fields:   logrus.Fields{"token": "abc123"},
			expected: logrus.Fields{"token": "******"},
		},
		{
			name:     "api_key field",
			fields:   logrus.Fields{"api_key": "key123"},
			expected: logrus.Fields{"api_key": "******"},
		},
		{
			name:     "secret field",
			fields:   logrus.Fields{"secret": "mysecret"},
			expected: logrus.Fields{"secret": "******"},
		},
		{
			name:     "credential field",
			fields:   logrus.Fields{"credential": "cred123"},
			expected: logrus.Fields{"credential": "******"},
		},
		{
			name:     "auth field",
			fields:   logrus.Fields{"auth": "authval"},
			expected: logrus.Fields{"auth": "******"},
		},
		{
			name:     "passwd field",
			fields:   logrus.Fields{"passwd": "pass123"},
			expected: logrus.Fields{"passwd": "******"},
		},
		{
			name:     "private_key field",
			fields:   logrus.Fields{"private_key": "pk123"},
			expected: logrus.Fields{"private_key": "******"},
		},
		{
			name:     "access_key field",
			fields:   logrus.Fields{"access_key": "ak123"},
			expected: logrus.Fields{"access_key": "******"},
		},
		{
			name:     "secret_key field",
			fields:   logrus.Fields{"secret_key": "sk123"},
			expected: logrus.Fields{"secret_key": "******"},
		},
		{
			name:     "key field",
			fields:   logrus.Fields{"key": "keyval"},
			expected: logrus.Fields{"key": "******"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeFields(tt.fields)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeFields_NonSensitiveFields(t *testing.T) {
	fields := logrus.Fields{
		"username": "john",
		"email":    "john@example.com",
		"age":      30,
		"active":   true,
	}

	result := SanitizeFields(fields)

	assert.Equal(t, "john", result["username"])
	assert.Equal(t, "john@example.com", result["email"])
	assert.Equal(t, 30, result["age"])
	assert.Equal(t, true, result["active"])
}

func TestSanitizeFields_MixedFields(t *testing.T) {
	fields := logrus.Fields{
		"username": "john",
		"password": "secret123",
		"email":    "john@example.com",
		"token":    "abc123",
	}

	result := SanitizeFields(fields)

	assert.Equal(t, "john", result["username"])
	assert.Equal(t, "******", result["password"])
	assert.Equal(t, "john@example.com", result["email"])
	assert.Equal(t, "******", result["token"])
}

func TestSanitizeFields_EmptyFields(t *testing.T) {
	fields := logrus.Fields{}
	result := SanitizeFields(fields)
	assert.Empty(t, result)
}

func TestSanitizeStringValue_PasswordPatterns(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
	}{
		{
			name:     "password= pattern",
			input:    "password=secret123",
			contains: "[REDACTED]",
		},
		{
			name:     "password: pattern",
			input:    "password:secret123",
			contains: "[REDACTED]",
		},
		{
			name:     "passwd= pattern",
			input:    "passwd=secret123",
			contains: "[REDACTED]",
		},
		{
			name:     "pwd= pattern",
			input:    "pwd=secret123",
			contains: "[REDACTED]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeStringValue(tt.input)
			assert.Contains(t, result, tt.contains)
			assert.NotContains(t, result, "secret123")
		})
	}
}

func TestSanitizeStringValue_TokenPatterns(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		contains string
	}{
		{
			name:     "secret= pattern",
			input:    "secret=mysecret",
			contains: "[REDACTED]",
		},
		{
			name:     "token= pattern",
			input:    "token=mytoken",
			contains: "[REDACTED]",
		},
		{
			name:     "key= pattern",
			input:    "key=mykey",
			contains: "[REDACTED]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeStringValue(tt.input)
			assert.Contains(t, result, tt.contains)
		})
	}
}

func TestSanitizeStringValue_BearerToken(t *testing.T) {
	input := "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
	result := SanitizeStringValue(input)
	assert.Contains(t, result, "[REDACTED]")
}

func TestSanitizeStringValue_BasicAuth(t *testing.T) {
	input := "Authorization: Basic dXNlcjpwYXNzd29yZA=="
	result := SanitizeStringValue(input)
	assert.Contains(t, result, "[REDACTED]")
}

func TestSanitizeStringValue_NoSensitiveData(t *testing.T) {
	input := "user=john&email=john@example.com"
	result := SanitizeStringValue(input)
	assert.Equal(t, input, result)
}

func TestSanitizeStringValue_EmptyString(t *testing.T) {
	result := SanitizeStringValue("")
	assert.Equal(t, "", result)
}

func TestSanitizeStringValue_CaseInsensitive(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"uppercase PASSWORD", "PASSWORD=secret"},
		{"mixed case Password", "Password=secret"},
		{"uppercase SECRET", "SECRET=hidden"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeStringValue(tt.input)
			assert.Contains(t, result, "[REDACTED]")
		})
	}
}

func TestIsSensitiveField(t *testing.T) {
	tests := []struct {
		field    string
		expected bool
	}{
		{"password", true},
		{"PASSWORD", true},
		{"user_password", true},
		{"password_hash", true},
		{"token", true},
		{"access_token", true},
		{"api_key", true},
		{"secret", true},
		{"credential", true},
		{"auth", true},
		{"username", false},
		{"email", false},
		{"name", false},
		{"id", false},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			result := isSensitiveField(tt.field)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeHook_Levels(t *testing.T) {
	hook := NewSanitizeHook()
	levels := hook.Levels()
	assert.Equal(t, logrus.AllLevels, levels)
}

func TestSanitizeHook_Fire(t *testing.T) {
	hook := NewSanitizeHook()

	entry := &logrus.Entry{
		Data: logrus.Fields{
			"password": "secret123",
			"username": "john",
		},
	}

	err := hook.Fire(entry)
	assert.NoError(t, err)
	assert.Equal(t, "******", entry.Data["password"])
	assert.Equal(t, "john", entry.Data["username"])
}

func TestSanitizeHook_FireNilData(t *testing.T) {
	hook := NewSanitizeHook()

	entry := &logrus.Entry{
		Data: nil,
	}

	err := hook.Fire(entry)
	assert.NoError(t, err)
}

func TestLogger_WithField(t *testing.T) {
	logger, err := New("info", "")
	require.NoError(t, err)

	entry := logger.WithField("key", "value")
	assert.NotNil(t, entry)
}

func TestLogger_WithFields(t *testing.T) {
	logger, err := New("info", "")
	require.NoError(t, err)

	fields := logrus.Fields{"key1": "value1", "key2": "value2"}
	entry := logger.WithFields(fields)
	assert.NotNil(t, entry)
}

func TestLogger_WithError(t *testing.T) {
	logger, err := New("info", "")
	require.NoError(t, err)

	testErr := assert.AnError
	entry := logger.WithError(testErr)
	assert.NotNil(t, entry)
}

func TestLogger_Integration_SanitizeHook(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	logger, err := New("info", logFile)
	require.NoError(t, err)

	logger.WithField("password", "secret123").Info("test message")

	err = logger.Close()
	require.NoError(t, err)

	content, err := os.ReadFile(logFile)
	require.NoError(t, err)

	contentStr := string(content)
	assert.Contains(t, contentStr, "******")
	assert.NotContains(t, contentStr, "secret123")
}

func TestFileHook_Levels(t *testing.T) {
	hook := &fileHook{}
	levels := hook.Levels()
	assert.Equal(t, logrus.AllLevels, levels)
}

func TestFileHook_Close(t *testing.T) {
	t.Run("close with valid file", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test*.log")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		hook := &fileHook{file: tmpFile}
		err = hook.Close()
		assert.NoError(t, err)
	})

	t.Run("close with nil file", func(t *testing.T) {
		hook := &fileHook{file: nil}
		err := hook.Close()
		assert.NoError(t, err)
	})

	t.Run("double close", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "test*.log")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		hook := &fileHook{file: tmpFile}
		err = hook.Close()
		assert.NoError(t, err)

		err = hook.Close()
		assert.NoError(t, err)
	})
}

func TestWarnPlaintextPasswords_NilLogger(t *testing.T) {
	assert.NotPanics(t, func() {
		WarnPlaintextPasswords(nil, struct{ Password string }{"secret"})
	})
}

func TestWarnPlaintextPasswords_NilConfig(t *testing.T) {
	logger, err := New("info", "")
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		WarnPlaintextPasswords(logger, nil)
	})
}

func TestWarnPlaintextPasswords_StructWithPassword(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	logger, err := New("warn", logFile)
	require.NoError(t, err)

	config := struct {
		Username string
		Password string
	}{
		Username: "john",
		Password: "secret123",
	}

	WarnPlaintextPasswords(logger, config)

	err = logger.Close()
	require.NoError(t, err)

	content, err := os.ReadFile(logFile)
	require.NoError(t, err)
	contentStr := string(content)
	assert.Contains(t, contentStr, "Security Warning")
}

func TestWarnPlaintextPasswords_MapWithPassword(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	logger, err := New("warn", logFile)
	require.NoError(t, err)

	config := map[string]string{
		"username": "john",
		"password": "secret123",
	}

	WarnPlaintextPasswords(logger, config)

	err = logger.Close()
	require.NoError(t, err)

	content, err := os.ReadFile(logFile)
	require.NoError(t, err)
	contentStr := string(content)
	assert.Contains(t, contentStr, "Security Warning")
}

func TestWarnPlaintextPasswords_EmptyPassword(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	logger, err := New("warn", logFile)
	require.NoError(t, err)

	config := struct {
		Username string
		Password string
	}{
		Username: "john",
		Password: "",
	}

	WarnPlaintextPasswords(logger, config)

	err = logger.Close()
	require.NoError(t, err)

	content, err := os.ReadFile(logFile)
	require.NoError(t, err)
	contentStr := string(content)
	assert.NotContains(t, contentStr, "Security Warning")
}

func TestLogger_OutputFormat(t *testing.T) {
	var buf bytes.Buffer
	log := logrus.New()
	log.SetOutput(&buf)
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})
	log.SetLevel(logrus.InfoLevel)

	log.Info("test message")

	output := buf.String()
	assert.Contains(t, output, "test message")
	assert.Contains(t, output, "level=info")
}

func TestNew_InvalidLogFilePath(t *testing.T) {
	logger, err := New("info", "/nonexistent/path/that/does/not/exist/test.log")
	assert.Error(t, err)
	assert.Nil(t, logger)
}
