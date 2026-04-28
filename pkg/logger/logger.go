package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/sirupsen/logrus"
)

type Logger struct {
	*logrus.Logger
	fileHook *fileHook
	closeMu  sync.Once
}

func New(level, logFile string) (*Logger, error) {
	log := logrus.New()

	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level: %w", err)
	}
	log.SetLevel(lvl)

	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})

	log.AddHook(NewSanitizeHook())

	logger := &Logger{Logger: log}

	if logFile != "" {
		dir := filepath.Dir(logFile)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %w", err)
		}

		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %w", err)
		}

		log.SetOutput(os.Stdout)
		fh := &fileHook{file: file}
		log.AddHook(fh)
		logger.fileHook = fh
	} else {
		log.SetOutput(os.Stdout)
	}

	return logger, nil
}

func (l *Logger) Close() error {
	var err error
	l.closeMu.Do(func() {
		if l.fileHook != nil {
			err = l.fileHook.Close()
		}
	})
	return err
}

type fileHook struct {
	file *os.File
	mu   sync.Mutex
}

func (h *fileHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h *fileHook) Fire(entry *logrus.Entry) error {
	line, err := entry.String()
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.file != nil {
		_, err = h.file.WriteString(line)
	}
	return err
}

func (h *fileHook) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.file != nil {
		err := h.file.Close()
		h.file = nil
		return err
	}
	return nil
}

func (l *Logger) WithField(key string, value interface{}) *logrus.Entry {
	return l.Logger.WithField(key, value)
}

func (l *Logger) WithFields(fields logrus.Fields) *logrus.Entry {
	return l.Logger.WithFields(fields)
}

func (l *Logger) WithError(err error) *logrus.Entry {
	return l.Logger.WithError(err)
}
