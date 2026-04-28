package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/models"
)

// Manager handles checkpoint operations
type Manager struct {
	cfg          *config.CheckpointConfig
	logger       *logger.Logger
	checkpoint   *models.Checkpoint
	mu           sync.RWMutex
	saveTimer    *time.Timer
	dirty        bool
	stopChan     chan struct{}
	stopped      chan struct{}
}

// NewManager creates a new checkpoint manager
func NewManager(cfg *config.CheckpointConfig, logger *logger.Logger) (*Manager, error) {
	m := &Manager{
		cfg:        cfg,
		logger:     logger,
		checkpoint: nil,
		stopChan:   make(chan struct{}),
		stopped:    make(chan struct{}),
	}

	// Load existing checkpoint if available
	if err := m.Load(); err != nil {
		logger.WithError(err).Warn("Failed to load checkpoint, starting fresh")
		m.checkpoint = nil
	}

	// Start auto-save goroutine
	go m.autoSave()

	return m, nil
}

// Load loads the checkpoint from storage
func (m *Manager) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if file exists
	if _, err := os.Stat(m.cfg.StoragePath); os.IsNotExist(err) {
		m.logger.Info("No existing checkpoint found, starting fresh")
		return nil
	}

	// Read file
	data, err := os.ReadFile(m.cfg.StoragePath)
	if err != nil {
		return fmt.Errorf("failed to read checkpoint file: %w", err)
	}

	// Parse JSON
	var checkpoint models.Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return fmt.Errorf("failed to parse checkpoint: %w", err)
	}

	m.checkpoint = &checkpoint
	m.logger.WithFields(map[string]interface{}{
		"source_type": checkpoint.SourceType,
		"position":    checkpoint.Position,
		"updated_at":  checkpoint.UpdatedAt,
	}).Info("Checkpoint loaded successfully")

	return nil
}

// Save saves the checkpoint to storage
func (m *Manager) Save() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.checkpoint == nil {
		return nil
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(m.cfg.StoragePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(m.checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	// Write to temporary file first (atomic write)
	tempPath := m.cfg.StoragePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint temp file: %w", err)
	}

	// Rename to final path
	if err := os.Rename(tempPath, m.cfg.StoragePath); err != nil {
		return fmt.Errorf("failed to rename checkpoint file: %w", err)
	}

	m.dirty = false
	m.logger.WithFields(map[string]interface{}{
		"path":     m.cfg.StoragePath,
		"position": m.checkpoint.Position,
	}).Debug("Checkpoint saved")

	return nil
}

// ForceSave forces a checkpoint save regardless of dirty flag
// Used during emergency shutdown to preserve state
func (m *Manager) ForceSave() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.checkpoint == nil {
		m.logger.Warn("No checkpoint to save during emergency shutdown")
		return nil
	}

	dir := filepath.Dir(m.cfg.StoragePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	data, err := json.MarshalIndent(m.checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	tempPath := m.cfg.StoragePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint temp file: %w", err)
	}

	if err := os.Rename(tempPath, m.cfg.StoragePath); err != nil {
		return fmt.Errorf("failed to rename checkpoint file: %w", err)
	}

	m.dirty = false
	m.logger.WithFields(map[string]interface{}{
		"path":     m.cfg.StoragePath,
		"position": m.checkpoint.Position,
	}).Info("Emergency checkpoint saved")

	return nil
}

// GetCheckpoint returns the current checkpoint
func (m *Manager) GetCheckpoint() *models.Checkpoint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	if m.checkpoint == nil {
		return nil
	}
	
	// Return a copy
	checkpointCopy := *m.checkpoint
	return &checkpointCopy
}

// UpdatePosition updates the checkpoint position
func (m *Manager) UpdatePosition(position models.Position) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.checkpoint == nil {
		m.checkpoint = models.NewCheckpoint("unknown")
	}

	m.checkpoint.UpdatePosition(position)
	m.dirty = true
}

// Initialize creates a new checkpoint for the given source type
func (m *Manager) Initialize(sourceType string, position models.Position) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.checkpoint = models.NewCheckpoint(sourceType)
	m.checkpoint.Position = position
	m.dirty = true

	m.logger.WithFields(map[string]interface{}{
		"source_type": sourceType,
		"position":    position,
	}).Info("Checkpoint initialized")
}

// autoSave periodically saves the checkpoint
func (m *Manager) autoSave() {
	defer close(m.stopped)
	ticker := time.NewTicker(time.Duration(m.cfg.SaveIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if m.dirty {
				if err := m.Save(); err != nil {
					m.logger.WithError(err).Error("Failed to auto-save checkpoint")
				}
			}
		case <-m.stopChan:
			if m.dirty {
				if err := m.Save(); err != nil {
					m.logger.WithError(err).Error("Failed to save checkpoint on stop")
				}
			}
			return
		}
	}
}

// Stop stops the checkpoint manager
func (m *Manager) Stop() error {
	m.logger.Info("Stopping checkpoint manager...")

	close(m.stopChan)
	<-m.stopped

	m.logger.Info("Checkpoint manager stopped")
	return nil
}

// GetPosition returns the current position
func (m *Manager) GetPosition() models.Position {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.checkpoint == nil {
		return models.Position{}
	}

	return m.checkpoint.Position
}

// IsInitialized returns true if the checkpoint has been initialized
func (m *Manager) IsInitialized() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.checkpoint != nil
}

// Reset resets the checkpoint (useful for full re-sync)
func (m *Manager) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.checkpoint = nil
	m.dirty = false

	// Remove checkpoint file
	if err := os.Remove(m.cfg.StoragePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove checkpoint file: %w", err)
	}

	m.logger.Info("Checkpoint reset")
	return nil
}
