package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"data-ingestion-tool/pkg/checkpoint"
	"data-ingestion-tool/pkg/config"
	"data-ingestion-tool/pkg/connector"
	"data-ingestion-tool/pkg/logger"
	"data-ingestion-tool/pkg/pipeline"
)

var (
	configPath = flag.String("config", "config.yaml", "Path to configuration file")
	resetCheckpoint = flag.Bool("reset", false, "Reset checkpoint and start from beginning")
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	flag.Parse()

	// Print version info
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Printf("Data Ingestion Tool\nVersion: %s\nCommit: %s\nBuild Date: %s\n", version, commit, date)
		os.Exit(0)
	}

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	log, err := logger.New(cfg.App.LogLevel, cfg.App.LogFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	log.WithFields(map[string]interface{}{
		"version": version,
		"config":  *configPath,
	}).Info("Starting Data Ingestion Tool")

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize checkpoint manager
	checkpointManager, err := checkpoint.NewManager(&cfg.Checkpoint, log)
	if err != nil {
		log.WithError(err).Fatal("Failed to initialize checkpoint manager")
	}
	defer checkpointManager.Stop()

	// Reset checkpoint if requested
	if *resetCheckpoint {
		if err := checkpointManager.Reset(); err != nil {
			log.WithError(err).Fatal("Failed to reset checkpoint")
		}
		log.Info("Checkpoint reset, starting from beginning")
	}

	// Initialize storage
	storage, err := pipeline.NewLocalStorage(&cfg.Storage.Local, log)
	if err != nil {
		log.WithError(err).Fatal("Failed to initialize storage")
	}

	// Initialize pipeline
	pipe := pipeline.NewPipeline(cfg, log, storage)
	if err := pipe.Start(); err != nil {
		log.WithError(err).Fatal("Failed to start pipeline")
	}
	defer pipe.Stop()

	// Initialize connector
	connectorFactory := connector.NewFactory(cfg, log)
	conn, err := connectorFactory.CreateConnector()
	if err != nil {
		log.WithError(err).Fatal("Failed to create connector")
	}

	// Connect to source
	if err := conn.Connect(ctx); err != nil {
		log.WithError(err).Fatal("Failed to connect to source")
	}
	defer conn.Disconnect()

	// Set checkpoint position if available
	if checkpointManager.IsInitialized() {
		pos := checkpointManager.GetPosition()
		if err := conn.SetPosition(pos); err != nil {
			log.WithError(err).Warn("Failed to set position from checkpoint")
		}
	}

	// Start capturing data changes
	changeChan := pipe.GetChangeChannel()
	if err := conn.Start(ctx, changeChan); err != nil {
		log.WithError(err).Fatal("Failed to start connector")
	}
	defer conn.Stop()

	// Start checkpoint updater
	go updateCheckpoint(ctx, checkpointManager, conn, log)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Info("Data ingestion started. Press Ctrl+C to stop.")

	select {
	case sig := <-sigChan:
		log.WithField("signal", sig).Info("Received shutdown signal")
	case <-ctx.Done():
		log.Info("Context cancelled")
	}

	// Graceful shutdown
	shutdown(log, conn, pipe, checkpointManager)
}

// updateCheckpoint periodically updates the checkpoint
func updateCheckpoint(ctx context.Context, manager *checkpoint.Manager, conn connector.Connector, log *logger.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if conn.IsConnected() {
				pos := conn.GetPosition()
				manager.UpdatePosition(pos)
			}
		}
	}
}

// shutdown performs graceful shutdown
func shutdown(log *logger.Logger, conn connector.Connector, pipe *pipeline.Pipeline, manager *checkpoint.Manager) {
	log.Info("Shutting down gracefully...")

	// Stop connector
	if err := conn.Stop(); err != nil {
		log.WithError(err).Error("Error stopping connector")
	}

	// Stop pipeline
	if err := pipe.Stop(); err != nil {
		log.WithError(err).Error("Error stopping pipeline")
	}

	// Save final checkpoint
	if err := manager.Stop(); err != nil {
		log.WithError(err).Error("Error stopping checkpoint manager")
	}

	log.Info("Shutdown complete")
}

// handleError handles errors and decides whether to continue or exit
func handleError(log *logger.Logger, err error, fatal bool) {
	if fatal {
		log.WithError(err).Fatal("Fatal error occurred")
	} else {
		log.WithError(err).Error("Error occurred")
	}
}

// printStats prints ingestion statistics
func printStats(log *logger.Logger, startTime time.Time, processedCount int64) {
	duration := time.Since(startTime)
	rate := float64(processedCount) / duration.Seconds()
	
	log.WithFields(map[string]interface{}{
		"duration":        duration,
		"processed_count": processedCount,
		"rate_per_sec":    rate,
	}).Info("Ingestion statistics")
}
