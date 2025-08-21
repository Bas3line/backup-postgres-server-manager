package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pg-manager/internal/config"
	"github.com/pg-manager/internal/replicator"
	"github.com/pg-manager/internal/server"
)

func main() {
	log.Println("Starting PostgreSQL Backup Manager")
	
	configPath := flag.String("config", "config/config.json", "Path to config file")
	flag.Parse()

	log.Printf("Loading config from: %s", *configPath)
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}
	log.Println("Config loaded successfully")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Println("Creating replicator...")
	repl, err := replicator.New(cfg)
	if err != nil {
		log.Fatal("Failed to create replicator:", err)
	}
	log.Println("Replicator created successfully")

	log.Println("Creating server...")
	srv := server.New(cfg.Server.Port, repl)
	log.Println("Server created successfully")

	log.Printf("Starting server on port %d...", cfg.Server.Port)
	go func() {
		if err := srv.Start(); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	log.Println("Server started")

	log.Println("Starting replicator...")
	go func() {
		if err := repl.Start(ctx); err != nil {
			log.Printf("Replicator error: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	log.Println("Application started successfully")
	log.Println("Press Ctrl+C to stop...")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutdown signal received. Stopping...")
	repl.Stop()
	srv.Stop()
	log.Println("Application stopped gracefully")
}
