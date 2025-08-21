package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pg-manager/internal/config"
	"github.com/pg-manager/internal/replicator"
	"github.com/pg-manager/internal/server"
)

func main() {
	configPath := flag.String("config", "config/config.json", "Path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repl, err := replicator.New(cfg)
	if err != nil {
		log.Fatal("Failed to create replicator:", err)
	}

	srv := server.New(cfg.Server.Port, repl)

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatal("Server failed:", err)
		}
	}()

	go func() {
		if err := repl.Start(ctx); err != nil {
			log.Fatal("Replicator failed:", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	repl.Stop()
	srv.Stop()
}
