package config

import (
	"fmt"
	"encoding/json"
	"os"
	"time"
)

type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*d = Duration(time.Duration(value))
		return nil
	case string:
		tmp, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		*d = Duration(tmp)
		return nil
	default:
		return fmt.Errorf("invalid duration")
	}
}

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

type Config struct {
	Production  ProductionDB      `json:"production"`
	Backups     []BackupDB        `json:"backups"`
	Server      ServerConfig      `json:"server"`
	Replication ReplicationConfig `json:"replication"`
	Docker      DockerConfig      `json:"docker"`
}

type ProductionDB struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
	SSLMode  string `json:"ssl_mode"`
}

type BackupDB struct {
	ID          string `json:"id"`
	Host        string `json:"host"`
	Port        int    `json:"port"`
	User        string `json:"user"`
	Password    string `json:"password"`
	Database    string `json:"database"`
	SSLMode     string `json:"ssl_mode"`
	Priority    int    `json:"priority"`
	Active      bool   `json:"active"`
	AutoCreate  bool   `json:"auto_create"`
	DockerImage string `json:"docker_image"`
}

type ServerConfig struct {
	Port int `json:"port"`
}

type ReplicationConfig struct {
	BatchSize           int      `json:"batch_size"`
	WorkerCount         int      `json:"worker_count"`
	FlushInterval       Duration `json:"flush_interval"`
	MaxRetries          int      `json:"max_retries"`
	HealthCheckInterval Duration `json:"health_check_interval"`
}

type DockerConfig struct {
	Network  string `json:"network"`
	DataPath string `json:"data_path"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	setDefaults(&cfg)
	return &cfg, nil
}

func setDefaults(cfg *Config) {
	if cfg.Replication.BatchSize == 0 {
		cfg.Replication.BatchSize = 1000
	}
	if cfg.Replication.WorkerCount == 0 {
		cfg.Replication.WorkerCount = 10
	}
	if cfg.Replication.FlushInterval == 0 {
		cfg.Replication.FlushInterval = Duration(time.Second * 5)
	}
	if cfg.Replication.MaxRetries == 0 {
		cfg.Replication.MaxRetries = 3
	}
	if cfg.Replication.HealthCheckInterval == 0 {
		cfg.Replication.HealthCheckInterval = Duration(time.Second * 30)
	}
	if cfg.Production.SSLMode == "" {
		cfg.Production.SSLMode = "disable"
	}
	if cfg.Docker.Network == "" {
		cfg.Docker.Network = "db-replicator-network"
	}
	if cfg.Docker.DataPath == "" {
		cfg.Docker.DataPath = "./data"
	}
}
