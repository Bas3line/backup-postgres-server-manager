package docker

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/pg-manager/internal/config"
)

type Manager struct {
	cfg *config.Config
}

func New(cfg *config.Config) *Manager {
	return &Manager{cfg: cfg}
}

func (m *Manager) EnsureBackupDatabases() error {
	m.createNetwork()
	m.ensureDataDirectories()

	for _, backup := range m.cfg.Backups {
		if backup.AutoCreate && backup.Active {
			m.ensureBackupContainer(backup)
		}
	}

	return nil
}

func (m *Manager) createNetwork() error {
	cmd := exec.Command("docker", "network", "ls", "--filter", fmt.Sprintf("name=%s", m.cfg.Docker.Network), "--quiet")
	output, err := cmd.Output()
	if err != nil {
		return err
	}

	if strings.TrimSpace(string(output)) != "" {
		return nil
	}

	cmd = exec.Command("docker", "network", "create", m.cfg.Docker.Network)
	return cmd.Run()
}

func (m *Manager) ensureDataDirectories() error {
	if err := os.MkdirAll(m.cfg.Docker.DataPath, 0755); err != nil {
		return err
	}

	for _, backup := range m.cfg.Backups {
		if backup.AutoCreate && backup.Active {
			dataDir := fmt.Sprintf("%s/%s", m.cfg.Docker.DataPath, backup.ID)
			if err := os.MkdirAll(dataDir, 0755); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *Manager) ensureBackupContainer(backup config.BackupDB) error {
	containerName := fmt.Sprintf("pg-manager-%s", backup.ID)

	if m.isContainerRunning(containerName) {
		return nil
	}

	if m.containerExists(containerName) {
		m.removeContainer(containerName)
	}

	return m.createBackupContainer(backup, containerName)
}

func (m *Manager) isContainerRunning(containerName string) bool {
	cmd := exec.Command("docker", "ps", "--filter", fmt.Sprintf("name=%s", containerName), "--quiet")
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(output)) != ""
}

func (m *Manager) containerExists(containerName string) bool {
	cmd := exec.Command("docker", "ps", "-a", "--filter", fmt.Sprintf("name=%s", containerName), "--quiet")
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(output)) != ""
}

func (m *Manager) removeContainer(containerName string) error {
	cmd := exec.Command("docker", "rm", "-f", containerName)
	return cmd.Run()
}

func (m *Manager) createBackupContainer(backup config.BackupDB, containerName string) error {
	dataDir := fmt.Sprintf("%s/%s", m.cfg.Docker.DataPath, backup.ID)

	args := []string{
		"run",
		"-d",
		"--name", containerName,
		"--network", m.cfg.Docker.Network,
		"-p", fmt.Sprintf("%d:5432", backup.Port),
		"-e", fmt.Sprintf("POSTGRES_USER=%s", backup.User),
		"-e", fmt.Sprintf("POSTGRES_PASSWORD=%s", backup.Password),
		"-e", fmt.Sprintf("POSTGRES_DB=%s", backup.Database),
		"-v", fmt.Sprintf("%s:/var/lib/postgresql/data", dataDir),
		"--restart", "unless-stopped",
		backup.DockerImage,
	}

	cmd := exec.Command("docker", args...)
	if err := cmd.Run(); err != nil {
		return err
	}

	return m.waitForDatabase(backup)
}

func (m *Manager) waitForDatabase(backup config.BackupDB) error {
	maxRetries := 30
	retryInterval := 2 * time.Second

	for i := 0; i < maxRetries; i++ {
		cmd := exec.Command("docker", "exec", fmt.Sprintf("pg-manager-%s", backup.ID),
			"pg_isready", "-h", "localhost", "-p", "5432", "-U", backup.User)

		if err := cmd.Run(); err == nil {
			return nil
		}

		time.Sleep(retryInterval)
	}

	return fmt.Errorf("backup database %s did not become ready within timeout", backup.ID)
}

func (m *Manager) StopBackupContainers() error {
	for _, backup := range m.cfg.Backups {
		if backup.AutoCreate && backup.Active {
			containerName := fmt.Sprintf("pg-manager-%s", backup.ID)
			if m.isContainerRunning(containerName) {
				cmd := exec.Command("docker", "stop", containerName)
				cmd.Run()
			}
		}
	}
	return nil
}

func (m *Manager) GetContainerStatus() map[string]string {
	status := make(map[string]string)

	for _, backup := range m.cfg.Backups {
		if backup.AutoCreate && backup.Active {
			containerName := fmt.Sprintf("pg-manager-%s", backup.ID)
			if m.isContainerRunning(containerName) {
				status[backup.ID] = "running"
			} else if m.containerExists(containerName) {
				status[backup.ID] = "stopped"
			} else {
				status[backup.ID] = "not_created"
			}
		}
	}

	return status
}
