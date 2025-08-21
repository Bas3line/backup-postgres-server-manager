package replicator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/pg-manager/internal/config"
	"github.com/pg-manager/internal/db"
	"github.com/pg-manager/internal/docker"
)

type Replicator struct {
	cfg           *config.Config
	prodDB        *sql.DB
	backupDBs     map[string]*sql.DB
	listener      *pq.Listener
	changesCh     chan ChangeEvent
	workers       []*Worker
	dockerManager *docker.Manager
	mu            sync.RWMutex
	running       bool
}

func New(cfg *config.Config) (*Replicator, error) {
	r := &Replicator{
		cfg:           cfg,
		backupDBs:     make(map[string]*sql.DB),
		changesCh:     make(chan ChangeEvent, 10000),
		dockerManager: docker.New(cfg),
	}

	if err := r.dockerManager.EnsureBackupDatabases(); err != nil {
		return nil, fmt.Errorf("failed to ensure backup databases: %w", err)
	}

	if err := r.connectProduction(); err != nil {
		return nil, fmt.Errorf("failed to connect to production DB: %w", err)
	}

	time.Sleep(5 * time.Second)

	if err := r.connectBackups(); err != nil {
		return nil, fmt.Errorf("failed to connect to backup DBs: %w", err)
	}

	if err := r.setupChangeTracking(); err != nil {
		return nil, fmt.Errorf("failed to setup change tracking: %w", err)
	}

	r.initWorkers()
	return r, nil
}

func (r *Replicator) connectProduction() error {
	connStr := db.BuildConnectionString(
		r.cfg.Production.Host,
		r.cfg.Production.Port,
		r.cfg.Production.User,
		r.cfg.Production.Password,
		r.cfg.Production.Database,
		r.cfg.Production.SSLMode,
	)

	prodDB, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}

	prodDB.SetMaxOpenConns(50)
	prodDB.SetMaxIdleConns(25)
	prodDB.SetConnMaxLifetime(time.Hour)

	if err := prodDB.Ping(); err != nil {
		return fmt.Errorf("failed to ping production database: %w", err)
	}

	r.prodDB = prodDB
	return nil
}

func (r *Replicator) connectBackups() error {
	for _, backup := range r.cfg.Backups {
		if !backup.Active {
			continue
		}

		connStr := db.BuildConnectionString(
			backup.Host,
			backup.Port,
			backup.User,
			backup.Password,
			backup.Database,
			backup.SSLMode,
		)

		backupDB, err := sql.Open("postgres", connStr)
		if err != nil {
			continue
		}

		backupDB.SetMaxOpenConns(20)
		backupDB.SetMaxIdleConns(10)
		backupDB.SetConnMaxLifetime(time.Hour)

		maxRetries := 5
		for i := 0; i < maxRetries; i++ {
			if err := backupDB.Ping(); err != nil {
				if i < maxRetries-1 {
					time.Sleep(3 * time.Second)
					continue
				}
				backupDB.Close()
				continue
			}
			break
		}

		r.backupDBs[backup.ID] = backupDB
	}

	if len(r.backupDBs) == 0 {
		return fmt.Errorf("no backup databases available")
	}

	return nil
}

func (r *Replicator) setupChangeTracking() error {
	connStr := db.BuildConnectionString(
		r.cfg.Production.Host,
		r.cfg.Production.Port,
		r.cfg.Production.User,
		r.cfg.Production.Password,
		r.cfg.Production.Database,
		r.cfg.Production.SSLMode,
	)

	listener := pq.NewListener(connStr, 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf("PostgreSQL listener error: %v", err)
		}
	})

	if err := listener.Listen("table_changes"); err != nil {
		return err
	}

	r.listener = listener
	return nil
}

func (r *Replicator) initWorkers() {
	r.workers = make([]*Worker, r.cfg.Replication.WorkerCount)
	for i := 0; i < r.cfg.Replication.WorkerCount; i++ {
		r.workers[i] = NewWorker(i, r.backupDBs, r.changesCh, r.cfg.Replication.BatchSize)
	}
}

func (r *Replicator) Start(ctx context.Context) error {
	r.mu.Lock()
	r.running = true
	r.mu.Unlock()

	if err := r.performInitialSync(); err != nil {
		return fmt.Errorf("initial sync failed: %w", err)
	}

	for _, worker := range r.workers {
		go worker.Start(ctx)
	}

	go r.listenForChanges(ctx)
	go r.healthCheck(ctx)

	return nil
}

func (r *Replicator) performInitialSync() error {
	tables, err := r.getTables()
	if err != nil {
		return err
	}

	for _, table := range tables {
		if err := r.syncTable(table); err != nil {
			continue
		}
	}

	return nil
}

func (r *Replicator) getTables() ([]string, error) {
	query := `
		SELECT tablename 
		FROM pg_tables 
		WHERE schemaname = 'public' 
		AND tablename NOT LIKE 'pg_%'
		AND tablename NOT LIKE 'sql_%'
	`

	rows, err := r.prodDB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, rows.Err()
}

func (r *Replicator) syncTable(table string) error {
	if err := r.replicateTableStructure(table); err != nil {
		log.Printf("Warning: Could not replicate table structure for %s: %v", table, err)
	}

	var totalRows int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteIdentifier(table))
	if err := r.prodDB.QueryRow(countQuery).Scan(&totalRows); err != nil {
		return err
	}

	if totalRows == 0 {
		return nil
	}

	columns, err := r.getTableColumns(table)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("SELECT * FROM %s ORDER BY 1", pq.QuoteIdentifier(table))
	rows, err := r.prodDB.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	batch := make([]map[string]interface{}, 0, r.cfg.Replication.BatchSize)

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		record := make(map[string]interface{})
		for i, col := range columns {
			if values[i] == nil {
				record[col] = nil
			} else {
				record[col] = values[i]
			}
		}

		batch = append(batch, record)

		if len(batch) >= r.cfg.Replication.BatchSize {
			if err := r.insertBatch(table, batch, columns); err != nil {
				return fmt.Errorf("failed to insert batch: %w", err)
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := r.insertBatch(table, batch, columns); err != nil {
			return fmt.Errorf("failed to insert final batch: %w", err)
		}
	}

	return rows.Err()
}

func (r *Replicator) getTableColumns(table string) ([]string, error) {
	query := `
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_name = $1 AND table_schema = 'public'
		ORDER BY ordinal_position
	`

	rows, err := r.prodDB.Query(query, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}

	return columns, rows.Err()
}

func (r *Replicator) replicateTableStructure(table string) error {
	query := `
		SELECT 
			'CREATE TABLE IF NOT EXISTS ' || schemaname||'.'||tablename || ' (' ||
			string_agg(column_name || ' ' || type || not_null, ', ') || ');'
		FROM (
			SELECT 
				schemaname, tablename,
				column_name,
				data_type ||
				CASE 
					WHEN character_maximum_length IS NOT NULL 
					THEN '(' || character_maximum_length || ')'
					ELSE '' 
				END as type,
				CASE 
					WHEN is_nullable = 'NO' 
					THEN ' NOT NULL' 
					ELSE '' 
				END as not_null
			FROM information_schema.columns c
			JOIN pg_tables t ON c.table_name = t.tablename
			WHERE c.table_name = $1 AND c.table_schema = 'public'
			ORDER BY ordinal_position
		) AS table_def
		GROUP BY schemaname, tablename;
	`

	var createSQL string
	err := r.prodDB.QueryRow(query, table).Scan(&createSQL)
	if err != nil {
		return err
	}

	for _, backupDB := range r.backupDBs {
		if _, err := backupDB.Exec(createSQL); err != nil {
			continue
		}
	}

	return nil
}

func (r *Replicator) insertBatch(table string, batch []map[string]interface{}, columns []string) error {
	var lastErr error
	successCount := 0

	for _, backupDB := range r.backupDBs {
		if err := db.BulkInsert(backupDB, table, batch, columns); err != nil {
			lastErr = err
		} else {
			successCount++
		}
	}

	if successCount == 0 && lastErr != nil {
		return fmt.Errorf("all backup insertions failed, last error: %w", lastErr)
	}

	return nil
}

func (r *Replicator) listenForChanges(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case notification := <-r.listener.Notify:
			if notification != nil {
				var change ChangeEvent
				if err := json.Unmarshal([]byte(notification.Extra), &change); err != nil {
					continue
				}

				change.Timestamp = time.Now()

				select {
				case r.changesCh <- change:
				default:
				}
			}
		case <-time.After(90 * time.Second):
			if err := r.listener.Ping(); err != nil {
				log.Printf("Listener ping failed: %v", err)
			}
		}
	}
}

func (r *Replicator) healthCheck(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.Replication.HealthCheckInterval.Duration())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.checkBackupHealth()
		}
	}
}

func (r *Replicator) checkBackupHealth() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for backupID, backupDB := range r.backupDBs {
		if err := backupDB.Ping(); err != nil {
			backupDB.Close()
			delete(r.backupDBs, backupID)
		}
	}

	r.reconnectFailedBackups()
}

func (r *Replicator) reconnectFailedBackups() {
	for _, backup := range r.cfg.Backups {
		if !backup.Active {
			continue
		}

		if _, exists := r.backupDBs[backup.ID]; exists {
			continue
		}

		connStr := db.BuildConnectionString(
			backup.Host,
			backup.Port,
			backup.User,
			backup.Password,
			backup.Database,
			backup.SSLMode,
		)

		backupDB, err := sql.Open("postgres", connStr)
		if err != nil {
			continue
		}

		backupDB.SetMaxOpenConns(20)
		backupDB.SetMaxIdleConns(10)
		backupDB.SetConnMaxLifetime(time.Hour)

		if err := backupDB.Ping(); err != nil {
			backupDB.Close()
			continue
		}

		r.backupDBs[backup.ID] = backupDB
	}
}

func (r *Replicator) ExecuteQuery(query string, args ...interface{}) ([]map[string]interface{}, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.running || r.prodDB == nil {
		return nil, fmt.Errorf("replicator not running")
	}

	return db.ExecuteQuery(r.prodDB, query, args...)
}

func (r *Replicator) GetStatus() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	backupStatus := make(map[string]bool)
	for backupID := range r.backupDBs {
		backupStatus[backupID] = true
	}

	containerStatus := r.dockerManager.GetContainerStatus()

	return map[string]interface{}{
		"running":          r.running,
		"backup_count":     len(r.backupDBs),
		"backup_status":    backupStatus,
		"container_status": containerStatus,
		"queue_size":       len(r.changesCh),
		"worker_count":     len(r.workers),
		"database_type":    "postgresql",
		"production_db":    fmt.Sprintf("%s@%s:%d/%s", r.cfg.Production.User, r.cfg.Production.Host, r.cfg.Production.Port, r.cfg.Production.Database),
	}
}

func (r *Replicator) Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()

	if r.listener != nil {
		r.listener.Close()
	}

	if r.prodDB != nil {
		r.prodDB.Close()
	}

	for _, backupDB := range r.backupDBs {
		backupDB.Close()
	}

	close(r.changesCh)
}
