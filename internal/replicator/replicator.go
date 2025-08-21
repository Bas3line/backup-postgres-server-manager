package replicator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/pg-manager/internal/config"
	"github.com/pg-manager/internal/db"
	"github.com/pg-manager/internal/docker"
	"github.com/pg-manager/internal/logger"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type Replicator struct {
	cfg           *config.Config
	prodDB        *sql.DB
	backupDBs     map[string]*sql.DB
	listener      *pq.Listener
	changesCh     chan ChangeEvent
	workers       []*Worker
	dockerManager *docker.Manager
	logger        *logger.Logger
	mu            sync.RWMutex
	running       bool
}

func New(cfg *config.Config) (*Replicator, error) {
	r := &Replicator{
		cfg:           cfg,
		backupDBs:     make(map[string]*sql.DB),
		changesCh:     make(chan ChangeEvent, 10000),
		dockerManager: docker.New(cfg),
		logger:        logger.New(),
	}

	r.logger.Info("Starting PostgreSQL replicator initialization")

	if err := r.dockerManager.EnsureBackupDatabases(); err != nil {
		r.logger.Error(fmt.Sprintf("Failed to ensure backup databases: %v", err))
		return nil, fmt.Errorf("failed to ensure backup databases: %w", err)
	}

	if err := r.connectProduction(); err != nil {
		r.logger.Error(fmt.Sprintf("Failed to connect to production DB: %v", err))
		return nil, fmt.Errorf("failed to connect to production DB: %w", err)
	}

	time.Sleep(5 * time.Second)

	if err := r.connectBackups(); err != nil {
		r.logger.Error(fmt.Sprintf("Failed to connect to backup DBs: %v", err))
		return nil, fmt.Errorf("failed to connect to backup DBs: %w", err)
	}

	if err := r.setupChangeTracking(); err != nil {
		r.logger.Error(fmt.Sprintf("Failed to setup change tracking: %v", err))
		return nil, fmt.Errorf("failed to setup change tracking: %w", err)
	}

	r.initWorkers()
	r.logger.Info("PostgreSQL replicator initialized successfully")
	return r, nil
}

func (r *Replicator) connectProduction() error {
	r.logger.Info("Connecting to production database")
	
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	if err := prodDB.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping production database: %w", err)
	}

	r.prodDB = prodDB
	r.logger.Info(fmt.Sprintf("Connected to production database: %s@%s:%d/%s", 
		r.cfg.Production.User, r.cfg.Production.Host, r.cfg.Production.Port, r.cfg.Production.Database))
	return nil
}

func (r *Replicator) connectBackups() error {
	connectedCount := 0
	
	for _, backup := range r.cfg.Backups {
		if !backup.Active {
			continue
		}

		r.logger.Info(fmt.Sprintf("Attempting to connect to backup: %s", backup.ID))
		
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
			r.logger.Error(fmt.Sprintf("Failed to open connection to backup %s: %v", backup.ID, err))
			continue
		}

		backupDB.SetMaxOpenConns(20)
		backupDB.SetMaxIdleConns(10)
		backupDB.SetConnMaxLifetime(time.Hour)

		maxRetries := 3
		connectionSuccess := false
		
		for i := 0; i < maxRetries; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := backupDB.PingContext(ctx)
			cancel()
			
			if err == nil {
				connectionSuccess = true
				break
			}
			
			r.logger.Error(fmt.Sprintf("Retry %d/%d for backup %s: %v", i+1, maxRetries, backup.ID, err))
			time.Sleep(2 * time.Second)
		}

		if !connectionSuccess {
			r.logger.Error(fmt.Sprintf("Failed to connect to backup %s after %d retries", backup.ID, maxRetries))
			backupDB.Close()
			continue
		}

		r.backupDBs[backup.ID] = backupDB
		connectedCount++
		r.logger.Info(fmt.Sprintf("Connected to backup database: %s@%s:%d/%s", 
			backup.User, backup.Host, backup.Port, backup.Database))
	}

	r.logger.Info(fmt.Sprintf("Connected to %d backup databases", connectedCount))
	
	if connectedCount == 0 {
		r.logger.Error("Warning: No backup databases connected, continuing anyway")
	}

	return nil
}

func (r *Replicator) setupChangeTracking() error {
	r.logger.Info("Setting up PostgreSQL change tracking")
	
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
			r.logger.Error(fmt.Sprintf("PostgreSQL listener error: %v", err))
		}
	})

	if err := listener.Listen("table_changes"); err != nil {
		return err
	}

	r.listener = listener
	r.logger.Info("PostgreSQL change listener setup complete")
	return nil
}

func (r *Replicator) initWorkers() {
	r.workers = make([]*Worker, r.cfg.Replication.WorkerCount)
	for i := 0; i < r.cfg.Replication.WorkerCount; i++ {
		r.workers[i] = NewWorker(i, r.backupDBs, r.changesCh, r.cfg.Replication.BatchSize, r.logger)
	}
	r.logger.Info(fmt.Sprintf("Initialized %d PostgreSQL replication workers", r.cfg.Replication.WorkerCount))
}

func (r *Replicator) Start(ctx context.Context) error {
	r.mu.Lock()
	r.running = true
	r.mu.Unlock()

	r.logger.Info("Starting PostgreSQL initial data synchronization")
	if err := r.performInitialSync(); err != nil {
		return fmt.Errorf("initial sync failed: %w", err)
	}

	for _, worker := range r.workers {
		go worker.Start(ctx)
	}

	go r.listenForChanges(ctx)
	go r.healthCheck(ctx)

	r.logger.Info("PostgreSQL replicator started successfully")
	return nil
}

func (r *Replicator) performInitialSync() error {
	tables, err := r.getTables()
	if err != nil {
		return err
	}

	r.logger.Info(fmt.Sprintf("Found %d PostgreSQL tables to sync", len(tables)))
	for _, table := range tables {
		if err := r.syncTable(table); err != nil {
			r.logger.Error(fmt.Sprintf("Failed to sync PostgreSQL table %s: %v", table, err))
			continue
		}
	}

	r.logger.Info("PostgreSQL initial synchronization completed")
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
	r.logger.Info(fmt.Sprintf("Starting sync for table: %s", table))
	
	if err := r.replicateTableStructure(table); err != nil {
		r.logger.Error(fmt.Sprintf("Failed to replicate table structure for %s: %v", table, err))
	}

	var totalRows int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteIdentifier(table))
	if err := r.prodDB.QueryRow(countQuery).Scan(&totalRows); err != nil {
		r.logger.Error(fmt.Sprintf("Failed to count rows in table %s: %v", table, err))
		return err
	}

	if totalRows == 0 {
		r.logger.Info(fmt.Sprintf("Table %s is empty, skipping sync", table))
		return nil
	}

	r.logger.Info(fmt.Sprintf("Syncing %d rows from table %s", totalRows, table))

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
	processedRows := 0

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
		processedRows++

		if len(batch) >= r.cfg.Replication.BatchSize {
			if err := r.insertBatch(table, batch, columns); err != nil {
				return fmt.Errorf("failed to insert batch: %w", err)
			}
			r.logger.Replication("INITIAL_SYNC", table, len(batch), fmt.Sprintf("Synced %d/%d rows", processedRows, totalRows))
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := r.insertBatch(table, batch, columns); err != nil {
			return fmt.Errorf("failed to insert final batch: %w", err)
		}
		r.logger.Replication("INITIAL_SYNC", table, len(batch), "Synced final batch")
	}

	r.logger.Replication("INITIAL_SYNC", table, processedRows, fmt.Sprintf("Completed sync of %d rows to backup databases", processedRows))
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

	for backupID, backupDB := range r.backupDBs {
		if _, err := backupDB.Exec(createSQL); err != nil {
			r.logger.Error(fmt.Sprintf("Failed to create table structure in backup %s: %v", backupID, err))
		}
	}

	return nil
}

func (r *Replicator) insertBatch(table string, batch []map[string]interface{}, columns []string) error {
	var lastErr error
	successCount := 0

	for backupID, backupDB := range r.backupDBs {
		if err := db.BulkInsert(backupDB, table, batch, columns); err != nil {
			r.logger.Error(fmt.Sprintf("Failed to insert batch to backup %s: %v", backupID, err))
			lastErr = err
		} else {
			successCount++
			r.logger.Replication("INSERT", table, len(batch), fmt.Sprintf("Inserted %d records to backup %s", len(batch), backupID))
		}
	}

	if successCount == 0 && lastErr != nil {
		return fmt.Errorf("all backup insertions failed, last error: %w", lastErr)
	}

	return nil
}

func (r *Replicator) listenForChanges(ctx context.Context) {
	r.logger.Info("Starting PostgreSQL change listener")
	
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("PostgreSQL change listener stopping")
			return
		case notification := <-r.listener.Notify:
			if notification != nil {
				var change ChangeEvent
				if err := json.Unmarshal([]byte(notification.Extra), &change); err != nil {
					r.logger.Error(fmt.Sprintf("Failed to parse PostgreSQL change event: %v", err))
					continue
				}

				change.Timestamp = time.Now()

				select {
				case r.changesCh <- change:
					r.logger.Replication(change.Operation, change.Table, 1, fmt.Sprintf("Received %s change for table %s", change.Operation, change.Table))
				default:
					r.logger.Error("PostgreSQL changes channel full, dropping event")
				}
			}
		case <-time.After(90 * time.Second):
			if err := r.listener.Ping(); err != nil {
				r.logger.Error(fmt.Sprintf("PostgreSQL listener ping failed: %v", err))
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
			r.logger.Error(fmt.Sprintf("Backup %s health check failed: %v", backupID, err))
			backupDB.Close()
			delete(r.backupDBs, backupID)
			r.logger.Info(fmt.Sprintf("Removed failed backup: %s", backupID))
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

		r.logger.Info(fmt.Sprintf("Attempting to reconnect to backup: %s", backup.ID))

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
		r.logger.Info(fmt.Sprintf("Reconnected to backup database: %s", backup.ID))
	}
}

func (r *Replicator) ExecuteQuery(query string, args ...interface{}) ([]map[string]interface{}, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.running || r.prodDB == nil {
		return nil, fmt.Errorf("replicator not running")
	}

	r.logger.Info(fmt.Sprintf("Executing query: %s", query))
	
	results, err := db.ExecuteQuery(r.prodDB, query, args...)
	if err != nil {
		r.logger.Error(fmt.Sprintf("Query execution failed: %v", err))
		return nil, err
	}

	r.logger.Info(fmt.Sprintf("Query executed successfully, returned %d rows", len(results)))
	return results, nil
}

func (r *Replicator) GetLogs(limit int) []logger.LogEntry {
	return r.logger.GetLogs(limit)
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

	for backupID, backupDB := range r.backupDBs {
		backupDB.Close()
		r.logger.Info(fmt.Sprintf("Closed backup database: %s", backupID))
	}

	close(r.changesCh)
	r.logger.Info("PostgreSQL replicator stopped")
}
