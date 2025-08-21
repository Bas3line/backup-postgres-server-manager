package replicator

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/pg-manager/internal/db"
	"github.com/pg-manager/internal/logger"
)

type Worker struct {
	id        int
	backupDBs map[string]*sql.DB
	changesCh <-chan ChangeEvent
	batchSize int
	batch     []ChangeEvent
	logger    *logger.Logger
}

func NewWorker(id int, backupDBs map[string]*sql.DB, changesCh <-chan ChangeEvent, batchSize int, logger *logger.Logger) *Worker {
	return &Worker{
		id:        id,
		backupDBs: backupDBs,
		changesCh: changesCh,
		batchSize: batchSize,
		batch:     make([]ChangeEvent, 0, batchSize),
		logger:    logger,
	}
}

func (w *Worker) Start(ctx context.Context) {
	log.Printf("Worker %d started", w.id)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.processBatch()
			log.Printf("Worker %d stopped", w.id)
			return
		case change, ok := <-w.changesCh:
			if !ok {
				w.processBatch()
				return
			}
			w.batch = append(w.batch, change)
			if len(w.batch) >= w.batchSize {
				w.processBatch()
			}
		case <-ticker.C:
			if len(w.batch) > 0 {
				w.processBatch()
			}
		}
	}
}

func (w *Worker) processBatch() {
	if len(w.batch) == 0 {
		return
	}

	successCount := 0

	for backupID, backupDB := range w.backupDBs {
		if err := w.applyChanges(backupDB, w.batch); err != nil {
			w.logger.Error(fmt.Sprintf("Worker %d failed to apply changes to backup %s: %v", w.id, backupID, err))
		} else {
			successCount++
		}
	}

	if successCount > 0 {
		operations := make(map[string]int)
		tables := make(map[string]int)
		
		for _, change := range w.batch {
			operations[change.Operation]++
			tables[change.Table]++
		}
		
		for table := range tables {
			for operation, opCount := range operations {
				if opCount > 0 {
					message := fmt.Sprintf("Worker %d processed %d %s operations on table %s to %d/%d backups", 
						w.id, opCount, operation, table, successCount, len(w.backupDBs))
					w.logger.Replication(operation, table, opCount, message)
				}
			}
		}
	}

	w.batch = w.batch[:0]
}

func (w *Worker) applyChanges(backupDB *sql.DB, changes []ChangeEvent) error {
	tx, err := backupDB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, change := range changes {
		if err := w.applyChange(tx, change); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (w *Worker) applyChange(tx *sql.Tx, change ChangeEvent) error {
	switch change.Operation {
	case "INSERT":
		return db.InsertRecord(tx, change.Table, change.Data)
	case "UPDATE":
		return db.UpdateRecord(tx, change.Table, change.Data)
	case "DELETE":
		return db.DeleteRecord(tx, change.Table, change.Data)
	default:
		return nil
	}
}
