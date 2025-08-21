package replicator

import (
	"context"
	"database/sql"
	"time"

	"github.com/pg-manager/internal/db"
)

type Worker struct {
	id        int
	backupDBs map[string]*sql.DB
	changesCh <-chan ChangeEvent
	batchSize int
	batch     []ChangeEvent
}

func NewWorker(id int, backupDBs map[string]*sql.DB, changesCh <-chan ChangeEvent, batchSize int) *Worker {
	return &Worker{
		id:        id,
		backupDBs: backupDBs,
		changesCh: changesCh,
		batchSize: batchSize,
		batch:     make([]ChangeEvent, 0, batchSize),
	}
}

func (w *Worker) Start(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.processBatch()
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

	for _, backupDB := range w.backupDBs {
		w.applyChanges(backupDB, w.batch)
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
