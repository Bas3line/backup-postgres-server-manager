package logger

import (
	"sync"
	"time"
)

type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Table     string    `json:"table,omitempty"`
	Operation string    `json:"operation,omitempty"`
	Count     int       `json:"count,omitempty"`
}

type Logger struct {
	logs []LogEntry
	mu   sync.RWMutex
}

func New() *Logger {
	return &Logger{
		logs: make([]LogEntry, 0, 1000),
	}
}

func (l *Logger) Log(level, message, table, operation string, count int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	entry := LogEntry{
		Timestamp: time.Now(),
		Level:     level,
		Message:   message,
		Table:     table,
		Operation: operation,
		Count:     count,
	}
	
	l.logs = append(l.logs, entry)
	
	if len(l.logs) > 1000 {
		l.logs = l.logs[len(l.logs)-1000:]
	}
}

func (l *Logger) GetLogs(limit int) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	if limit <= 0 || limit > len(l.logs) {
		limit = len(l.logs)
	}
	
	start := len(l.logs) - limit
	if start < 0 {
		start = 0
	}
	
	result := make([]LogEntry, limit)
	copy(result, l.logs[start:])
	
	for i := 0; i < len(result)/2; i++ {
		result[i], result[len(result)-1-i] = result[len(result)-1-i], result[i]
	}
	
	return result
}

func (l *Logger) Info(message string) {
	l.Log("INFO", message, "", "", 0)
}

func (l *Logger) Error(message string) {
	l.Log("ERROR", message, "", "", 0)
}

func (l *Logger) Replication(operation, table string, count int, message string) {
	l.Log("REPLICATION", message, table, operation, count)
}
