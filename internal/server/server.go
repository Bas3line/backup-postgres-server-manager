package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/pg-manager/internal/replicator"
	"github.com/gorilla/mux"
)

type Server struct {
	port       int
	replicator *replicator.Replicator
	server     *http.Server
}

func New(port int, repl *replicator.Replicator) *Server {
	return &Server{
		port:       port,
		replicator: repl,
	}
}

func (s *Server) Start() error {
	r := mux.NewRouter()
	
	r.HandleFunc("/health", s.healthHandler).Methods("GET")
	r.HandleFunc("/status", s.statusHandler).Methods("GET")
	r.HandleFunc("/query", s.queryHandler).Methods("POST")
	r.HandleFunc("/logs", s.logsHandler).Methods("GET")

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: r,
	}

	return s.server.ListenAndServe()
}

func (s *Server) Stop() {
	if s.server != nil {
		s.server.Close()
	}
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	status := s.replicator.GetStatus()
	json.NewEncoder(w).Encode(status)
}

func (s *Server) queryHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query string        `json:"query"`
		Args  []interface{} `json:"args"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	results, err := s.replicator.ExecuteQuery(req.Query, req.Args...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"results": results,
		"count":   len(results),
	})
}

func (s *Server) logsHandler(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	limit := 100
	
	if limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	logs := s.replicator.GetLogs(limit)
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"logs":  logs,
		"count": len(logs),
		"limit": limit,
	})
}
