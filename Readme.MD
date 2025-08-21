# PostgreSQL Backup Server Manager

<p align="center">
  <img src="https://img.shields.io/badge/Go-00ADD8?style=for-the-badge&logo=go&logoColor=white" alt="Go">
  <img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker">
  <img src="https://img.shields.io/badge/Kubernetes-326CE5?style=for-the-badge&logo=kubernetes&logoColor=white" alt="Kubernetes">
  <img src="https://img.shields.io/badge/REST-02569B?style=for-the-badge&logo=rest&logoColor=white" alt="REST API">
</p>

<p align="center">
  <img src="https://img.shields.io/badge/status-under%20development-orange?style=for-the-badge" alt="Under Development">
  <img src="https://img.shields.io/badge/production-not%20ready-red?style=for-the-badge" alt="Not Production Ready">
</p>

## **DEVELOPMENT STATUS**

> **This project is currently under active development and is NOT production-ready. Use at your own risk.**

## **What is PostgreSQL Backup Server Manager?**

A high-performance, real-time PostgreSQL replication system that automatically creates and manages multiple backup databases using Docker containers. Built with Go for maximum performance and reliability.

## ✨ **Key Features**

- **Lightning Fast Replication** - High-performance batch processing with concurrent workers
- **Auto-Docker Management** - Automatically creates backup PostgreSQL containers when they don't exist
- **Real-time Change Tracking** - Uses PostgreSQL LISTEN/NOTIFY for instant data synchronization
- **Multiple Backup Support** - Configure unlimited backup databases with priority levels
- **Health Monitoring** - Automatic failover and reconnection logic
- **REST API** - Query interface with JSON responses
- **Kubernetes Ready** - Full K8s deployment with ConfigMaps and Services
- **Production Features** - Connection pooling, graceful shutdown, comprehensive logging

## 🛠️ **Tech Stack**

| Technology | Purpose | Version |
|------------|---------|---------|
| ![Go](https://img.shields.io/badge/Go-00ADD8?style=flat-square&logo=go&logoColor=white) | Core Application | 1.24.6 |
| ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=flat-square&logo=postgresql&logoColor=white) | Database Engine | 15+ |
| ![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white) | Containerization | Latest |
| ![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=flat-square&logo=kubernetes&logoColor=white) | Orchestration | 1.20+ |
| ![Gorilla Mux](https://img.shields.io/badge/Gorilla%20Mux-000000?style=flat-square&logo=go&logoColor=white) | HTTP Router | v1.8.1 |

## 📦 **Dependencies**

```
github.com/gorilla/mux v1.8.1
github.com/lib/pq v1.10.9
```

## **Quick Start**

### Prerequisites
- Go 1.24.6+
- Docker & Docker Compose
- PostgreSQL (production database)

### 1. Clone & Setup
```
git clone https://github.com/Bas3line/backup-postgres-server-manager.git
cd backup-postgres-server-manager

# Copy and configure settings
cp config/config.example.json config/config.json
```

### 2. Configure Your Database
```
{
  "production": {
    "host": "localhost",
    "port": 5432,
    "user": "your_user",
    "password": "your_password",
    "database": "your_database",
    "ssl_mode": "disable"
  }
}
```

### 3. Run with Docker Compose
```
docker-compose up --build
```

### 4. Access the API
```
# Health check
curl http://localhost:8080/health

# System status
curl http://localhost:8080/status

# Execute queries
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT COUNT(*) FROM your_table"}'
```

## 📋 **Configuration**

### Basic Configuration
```
{
  "production": {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "your_password",
    "database": "production_db",
    "ssl_mode": "disable"
  },
  "backups": [
    {
      "id": "backup1",
      "host": "localhost",
      "port": 5433,
      "auto_create": true,
      "docker_image": "postgres:15"
    }
  ],
  "replication": {
    "batch_size": 1000,
    "worker_count": 10,
    "flush_interval": "5s"
  }
}
```

### Environment Variables
```
PG_HOST=localhost
PG_PORT=5432
PG_USER=postgres
PG_PASSWORD=your_password
PG_DATABASE=your_database
```

## **Architecture**

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Production DB  │───▶│   Replicator    │───▶│   Backup DBs    │
│   PostgreSQL    │    │  (Go Workers)   │    │ (Auto-Created)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   REST API      │
                       │   (Port 8080)   │
                       └─────────────────┘
```

## 🔌 **API Endpoints**

| Method | Endpoint | Description            |
|--------|----------|------------------------|
| `GET`  | `/health`| Service health check   |
| `GET`  | `/status`| Detailed system status |
| `POST` | `/query` | Execute SQL queries    |

### Query Example
```
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM users WHERE id = $1",
    "args": 
  }'
```

## **Kubernetes Deployment**

```
# Deploy to Kubernetes
kubectl apply -f k8s/

# Check deployment
kubectl get pods -l app=db-replicator

# View logs
kubectl logs -f deployment/db-replicator
```

## 🔧 **Development**

### Build from Source
```
go mod tidy
go build -o pg-manager .
./pg-manager
```

### Run Tests
```
go test ./...
```

### Development Mode
```
go run . -config config/config.json
```

## **Performance Features**

- **Batch Processing** - Configurable batch sizes for optimal throughput
- **Concurrent Workers** - Multiple worker goroutines for parallel processing  
- **Connection Pooling** - Optimized database connection management
- **Health Monitoring** - Automatic failover and recovery
- **Real-time Tracking** - PostgreSQL triggers for instant change detection

## 🤝 **Contributing**

We welcome contributions! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## **Acknowledgments**

- PostgreSQL Team for the amazing database
- Go Team for the incredible language
- Docker for containerization
- Kubernetes for orchestration

## **Support**

- 🐛 **Issues**: [GitHub Issues](https://github.com/Bas3line/backup-postgres-server-manager/issues)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/Bas3line/backup-postgres-server-manager/discussions)

---

<p align="center">
  Made with ❤️ by <a href="https://github.com/Bas3line">@Bas3line</a>
</p>

<p align="center">
  <img src="https://img.shields.io/github/stars/Bas3line/backup-postgres-server-manager?style=social" alt="GitHub Stars">
  <img src="https://img.shields.io/github/forks/Bas3line/backup-postgres-server-manager?style=social" alt="GitHub Forks">
  <img src="https://img.shields.io/github/watchers/Bas3line/backup-postgres-server-manager?style=social" alt="GitHub Watchers">
</p>