echo "Building pg-manager..."

go mod tidy
go test ./...

if [ $? -eq 0 ]; then
    echo "Tests passed. Building Docker image..."
    docker build -t pg-manager:latest .
    echo "Build complete!"
else
    echo "Tests failed. Build aborted."
    exit 1
fi
