echo "Deploying to Kubernetes..."

kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/postgres-failover.yaml
kubectl apply -f k8s/deployment.yaml

echo "Waiting for deployment to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/pg-manager

echo "Deployment complete!"
kubectl get pods -l app=pg-manager
