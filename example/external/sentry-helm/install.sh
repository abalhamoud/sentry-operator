helm repo add sentry https://sentry-kubernetes.github.io/charts
helm repo update
kubectl create namespace sentry
kubectl apply -f sentry-secret.yaml -n sentry
kubectl apply -f sentry-s3-credentials.yaml -n sentry
helm install sentry -n sentry sentry/sentry -f values.yaml --wait --timeout=1000s