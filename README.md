# Sentry Operator

A Kubernetes operator for deploying and managing [Sentry](https://sentry.io) instances on Kubernetes.

## Overview

The Sentry Operator automates the deployment and management of Sentry and its dependencies on Kubernetes. It provides a custom resource definition (CRD) called `SentryCluster` that allows you to define your Sentry deployment in a declarative way.

## Features

- Deploy and manage Sentry and all its dependencies:
  - PostgreSQL
  - Redis
  - Kafka
  - ClickHouse
  - Snuba
  - Relay
  - Symbolicator
- Automatic initialization and migration
- Support for external services (PostgreSQL, Redis, Kafka, ClickHouse)
- Configurable resource requests and limits
- Configurable persistence options
- Ingress configuration with TLS support
- Sentry-specific configuration options
- Horizontal scaling for web and worker components

## Prerequisites

- Kubernetes 1.19+
- Kubectl 1.19+
- Helm 3+ (optional, for installing the operator)

## Installation

### Using Kubectl

```bash
# Install the CRDs
kubectl apply -f https://github.com/abalhamoud/sentry-operator/releases/latest/download/sentry-operator-crds.yaml

# Install the operator
kubectl apply -f https://github.com/abalhamoud/sentry-operator/releases/latest/download/sentry-operator.yaml
```

### Using Helm

```bash
# Add the Helm repository
helm repo add sentry-operator https://abalhamoud.github.io/sentry-operator/charts

# Update the repository
helm repo update

# Install the operator
helm install sentry-operator sentry-operator/sentry-operator
```

## Usage

### Creating a SentryCluster

Create a YAML file with your SentryCluster definition:

```yaml
apiVersion: sentry.sentry.io/v1alpha1
kind: SentryCluster
metadata:
  name: my-sentry
spec:
  version: "24.5.0"
  resources:
    web:
      requests:
        cpu: "500m"
        memory: "512Mi"
      limits:
        cpu: "1000m"
        memory: "1Gi"
    # ... other resource configurations
  persistence:
    postgresql:
      storageClass: "standard"
      size: "10Gi"
    # ... other persistence configurations
  ingress:
    enabled: true
    host: "sentry.example.com"
    path: "/"
    tls:
      enabled: true
      secretName: "sentry-tls-secret"
  config:
    email:
      host: "smtp.example.com"
      port: 587
      user: "sentry@example.com"
      password: "your-smtp-password"
      from: "sentry@example.com"
    # ... other configurations
  replica:
    web: 2
    worker: 2
    relay: 2
```

Apply the SentryCluster:

```bash
kubectl apply -f my-sentry.yaml
```

### Using External Services

You can configure Sentry to use external services instead of deploying them as part of the SentryCluster:

```yaml
spec:
  persistence:
    postgresql:
      external:
        secretName: "external-postgres-secret"
    redis:
      external:
        secretName: "external-redis-secret"
    kafka:
      external:
        secretName: "external-kafka-secret"
    clickhouse:
      external:
        secretName: "external-clickhouse-secret"
```

The secrets should contain the necessary connection information for each service. For example, the PostgreSQL secret should contain:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: external-postgres-secret
type: Opaque
stringData:
  host: "postgres.example.com"
  port: "5432"
  database: "sentry"
  user: "sentry"
  password: "your-postgres-password"
```

### Scaling

You can scale the web and worker components by updating the `replica` field in your SentryCluster:

```yaml
spec:
  replica:
    web: 3
    worker: 5
    relay: 2
```

Apply the updated SentryCluster:

```bash
kubectl apply -f my-sentry.yaml
```

## Configuration Reference

### SentryCluster Spec

| Field | Type | Description |
|-------|------|-------------|
| `version` | string | The Sentry version to deploy. |
| `resources` | object | Resource requests and limits for Sentry components. |
| `persistence` | object | Persistence configuration for stateful components. |
| `ingress` | object | Ingress configuration for exposing Sentry. |
| `config` | object | Sentry-specific configuration. |
| `replica` | object | Replica count for scalable components. |

For a complete reference, see the [API documentation](docs/api.md).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
