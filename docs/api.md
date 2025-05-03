# SentryCluster API Reference

This document provides a detailed reference for the `SentryCluster` custom resource definition (CRD).

## SentryCluster

The `SentryCluster` resource defines a Sentry deployment and all its dependencies.

### Spec

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `version` | string | The Sentry version to deploy. | Yes |
| `resources` | [Resources](#resources) | Resource requests and limits for Sentry components. | Yes |
| `persistence` | [Persistence](#persistence) | Persistence configuration for stateful components. | Yes |
| `ingress` | [Ingress](#ingress) | Ingress configuration for exposing Sentry. | Yes |
| `config` | [Config](#config) | Sentry-specific configuration. | Yes |
| `replica` | [Replica](#replica) | Replica count for scalable components. | Yes |

### Resources

Resource requests and limits for Sentry components.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `web` | [ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#resourcerequirements-v1-core) | Resource requirements for Sentry web component. | Yes |
| `worker` | [ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#resourcerequirements-v1-core) | Resource requirements for Sentry worker component. | Yes |
| `postgres` | [ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#resourcerequirements-v1-core) | Resource requirements for PostgreSQL. | Yes |
| `redis` | [ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#resourcerequirements-v1-core) | Resource requirements for Redis. | Yes |
| `kafka` | [ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#resourcerequirements-v1-core) | Resource requirements for Kafka. | Yes |
| `clickhouse` | [ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#resourcerequirements-v1-core) | Resource requirements for ClickHouse. | Yes |
| `snuba` | [ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#resourcerequirements-v1-core) | General resource requirements for Snuba. | Yes |
| `relay` | [ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#resourcerequirements-v1-core) | Resource requirements for Relay. | Yes |
| `symbolicator` | [ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#resourcerequirements-v1-core) | Resource requirements for Symbolicator. | Yes |

### Persistence

Persistence configuration for stateful components.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `postgresql` | [PostgreSQLPersistence](#postgresqlpersistence) | PostgreSQL persistence configuration. | Yes |
| `redis` | [RedisPersistence](#redispersistence) | Redis persistence configuration. | Yes |
| `kafka` | [KafkaPersistence](#kafkapersistence) | Kafka persistence configuration. | Yes |
| `clickhouse` | [ClickHousePersistence](#clickhousepersistence) | ClickHouse persistence configuration. | Yes |
| `snuba` | [SnubaConfig](#snubaconfig) | Snuba configuration. | Yes |

#### PostgreSQLPersistence

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `storageClass` | string | The storage class to use for PostgreSQL PVCs. | No (uses default storage class if not specified) |
| `size` | string | The size of the PostgreSQL PVC (e.g., "10Gi"). | Yes (if external is not specified) |
| `external` | [ExternalPostgreSQL](#externalpostgresql) | External PostgreSQL configuration. | No |

#### ExternalPostgreSQL

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `secretName` | string | The name of the secret containing PostgreSQL connection information. | Yes |

The secret should contain the following keys:
- `host`: The PostgreSQL host.
- `port`: The PostgreSQL port.
- `database`: The PostgreSQL database name.
- `user`: The PostgreSQL user.
- `password`: The PostgreSQL password.

#### RedisPersistence

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `storageClass` | string | The storage class to use for Redis PVCs. | No (uses default storage class if not specified) |
| `size` | string | The size of the Redis PVC (e.g., "5Gi"). | Yes (if external is not specified) |
| `external` | [ExternalRedis](#externalredis) | External Redis configuration. | No |

#### ExternalRedis

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `secretName` | string | The name of the secret containing Redis connection information. | Yes |

The secret should contain the following keys:
- `host`: The Redis host.
- `port`: The Redis port.
- `password`: The Redis password.

#### KafkaPersistence

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `storageClass` | string | The storage class to use for Kafka PVCs. | No (uses default storage class if not specified) |
| `size` | string | The size of the Kafka PVC (e.g., "10Gi"). | Yes (if external is not specified) |
| `external` | [ExternalKafka](#externalkafka) | External Kafka configuration. | No |

#### ExternalKafka

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `secretName` | string | The name of the secret containing Kafka connection information. | Yes |

The secret should contain the following keys:
- `bootstrap.servers`: The Kafka bootstrap servers.

#### ClickHousePersistence

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `storageClass` | string | The storage class to use for ClickHouse PVCs. | No (uses default storage class if not specified) |
| `size` | string | The size of the ClickHouse PVC (e.g., "20Gi"). | Yes (if external is not specified) |
| `external` | [ExternalClickHouse](#externalclickhouse) | External ClickHouse configuration. | No |

#### ExternalClickHouse

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `secretName` | string | The name of the secret containing ClickHouse connection information. | Yes |

The secret should contain the following keys:
- `host`: The ClickHouse host.
- `port`: The ClickHouse port.
- `http_port`: The ClickHouse HTTP port.
- `user`: The ClickHouse user.
- `password`: The ClickHouse password.
- `database`: The ClickHouse database name.

#### SnubaConfig

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `replicas` | [SnubaReplicas](#snubareplicas) | Replica configuration for Snuba components. | No |
| `resources` | [SnubaResources](#snubaresources) | Resource requirements for Snuba components. | No |
| `config` | map[string]string | Snuba-specific configuration. | No |

#### SnubaReplicas

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `api` | integer | The number of Snuba API replicas. | No (defaults to 1) |
| `consumer` | integer | The number of Snuba consumer replicas. | No (defaults to 1) |
| `replacer` | integer | The number of Snuba replacer replicas. | No (defaults to 1) |
| `subscriptionConsumer` | integer | The number of Snuba subscription-consumer replicas. | No (defaults to 1) |

#### SnubaResources

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `api` | [ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#resourcerequirements-v1-core) | Resource requirements for Snuba API. | No |
| `consumer` | [ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#resourcerequirements-v1-core) | Resource requirements for Snuba consumer. | No |
| `replacer` | [ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#resourcerequirements-v1-core) | Resource requirements for Snuba replacer. | No |
| `subscriptionConsumer` | [ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#resourcerequirements-v1-core) | Resource requirements for Snuba subscription-consumer. | No |

### Ingress

Ingress configuration for exposing Sentry.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `enabled` | boolean | Whether to enable the ingress. | Yes |
| `host` | string | The hostname for the ingress. | Yes |
| `path` | string | The path for the ingress. | No (defaults to "/") |
| `tls` | [IngressTLS](#ingresstls) | TLS configuration for the ingress. | No |

#### IngressTLS

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `enabled` | boolean | Whether to enable TLS. | Yes |
| `secretName` | string | The name of the secret containing the TLS certificate. | Yes |

### Config

Sentry-specific configuration.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `email` | [EmailConfig](#emailconfig) | Email configuration. | Yes |
| `auth` | [AuthConfig](#authconfig) | Authentication configuration. | No |
| `features` | [FeaturesConfig](#featuresconfig) | Feature flags. | No |

#### EmailConfig

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `host` | string | The SMTP host. | Yes |
| `port` | integer | The SMTP port. | Yes |
| `user` | string | The SMTP user. | Yes |
| `password` | string | The SMTP password. | Yes |
| `from` | string | The email address to send from. | Yes |

#### AuthConfig

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `allowRegistration` | boolean | Whether to allow user registration. | No (defaults to false) |
| `requireEmailVerification` | boolean | Whether to require email verification. | No (defaults to true) |

#### FeaturesConfig

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `organizations` | [OrganizationsConfig](#organizationsconfig) | Organizations feature configuration. | No |
| `projects` | [ProjectsConfig](#projectsconfig) | Projects feature configuration. | No |

#### OrganizationsConfig

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `default` | boolean | Whether to enable organizations by default. | No (defaults to true) |

#### ProjectsConfig

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `default` | boolean | Whether to enable projects by default. | No (defaults to true) |

### Replica

Replica count for scalable components.

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `web` | integer | The number of Sentry web replicas. | No (defaults to 1) |
| `worker` | integer | The number of Sentry worker replicas. | No (defaults to 1) |
| `relay` | integer | The number of Relay replicas. | No (defaults to 1) |
| `snuba` | [SnubaReplicas](#snubareplicas) | The number of Snuba replicas for each component. | No |

## Status

The `SentryCluster` resource also has a status field that provides information about the current state of the Sentry deployment.

| Field | Type | Description |
|-------|------|-------------|
| `phase` | string | The current phase of the Sentry deployment (e.g., "Pending", "Running", "Error"). |
| `version` | string | The current version of Sentry. |
| `url` | string | The URL to access Sentry. |
| `lastUpdated` | [Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#time-v1-meta) | The last time the status was updated. |
| `observedGeneration` | integer | The generation observed by the controller. |
| `conditions` | [Conditions](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/#condition-v1-meta) | The current conditions of the Sentry deployment. |
| `componentStatus` | [ComponentStatus](#componentstatus) | The status of each Sentry component. |

### ComponentStatus

| Field | Type | Description |
|-------|------|-------------|
| `postgresql` | [ComponentStatusDetail](#componentstatusdetail) | The status of the PostgreSQL component. |
| `redis` | [ComponentStatusDetail](#componentstatusdetail) | The status of the Redis component. |
| `kafka` | [ComponentStatusDetail](#componentstatusdetail) | The status of the Kafka component. |
| `clickhouse` | [ComponentStatusDetail](#componentstatusdetail) | The status of the ClickHouse component. |
| `snuba` | [ComponentStatusDetail](#componentstatusdetail) | The status of the Snuba component. |
| `symbolicator` | [ComponentStatusDetail](#componentstatusdetail) | The status of the Symbolicator component. |
| `web` | [ComponentStatusDetail](#componentstatusdetail) | The status of the Sentry web component. |
| `worker` | [ComponentStatusDetail](#componentstatusdetail) | The status of the Sentry worker component. |
| `relay` | [ComponentStatusDetail](#componentstatusdetail) | The status of the Relay component. |

### ComponentStatusDetail

| Field | Type | Description |
|-------|------|-------------|
| `ready` | boolean | Whether the component is ready. |
| `message` | string | A message describing the current state of the component. |
