# Sentry Operator Design Choices

This document explains the design choices made for the Sentry Custom Resource Definition (CRD) and addresses the important considerations for each field in the specification.

## SentryClusterSpec Fields

### Version

**Why included**: The Version field is crucial for specifying which Sentry version to deploy. It enables users to control which version they want to run and facilitates upgrades.

**Data type**: `string` - This allows for semantic versioning formats like "24.5.0".

**Possible values**: Any valid Sentry version (e.g., "24.5.0", "24.4.0").

**Effect on deployment**: Determines the Sentry container image tag used in the deployment.

**Security considerations**: No special security considerations.

**Validation rules**: Should be a valid semantic version string.

### Resources

**Why included**: The Resources field allows users to configure CPU and memory requests and limits for each Sentry component. This is essential for proper resource management in Kubernetes.

**Data type**: Custom `Resources` struct containing `corev1.ResourceRequirements` fields for each component.

**Possible values**: Valid Kubernetes resource requests and limits for CPU and memory.

**Effect on deployment**: Determines the resource allocation for each component, affecting performance and stability.

**Security considerations**: No special security considerations.

**Validation rules**: Should follow Kubernetes resource quantity format.

### Persistence

**Why included**: The Persistence field handles storage requirements for PostgreSQL, Redis, Kafka, ClickHouse, and Snuba. This is essential for data durability.

**Data type**: Custom `Persistence` struct with configurations for each data store.

**Possible values**:
- For managed instances: StorageClass name and size
- For external instances: Connection details via Secret references

**Effect on deployment**: Determines how data is stored and persisted, affecting data durability and performance.

**Security considerations**: Connection credentials for external services are stored in Secrets.

**Validation rules**:
- Size should be a valid Kubernetes quantity
- StorageClass should exist in the cluster
- Secret references should point to existing Secrets

### Ingress

**Why included**: The Ingress field provides options for exposing Sentry to external traffic, supporting different Ingress controllers.

**Data type**: Custom `Ingress` struct with configuration options.

**Possible values**:
- Ingress types: "nginx", "traefik", "openshift-route"
- TLS configuration
- Host and path settings

**Effect on deployment**: Determines how Sentry is exposed externally and how TLS is configured.

**Security considerations**: TLS certificates should be properly managed.

**Validation rules**:
- Host should be a valid domain name
- Path should be a valid URL path

### Config

**Why included**: The Config field manages Sentry-specific settings, allowing users to customize Sentry's behavior.

**Data type**: Custom `Config` struct with nested configuration options.

**Possible values**: Various settings for email, authentication, rate limiting, data retention, etc.

**Effect on deployment**: Determines Sentry's behavior and features.

**Security considerations**:
- Secret key should be stored securely
- Integration credentials are stored in Secrets
- Privacy settings affect data handling

**Validation rules**:
- Email addresses should be valid
- Rate limits should be positive integers
- Retention days should be positive integers

### Replica

**Why included**: The Replica field defines the number of replicas for each component, allowing users to scale their Sentry deployment.

**Data type**: Custom `Replica` struct with integer fields for each component.

**Possible values**: Positive integers representing the number of replicas.

**Effect on deployment**: Determines the scale and high availability of the Sentry deployment.

**Security considerations**: No special security considerations.

**Validation rules**: Values should be positive integers.

## Detailed Component Configurations

### PostgreSQL Configuration

PostgreSQL is Sentry's primary database, storing user accounts, projects, and other metadata.

**Design choices**:
- Support for both managed and external PostgreSQL
- For managed instances, configurable storage class and size
- For external instances, connection details via Secret
- Resource requirements configurable

**Considerations**:
- Data durability is critical
- Performance impacts overall Sentry performance
- Backup and restore capabilities are important

### Redis Configuration

Redis is used for caching, rate limiting, and as a message broker.

**Design choices**:
- Support for both managed and external Redis
- For managed instances, configurable storage class and size
- For external instances, connection details via Secret
- Resource requirements configurable

**Considerations**:
- Performance impacts overall Sentry performance
- Data persistence is important but less critical than PostgreSQL

### Kafka Configuration

Kafka is used for event streaming in Sentry's processing pipeline.

**Design choices**:
- Support for both managed and external Kafka
- For managed instances, configurable replicas, resources, and storage
- For external instances, connection details and topic configuration via Secret
- Configurable broker settings

**Considerations**:
- Performance impacts event processing throughput
- Topic configuration is important for proper event routing

### ClickHouse Configuration

ClickHouse is used for analytics and event storage.

**Design choices**:
- Support for both managed and external ClickHouse
- For managed instances, configurable replicas, resources, and storage
- For external instances, connection details via Secret
- Configurable ClickHouse settings

**Considerations**:
- Performance impacts query performance for event data
- Storage requirements can be significant for high-volume deployments

### Snuba Configuration

Snuba is Sentry's event storage service that sits on top of ClickHouse.

**Design choices**:
- Split into multiple role-specific deployments (API, consumer, replacer, subscription-consumer)
- Configurable replicas for each role independently
- Separate resource requirements for each role
- Role-specific commands and arguments
- Configurable Snuba-specific settings
- Labels to distinguish each deployment
- Status tracking via conditions for each role

**Considerations**:
- Performance impacts query performance for event data
- Different roles have different scaling characteristics and resource needs
- API role handles query requests and needs to be scaled for read performance
- Consumer role processes incoming events and needs to be scaled for write throughput
- Replacer role handles event replacements and deletions
- Subscription-consumer role handles subscription processing for real-time dashboards and alerts
- Each role has dependencies on the bootstrap job

## Security Considerations

1. **Secret Management**:
   - Sensitive data like passwords, API keys, and tokens are stored in Kubernetes Secrets
   - References to Secrets are used instead of embedding sensitive data in the CRD

2. **TLS Configuration**:
   - Support for TLS termination at the Ingress level
   - Integration with cert-manager for certificate management

3. **Authentication**:
   - Support for various authentication methods (SSO, LDAP)
   - Configuration for user registration and email verification

4. **Privacy Settings**:
   - IP anonymization options
   - Data scrubbing capabilities
   - Field-level control over sensitive data

## Validation Rules

While not implemented in this initial version, the following validation rules should be applied:

1. **Version**:
   - Must be a valid semantic version string
   - Should be a version that exists in the Sentry Docker registry

2. **Resources**:
   - CPU and memory values must be valid Kubernetes resource quantities

3. **Persistence**:
   - Storage size must be a valid Kubernetes quantity
   - StorageClass must exist in the cluster
   - Secret references must point to existing Secrets

4. **Ingress**:
   - Host must be a valid domain name
   - Path must be a valid URL path

5. **Config**:
   - Email addresses must be valid
   - Rate limits must be positive integers
   - Retention days must be positive integers

6. **Replica**:
   - Values must be positive integers

## Future Enhancements

1. **Validation Webhooks**:
   - Implement validation webhooks to enforce the validation rules

2. **Status Conditions**:
   - Expand status conditions to provide more detailed information about the state of each component

3. **Metrics**:
   - Add metrics for monitoring the health and performance of the Sentry deployment

4. **Backup and Restore**:
   - Add support for backing up and restoring Sentry data

5. **Upgrades**:
   - Implement a more sophisticated upgrade strategy for handling version upgrades
