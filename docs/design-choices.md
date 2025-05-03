# SentryCluster CRD Design Choices

This document explains the design choices made for the SentryCluster Custom Resource Definition (CRD), addressing the important considerations for each field in the specification.

## Version Field

### Why included
The Version field is crucial for specifying which Sentry version to deploy. This allows users to control which version of Sentry they want to run, enabling planned upgrades and rollbacks.

### Data type
The Version field is a string, as Sentry versions follow semantic versioning (e.g., "24.5.0").

### Possible values
Any valid Sentry version string (e.g., "24.5.0", "24.4.0").

### Effect on deployment
The Version field determines the container image tag used for all Sentry components (web, worker, etc.). It ensures that all components are running the same version of Sentry.

### Security considerations
No special security considerations for this field.

### Validation rules
Should be a valid semantic version string. The operator should validate that the specified version exists as a container image.

## Resources Field

### Why included
The Resources field allows users to configure CPU and memory requests and limits for each Sentry component. This is essential for resource management in Kubernetes, ensuring that each component has the resources it needs to function properly.

### Data type
The Resources field is an object containing nested objects for each component, each using Kubernetes' standard ResourceRequirements type. This provides a familiar and consistent way to specify resource requirements.

### Possible values
Each component can have requests and limits for CPU and memory, following Kubernetes' resource quantity format (e.g., "500m" CPU, "1Gi" memory).

### Effect on deployment
The Resources field directly affects the resource requests and limits set on the Kubernetes Pods for each component. This influences scheduling decisions, quality of service, and resource allocation.

### Security considerations
No direct security implications, but properly configured resources help prevent resource exhaustion attacks.

### Validation rules
- CPU and memory values should follow Kubernetes' resource quantity format.
- Requests should not exceed limits.
- Minimum values may be enforced for certain components to ensure stability.

## Persistence Field

### Why included
The Persistence field handles storage requirements for stateful components like PostgreSQL, Redis, Kafka, and ClickHouse. This is essential for data durability and performance.

### Data type
The Persistence field is an object containing nested objects for each stateful component, with options for both managed (PVC-based) and external instances.

### Possible values
- For managed instances: StorageClass name and size.
- For external instances: Secret name containing connection details.

### Effect on deployment
- For managed instances: Creates PersistentVolumeClaims with the specified size and StorageClass.
- For external instances: Configures components to connect to external services using the provided credentials.

### Security considerations
- Connection details for external services are stored in Kubernetes Secrets.
- Access to these Secrets should be restricted.

### Validation rules
- Size should be a valid Kubernetes quantity.
- StorageClass should exist in the cluster.
- For external instances, the referenced Secret should exist and contain the required keys.

## Ingress Field

### Why included
The Ingress field provides options for exposing Sentry to external traffic, allowing users to access the Sentry web interface.

### Data type
The Ingress field is an object with properties for configuring Kubernetes Ingress resources.

### Possible values
- enabled: true/false
- host: domain name
- path: URL path
- TLS configuration: enabled, secretName, etc.
- Annotations for different Ingress controllers

### Effect on deployment
Creates a Kubernetes Ingress resource with the specified configuration, making Sentry accessible from outside the cluster.

### Security considerations
- TLS should be enabled for production deployments.
- The TLS certificate should be valid and trusted.
- Access control should be implemented at the application level.

### Validation rules
- Host should be a valid domain name.
- If TLS is enabled, a valid secretName should be provided or cert-manager configuration should be valid.

## Config Field

### Why included
The Config field manages Sentry's application-specific configuration, allowing users to customize Sentry's behavior according to their needs.

### Data type
The Config field is an object with nested objects for different aspects of Sentry's configuration.

### Possible values
Various settings for email, authentication, rate limiting, data retention, integrations, privacy, and performance.

### Effect on deployment
Generates ConfigMaps and Secrets that configure Sentry's behavior through environment variables and configuration files.

### Security considerations
- Sensitive information like secret keys and passwords are stored in Kubernetes Secrets.
- The Sentry secret key is particularly important for cryptographic signing.

### Validation rules
- Email configuration should include valid SMTP settings.
- Rate limiting values should be positive integers.
- Sample rates should be between 0.0 and 1.0.

## Replica Field

### Why included
The Replica field defines the number of replicas for scalable components like web, worker, and relay. This allows users to scale Sentry horizontally based on their needs.

### Data type
The Replica field is an object with integer fields for each scalable component.

### Possible values
Positive integers representing the number of replicas for each component.

### Effect on deployment
Sets the replicas field on Deployments and StatefulSets for the respective components.

### Security considerations
No direct security implications.

### Validation rules
- Values should be positive integers.
- Maximum values may be enforced to prevent resource exhaustion.

## Overall Design Philosophy

The SentryCluster CRD is designed with the following principles in mind:

1. **Flexibility**: Users can choose between managed and external services for stateful components.
2. **Familiarity**: The CRD uses Kubernetes native types where possible (e.g., ResourceRequirements).
3. **Completeness**: All aspects of Sentry configuration are covered.
4. **Security**: Sensitive information is stored in Secrets.
5. **Validation**: The CRD includes validation rules to prevent misconfiguration.
6. **Extensibility**: The CRD can be extended to support new features in future versions of Sentry.

This design allows users to deploy and manage Sentry in a way that best fits their needs, whether they're running a small development instance or a large production deployment.
