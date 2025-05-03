/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// SentryClusterSpec defines the desired state of SentryCluster
type SentryClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Version is the Sentry version to deploy.
	Version string `json:"version"`

	// Resources defines the CPU/memory requests and limits for Sentry components.
	Resources Resources `json:"resources,omitempty"`

	// Persistence defines the storage requirements for PostgreSQL and Redis.
	Persistence Persistence `json:"persistence,omitempty"`

	// Ingress defines the options for exposing Sentry to external traffic.
	Ingress Ingress `json:"ingress,omitempty"`

	// Config defines Sentry-specific settings.
	Config Config `json:"config,omitempty"`

	// Replica defines the number of web and worker replicas.
	Replica Replica `json:"replica,omitempty"`
}

// Resources defines the CPU/memory requests and limits for Sentry components.
type Resources struct {
	// Web defines resources for Sentry web UI nodes.
	Web corev1.ResourceRequirements `json:"web,omitempty"`
	
	// Worker defines resources for Sentry background worker nodes.
	Worker corev1.ResourceRequirements `json:"worker,omitempty"`
	
	// Redis defines resources for Redis nodes.
	Redis corev1.ResourceRequirements `json:"redis,omitempty"`
	
	// Postgresql defines resources for PostgreSQL nodes.
	Postgresql corev1.ResourceRequirements `json:"postgresql,omitempty"`
	
	// Kafka defines resources for Kafka brokers.
	Kafka corev1.ResourceRequirements `json:"kafka,omitempty"`
	
	// ClickHouse defines resources for ClickHouse nodes.
	ClickHouse corev1.ResourceRequirements `json:"clickhouse,omitempty"`
	
	// Snuba defines resources for Snuba components.
	Snuba *SnubaResources `json:"snuba,omitempty"`
	
	// Relay defines resources for Sentry Relay nodes.
	Relay corev1.ResourceRequirements `json:"relay,omitempty"`
	
	// Symbolicator defines resources for Symbolicator nodes.
	Symbolicator corev1.ResourceRequirements `json:"symbolicator,omitempty"`
}

// Persistence defines the storage requirements for Sentry's data stores.
type Persistence struct {
	// Postgresql is the database used for Sentry's primary data storage.
	Postgresql PersistenceConfig `json:"postgresql,omitempty"`
	
	// Redis is used for caching, rate limiting, and as a message broker.
	Redis PersistenceConfig `json:"redis,omitempty"`
	
	// Kafka is used for event streaming in Sentry's processing pipeline.
	Kafka *KafkaConfig `json:"kafka,omitempty"`
	
	// ClickHouse is used for analytics and event storage.
	ClickHouse *ClickHouseConfig `json:"clickhouse,omitempty"`
	
	// Snuba is Sentry's event storage service that sits on top of ClickHouse.
	Snuba *SnubaConfig `json:"snuba,omitempty"`
}

// KafkaConfig defines configuration for Kafka.
type KafkaConfig struct {
	// Managed defines the configuration for a managed Kafka instance.
	// If set, the operator will create and manage the Kafka cluster.
	Managed *KafkaManagedConfig `json:"managed,omitempty"`
	
	// External defines the configuration for connecting to an external Kafka cluster.
	// If set, the operator will use the connection details from the specified Secret.
	External *KafkaExternalConfig `json:"external,omitempty"`
}

// KafkaManagedConfig defines configuration for a managed Kafka instance.
type KafkaManagedConfig struct {
	// Replicas is the number of Kafka brokers to deploy.
	Replicas int32 `json:"replicas,omitempty"`
	
	// Resources defines the CPU/memory requests and limits for Kafka brokers.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	
	// Storage defines the storage configuration for Kafka brokers.
	Storage *PersistentVolumeClaim `json:"storage,omitempty"`
	
	// Version is the Kafka version to deploy.
	Version string `json:"version,omitempty"`
	
	// Config defines Kafka broker configuration.
	Config map[string]string `json:"config,omitempty"`
}

// KafkaExternalConfig defines configuration for connecting to an external Kafka cluster.
type KafkaExternalConfig struct {
	// SecretName is the name of the Secret containing Kafka connection details.
	// Expected keys: "bootstrap.servers", "sasl.username" (optional), "sasl.password" (optional)
	SecretName string `json:"secretName"`
	
	// Topics is a list of topics that Sentry will use.
	Topics []string `json:"topics,omitempty"`
}

// ClickHouseConfig defines configuration for ClickHouse.
type ClickHouseConfig struct {
	// Managed defines the configuration for a managed ClickHouse instance.
	// If set, the operator will create and manage the ClickHouse cluster.
	Managed *ClickHouseManagedConfig `json:"managed,omitempty"`
	
	// External defines the configuration for connecting to an external ClickHouse cluster.
	// If set, the operator will use the connection details from the specified Secret.
	External *ClickHouseExternalConfig `json:"external,omitempty"`
}

// ClickHouseManagedConfig defines configuration for a managed ClickHouse instance.
type ClickHouseManagedConfig struct {
	// Replicas is the number of ClickHouse nodes to deploy.
	Replicas int32 `json:"replicas,omitempty"`
	
	// Resources defines the CPU/memory requests and limits for ClickHouse nodes.
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	
	// Storage defines the storage configuration for ClickHouse nodes.
	Storage *PersistentVolumeClaim `json:"storage,omitempty"`
	
	// Version is the ClickHouse version to deploy.
	Version string `json:"version,omitempty"`
	
	// Config defines ClickHouse configuration.
	Config map[string]string `json:"config,omitempty"`
}

// ClickHouseExternalConfig defines configuration for connecting to an external ClickHouse cluster.
type ClickHouseExternalConfig struct {
	// SecretName is the name of the Secret containing ClickHouse connection details.
	// Expected keys: "host", "port", "user", "password", "database"
	SecretName string `json:"secretName"`
}

// SnubaConfig defines configuration for Snuba.
type SnubaConfig struct {
	// Replicas is the number of Snuba consumer and API nodes to deploy.
	Replicas *SnubaReplicas `json:"replicas,omitempty"`
	
	// Resources defines the CPU/memory requests and limits for Snuba components.
	Resources *SnubaResources `json:"resources,omitempty"`
	
	// Config defines Snuba-specific configuration.
	Config map[string]string `json:"config,omitempty"`
}

// SnubaReplicas defines the number of replicas for different Snuba components.
type SnubaReplicas struct {
	// API is the number of Snuba API nodes.
	API int32 `json:"api,omitempty"`
	
	// Consumer is the number of Snuba consumer nodes.
	Consumer int32 `json:"consumer,omitempty"`
	
	// Replacer is the number of Snuba replacer nodes.
	Replacer int32 `json:"replacer,omitempty"`
}

// SnubaResources defines resource requirements for different Snuba components.
type SnubaResources struct {
	// API defines resources for Snuba API nodes.
	API corev1.ResourceRequirements `json:"api,omitempty"`
	
	// Consumer defines resources for Snuba consumer nodes.
	Consumer corev1.ResourceRequirements `json:"consumer,omitempty"`
	
	// Replacer defines resources for Snuba replacer nodes.
	Replacer corev1.ResourceRequirements `json:"replacer,omitempty"`
}

// PersistenceConfig defines configuration for a persistent service (Postgres/Redis).
// It allows specifying either managed persistence options (PVC) or details for an external instance.
type PersistenceConfig struct {
	// Managed defines the configuration for a managed instance using a PersistentVolumeClaim.
	// If set, the operator will create and manage the PVC, Service, and StatefulSet.
	Managed *PersistentVolumeClaim `json:"managed,omitempty"`

	// External defines the configuration for connecting to an externally managed instance.
	// If set, the operator will use the connection details from the specified Secret.
	External *ExternalInstance `json:"external,omitempty"`
}

// PersistentVolumeClaim defines the StorageClass and size of the persistent volumes for managed instances.
type PersistentVolumeClaim struct {
	// StorageClass is the name of the StorageClass to use for the PVC.
	// +optional
	StorageClass string `json:"storageClass,omitempty"`
	// Size is the requested size of the persistent volume (e.g., "10Gi").
	Size string `json:"size"` // Made mandatory for managed persistence
}

// ExternalInstance defines the connection details for an externally managed service.
type ExternalInstance struct {
	// SecretName is the name of the Secret in the same namespace containing the connection details.
	// For Redis, expected keys: "host", "port", "password" (optional).
	// For Postgres, expected keys: "host", "port", "user", "password", "dbname".
	SecretName string `json:"secretName"`
}

// Ingress defines the options for exposing Sentry to external traffic.
type Ingress struct {
	// Enabled indicates whether to create an Ingress resource.
	Enabled bool `json:"enabled"`
	
	// Type specifies the Ingress controller type (e.g., "nginx", "traefik", "openshift-route").
	Type string `json:"type,omitempty"`
	
	// Host is the hostname for Sentry (e.g., "sentry.example.com").
	Host string `json:"host"`
	
	// Path is the URL path prefix for Sentry (e.g., "/sentry").
	Path string `json:"path,omitempty"`
	
	// TLS defines TLS configuration for the Ingress.
	TLS *IngressTLS `json:"tls,omitempty"`
	
	// Annotations are additional annotations to add to the Ingress resource.
	Annotations map[string]string `json:"annotations,omitempty"`
	
	// ClassName is the IngressClass name for Kubernetes 1.18+ clusters.
	ClassName string `json:"className,omitempty"`

	// PathType is the PathType for Sentry (e.g., "ImplementationSpecific").
	PathType string `json:"pathType"`
}

// IngressTLS defines TLS configuration for the Ingress.
type IngressTLS struct {
	// Enabled indicates whether TLS is enabled.
	Enabled bool `json:"enabled"`
	
	// SecretName is the name of the Secret containing the TLS certificate and key.
	// If not provided, a Secret will be created with a self-signed certificate.
	SecretName string `json:"secretName,omitempty"`
	
	// CertManager indicates whether to use cert-manager for certificate management.
	CertManager bool `json:"certManager,omitempty"`
	
	// Issuer is the cert-manager Issuer to use when CertManager is enabled.
	Issuer string `json:"issuer,omitempty"`
	
	// IssuerKind is the kind of cert-manager Issuer (e.g., "Issuer", "ClusterIssuer").
	IssuerKind string `json:"issuerKind,omitempty"`
}

// Config defines Sentry-specific settings.
type Config struct {
	// SecretKey is used for cryptographic signing in Sentry.
	// If not provided, a random key will be generated.
	SecretKey string `json:"secretKey,omitempty"`

	// Email defines the email configuration for Sentry.
	Email *EmailConfig `json:"email,omitempty"`

	// Auth defines authentication settings for Sentry.
	Auth *AuthConfig `json:"auth,omitempty"`

	// RateLimiting defines rate limiting settings for Sentry.
	RateLimiting *RateLimitingConfig `json:"rateLimiting,omitempty"`

	// DataRetention defines how long Sentry keeps event data.
	DataRetention *DataRetentionConfig `json:"dataRetention,omitempty"`

	// Features defines feature flags for Sentry.
	Features map[string]bool `json:"features,omitempty"`

	// Integrations defines settings for external service integrations.
	Integrations *IntegrationsConfig `json:"integrations,omitempty"`

	// Privacy defines privacy-related settings.
	Privacy *PrivacyConfig `json:"privacy,omitempty"`

	// Performance defines settings for performance monitoring.
	Performance *PerformanceConfig `json:"performance,omitempty"`
}

// EmailConfig defines email settings for Sentry.
type EmailConfig struct {
	// Backend is the email backend to use (e.g., "smtp", "console").
	Backend string `json:"backend,omitempty"`

	// From is the email address to send from.
	From string `json:"from,omitempty"`

	// SMTP defines SMTP server settings when using the SMTP backend.
	SMTP *SMTPConfig `json:"smtp,omitempty"`
}

// SMTPConfig defines SMTP server settings.
type SMTPConfig struct {
	// Host is the SMTP server hostname.
	Host string `json:"host"`

	// Port is the SMTP server port.
	Port int32 `json:"port"`

	// Username for SMTP authentication.
	Username string `json:"username,omitempty"`

	// SecretName is the name of the Secret containing the SMTP password.
	SecretName string `json:"secretName,omitempty"`

	// UseTLS indicates whether to use TLS for SMTP connections.
	UseTLS bool `json:"useTLS,omitempty"`
}

// AuthConfig defines authentication settings for Sentry.
type AuthConfig struct {
	// AllowRegistration determines if new users can register.
	AllowRegistration bool `json:"allowRegistration,omitempty"`

	// RequireEmailVerification requires email verification before login.
	RequireEmailVerification bool `json:"requireEmailVerification,omitempty"`

	// SSO defines Single Sign-On configuration.
	SSO *SSOConfig `json:"sso,omitempty"`

	// LDAP defines LDAP authentication configuration.
	LDAP *LDAPConfig `json:"ldap,omitempty"`
}

// SSOConfig defines Single Sign-On configuration.
type SSOConfig struct {
	// Enabled indicates whether SSO is enabled.
	Enabled bool `json:"enabled"`

	// Provider is the SSO provider (e.g., "google", "github", "okta").
	Provider string `json:"provider,omitempty"`

	// SecretName is the name of the Secret containing SSO credentials.
	SecretName string `json:"secretName,omitempty"`
}

// LDAPConfig defines LDAP authentication configuration.
type LDAPConfig struct {
	// Enabled indicates whether LDAP authentication is enabled.
	Enabled bool `json:"enabled"`

	// ServerURI is the LDAP server URI.
	ServerURI string `json:"serverUri,omitempty"`

	// BindDN is the LDAP bind DN.
	BindDN string `json:"bindDn,omitempty"`

	// SecretName is the name of the Secret containing the LDAP bind password.
	SecretName string `json:"secretName,omitempty"`

	// UserSearchBase is the LDAP search base for users.
	UserSearchBase string `json:"userSearchBase,omitempty"`

	// UserSearchFilter is the LDAP search filter for users.
	UserSearchFilter string `json:"userSearchFilter,omitempty"`
}

// RateLimitingConfig defines rate limiting settings for Sentry.
type RateLimitingConfig struct {
	// EventsPerMinute limits the number of events accepted per minute.
	EventsPerMinute int32 `json:"eventsPerMinute,omitempty"`

	// ErrorsPerMinute limits the number of error events per minute.
	ErrorsPerMinute int32 `json:"errorsPerMinute,omitempty"`

	// TransactionsPerMinute limits the number of transaction events per minute.
	TransactionsPerMinute int32 `json:"transactionsPerMinute,omitempty"`
}

// DataRetentionConfig defines data retention settings.
type DataRetentionConfig struct {
	// EventRetentionDays defines how many days to keep event data.
	EventRetentionDays int32 `json:"eventRetentionDays,omitempty"`

	// IssueRetentionDays defines how many days to keep issue data.
	IssueRetentionDays int32 `json:"issueRetentionDays,omitempty"`
}

// IntegrationsConfig defines settings for external service integrations.
type IntegrationsConfig struct {
	// GitHub defines GitHub integration settings.
	GitHub *GitHubConfig `json:"github,omitempty"`

	// Slack defines Slack integration settings.
	Slack *SlackConfig `json:"slack,omitempty"`

	// Jira defines Jira integration settings.
	Jira *JiraConfig `json:"jira,omitempty"`
}

// GitHubConfig defines GitHub integration settings.
type GitHubConfig struct {
	// Enabled indicates whether GitHub integration is enabled.
	Enabled bool `json:"enabled"`

	// SecretName is the name of the Secret containing GitHub credentials.
	SecretName string `json:"secretName,omitempty"`
}

// SlackConfig defines Slack integration settings.
type SlackConfig struct {
	// Enabled indicates whether Slack integration is enabled.
	Enabled bool `json:"enabled"`

	// SecretName is the name of the Secret containing Slack credentials.
	SecretName string `json:"secretName,omitempty"`
}

// JiraConfig defines Jira integration settings.
type JiraConfig struct {
	// Enabled indicates whether Jira integration is enabled.
	Enabled bool `json:"enabled"`

	// SecretName is the name of the Secret containing Jira credentials.
	SecretName string `json:"secretName,omitempty"`
}

// PrivacyConfig defines privacy-related settings.
type PrivacyConfig struct {
	// IPAnonymization determines whether to anonymize IP addresses.
	IPAnonymization bool `json:"ipAnonymization,omitempty"`

	// ExcludedIPs is a list of IP addresses to exclude from event data.
	ExcludedIPs []string `json:"excludedIps,omitempty"`

	// ScrubData determines whether to scrub potentially sensitive data.
	ScrubData bool `json:"scrubData,omitempty"`

	// ScrubDefaults determines whether to scrub default fields.
	ScrubDefaults bool `json:"scrubDefaults,omitempty"`

	// ScrubFields is a list of additional fields to scrub.
	ScrubFields []string `json:"scrubFields,omitempty"`
}

// PerformanceConfig defines settings for performance monitoring.
type PerformanceConfig struct {
	// Enabled indicates whether performance monitoring is enabled.
	Enabled bool `json:"enabled"`

	// SampleRate defines the sampling rate for performance data (0.0-1.0).
	SampleRate string `json:"sampleRate,omitempty"`

	// TracesSampleRate defines the sampling rate for traces (0.0-1.0).
	TracesSampleRate string `json:"tracesSampleRate,omitempty"`
}

// Replica defines the number of replicas for each Sentry component.
type Replica struct {
	// Web is the number of Sentry web UI nodes.
	Web int32 `json:"web,omitempty"`
	
	// Worker is the number of Sentry background worker nodes.
	Worker int32 `json:"worker,omitempty"`
	
	// Kafka is the number of Kafka broker nodes.
	Kafka int32 `json:"kafka,omitempty"`
	
	// ClickHouse is the number of ClickHouse nodes.
	ClickHouse int32 `json:"clickhouse,omitempty"`
	
	// Snuba defines the number of replicas for different Snuba components.
	Snuba *SnubaReplicas `json:"snuba,omitempty"`
	
	// Relay is the number of Sentry Relay nodes.
	Relay int32 `json:"relay,omitempty"`
	
	// Symbolicator is the number of Symbolicator nodes.
	Symbolicator int32 `json:"symbolicator,omitempty"`
}

// SentryClusterStatus defines the observed state of SentryCluster
type SentryClusterStatus struct {
	// Phase is a simple, high-level summary of where the SentryCluster is in its lifecycle.
	// The phase is not intended to be a comprehensive rollup of observations of the SentryCluster state,
	// nor is it intended to be a comprehensive state machine.
	// Possible values: Pending, Provisioning, Running, Upgrading, Degraded, Failed
	Phase string `json:"phase,omitempty"`

	// Nodes are the names of the Sentry pods.
	Nodes []string `json:"nodes,omitempty"`

	// Version is the current Sentry version.
	Version string `json:"version,omitempty"`

	// URL is the external URL where Sentry can be accessed.
	URL string `json:"url,omitempty"`

	// ComponentStatus provides status information for each Sentry component.
	ComponentStatus ComponentStatus `json:"componentStatus,omitempty"`

	// Conditions represent the state of the Sentry deployment.
	// +patchMergeKey=type
	// +patchStrategy=merge
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// ObservedGeneration is the most recent generation observed by the controller.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastUpdated is the timestamp when the status was last updated.
	LastUpdated metav1.Time `json:"lastUpdated,omitempty"`
}

// ComponentStatus provides status information for each Sentry component.
type ComponentStatus struct {
	// Web provides status information for the Sentry web component.
	Web ComponentInfo `json:"web,omitempty"`

	// Worker provides status information for the Sentry worker component.
	Worker ComponentInfo `json:"worker,omitempty"`

	// Postgresql provides status information for the PostgreSQL component.
	Postgresql ComponentInfo `json:"postgresql,omitempty"`

	// Redis provides status information for the Redis component.
	Redis ComponentInfo `json:"redis,omitempty"`

	// Kafka provides status information for the Kafka component.
	Kafka ComponentInfo `json:"kafka,omitempty"`

	// ClickHouse provides status information for the ClickHouse component.
	ClickHouse ComponentInfo `json:"clickhouse,omitempty"`

	// Snuba provides status information for the Snuba component.
	Snuba ComponentInfo `json:"snuba,omitempty"`

	// Relay provides status information for the Relay component.
	Relay ComponentInfo `json:"relay,omitempty"`

	// Symbolicator provides status information for the Symbolicator component.
	Symbolicator ComponentInfo `json:"symbolicator,omitempty"`
}

// ComponentInfo provides status information for a specific component.
type ComponentInfo struct {
	// Ready indicates whether the component is ready.
	Ready bool `json:"ready"`

	// Replicas is the number of desired replicas.
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is the number of ready replicas.
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// AvailableReplicas is the number of available replicas.
	AvailableReplicas int32 `json:"availableReplicas,omitempty"`

	// UnavailableReplicas is the number of unavailable replicas.
	UnavailableReplicas int32 `json:"unavailableReplicas,omitempty"`

	// UpdatedReplicas is the number of updated replicas.
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty"`

	// Message provides additional information about the component status.
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// SentryCluster is the Schema for the sentryclusters API
type SentryCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SentryClusterSpec   `json:"spec,omitempty"`
	Status SentryClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SentryClusterList contains a list of SentryCluster
type SentryClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SentryCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SentryCluster{}, &SentryClusterList{})
}
