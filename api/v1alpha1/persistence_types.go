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
)

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
