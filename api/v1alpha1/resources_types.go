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
	Snuba corev1.ResourceRequirements `json:"snuba,omitempty"`

	// Relay defines resources for Sentry Relay nodes.
	Relay corev1.ResourceRequirements `json:"relay,omitempty"`

	// Symbolicator defines resources for Symbolicator nodes.
	Symbolicator corev1.ResourceRequirements `json:"symbolicator,omitempty"`
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

// SnubaReplicas defines the number of replicas for different Snuba components.
type SnubaReplicas struct {
	// API is the number of Snuba API nodes.
	API int32 `json:"api,omitempty"`

	// Consumer is the number of Snuba consumer nodes.
	Consumer int32 `json:"consumer,omitempty"`

	// Replacer is the number of Snuba replacer nodes.
	Replacer int32 `json:"replacer,omitempty"`

	// SubscriptionConsumer is the number of Snuba subscription-consumer nodes.
	SubscriptionConsumer int32 `json:"subscriptionConsumer,omitempty"`
}

// SnubaResources defines resource requirements for different Snuba components.
type SnubaResources struct {
	// API defines resources for Snuba API nodes.
	API corev1.ResourceRequirements `json:"api,omitempty"`

	// Consumer defines resources for Snuba consumer nodes.
	Consumer corev1.ResourceRequirements `json:"consumer,omitempty"`

	// Replacer defines resources for Snuba replacer nodes.
	Replacer corev1.ResourceRequirements `json:"replacer,omitempty"`

	// SubscriptionConsumer defines resources for Snuba subscription-consumer nodes.
	SubscriptionConsumer corev1.ResourceRequirements `json:"subscriptionConsumer,omitempty"`
}
