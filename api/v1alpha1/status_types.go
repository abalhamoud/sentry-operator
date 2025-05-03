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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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

// SetCondition sets a condition in the SentryCluster status
func (s *SentryClusterStatus) SetCondition(condition metav1.Condition) {
	// Find the condition
	for i, c := range s.Conditions {
		if c.Type == condition.Type {
			// Update the condition
			s.Conditions[i] = condition
			return
		}
	}
	// Condition not found, add it
	s.Conditions = append(s.Conditions, condition)
}

// GetCondition gets a condition from the SentryCluster status
func (s *SentryClusterStatus) GetCondition(conditionType string) *metav1.Condition {
	for _, c := range s.Conditions {
		if c.Type == conditionType {
			return &c
		}
	}
	return nil
}
