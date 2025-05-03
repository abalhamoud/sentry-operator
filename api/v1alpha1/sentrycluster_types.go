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
