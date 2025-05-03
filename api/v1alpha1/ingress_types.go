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
