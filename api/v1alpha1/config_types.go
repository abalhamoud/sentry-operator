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
