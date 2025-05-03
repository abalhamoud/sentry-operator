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

package components

import (
	"crypto/rand"
	"encoding/base64"

	sentryv1alpha1 "github.com/abalhamoud/sentry-operator/api/v1alpha1"
)

// Constants for ports used by various components
const (
	PostgresPort       = 5432
	PostgresUser       = "sentry"
	PostgresDB         = "sentry"
	RedisPort          = 6379
	KafkaPort          = 9092
	ClickHousePort     = 9000
	ClickHouseHTTPPort = 8123
	SnubaAPIPort       = 1218
	RelayPort          = 3000
	SymbolicatorPort   = 3021
)

// GetStandardLabels returns the standard labels for all Sentry components
func GetStandardLabels(sentryCluster *sentryv1alpha1.SentryCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "sentry",
		"app.kubernetes.io/instance":   sentryCluster.Name,
		"app.kubernetes.io/managed-by": "sentry-operator",
		"app.kubernetes.io/part-of":    "sentry",
	}
}

// GetComponentLabels returns the labels for a specific component
func GetComponentLabels(sentryCluster *sentryv1alpha1.SentryCluster, component string) map[string]string {
	labels := GetStandardLabels(sentryCluster)
	labels["app.kubernetes.io/component"] = component
	return labels
}

// GenerateRandomString generates a random string of the specified length
func GenerateRandomString(length int) (string, error) {
	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b)[:length], nil
}
