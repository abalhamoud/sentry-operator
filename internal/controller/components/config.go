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
	"context"
	"fmt"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	sentryv1alpha1 "github.com/abalhamoud/sentry-operator/api/v1alpha1"
)

// ConfigReconciler reconciles configuration (ConfigMaps and Secrets) for SentryCluster
type ConfigReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile handles the ConfigMap and Secret for Sentry.
func (r *ConfigReconciler) Reconcile(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling Config", "SentryCluster", sentryCluster.Name)

	// Reconcile Secret
	secretName := sentryCluster.Name + "-secret"
	secret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: sentryCluster.Namespace}, secret)
	if err != nil && apierrors.IsNotFound(err) {
		desiredSecret, err := r.defineSentrySecret(sentryCluster)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to define desired Secret")
		}
		log.Info("Creating a new Secret", "Secret.Namespace", desiredSecret.Namespace, "Secret.Name", desiredSecret.Name)
		if err := r.Create(ctx, desiredSecret); err != nil {
			log.Error(err, "Failed to create new Secret", "Secret.Namespace", desiredSecret.Namespace, "Secret.Name", desiredSecret.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Secret")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Secret")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Secret")
	} else {
		log.V(1).Info("Secret already exists", "Secret.Namespace", secret.Namespace, "Secret.Name", secret.Name)
		// TODO: Add update logic if needed, e.g., if user provides a new secretKey in spec.
	}

	// Reconcile ConfigMap
	configMapName := sentryCluster.Name + "-config"
	configMap := &corev1.ConfigMap{}
	err = r.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: sentryCluster.Namespace}, configMap)
	if err != nil && apierrors.IsNotFound(err) {
		desiredConfigMap := r.defineSentryConfigMap(sentryCluster)
		log.Info("Creating a new ConfigMap", "ConfigMap.Namespace", desiredConfigMap.Namespace, "ConfigMap.Name", desiredConfigMap.Name)
		if err := r.Create(ctx, desiredConfigMap); err != nil {
			log.Error(err, "Failed to create new ConfigMap", "ConfigMap.Namespace", desiredConfigMap.Namespace, "ConfigMap.Name", desiredConfigMap.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create ConfigMap")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get ConfigMap")
		return ctrl.Result{}, errors.Wrap(err, "failed to get ConfigMap")
	} else {
		log.V(1).Info("ConfigMap already exists", "ConfigMap.Namespace", configMap.Namespace, "ConfigMap.Name", configMap.Name)
		// TODO: Add update logic if needed, comparing data and updating.
	}

	log.Info("Config reconciled successfully", "SentryCluster", sentryCluster.Name)
	return ctrl.Result{}, nil
}

// defineSentrySecret creates the desired Secret object for Sentry.
func (r *ConfigReconciler) defineSentrySecret(sentryCluster *sentryv1alpha1.SentryCluster) (*corev1.Secret, error) {
	secretKey := sentryCluster.Spec.Config.SecretKey
	if secretKey == "" {
		log.FromContext(context.Background()).Info("Sentry secret key not provided in spec, generating a random one.")
		generatedKey, err := GenerateRandomString(64)
		if err != nil {
			return nil, errors.Wrap(err, "failed to generate random secret key")
		}
		secretKey = generatedKey
	}

	secretData := map[string]string{
		"sentry-secret-key": secretKey,
	}

	// Generate Postgres password if managing internally
	if sentryCluster.Spec.Persistence.Postgresql.Managed != nil {
		postgresPassword, err := GenerateRandomString(32)
		if err != nil {
			return nil, errors.Wrap(err, "failed to generate postgres password")
		}
		secretData["postgres-password"] = postgresPassword
	}

	// Generate Redis password if managing internally
	if sentryCluster.Spec.Persistence.Redis.Managed != nil {
		redisPassword, err := GenerateRandomString(32)
		if err != nil {
			return nil, errors.Wrap(err, "failed to generate redis password")
		}
		secretData["redis-password"] = redisPassword
	}

	// Generate ClickHouse password if managing internally
	if sentryCluster.Spec.Persistence.ClickHouse != nil && sentryCluster.Spec.Persistence.ClickHouse.Managed != nil {
		clickhousePassword, err := GenerateRandomString(32)
		if err != nil {
			return nil, errors.Wrap(err, "failed to generate clickhouse password")
		}
		secretData["clickhouse-password"] = clickhousePassword
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-secret",
			Namespace: sentryCluster.Namespace,
			Labels:    GetStandardLabels(sentryCluster),
		},
		Type:       corev1.SecretTypeOpaque,
		StringData: secretData,
	}
	if err := controllerutil.SetControllerReference(sentryCluster, secret, r.Scheme); err != nil {
		return nil, errors.Wrap(err, "failed to set controller reference on Secret")
	}
	return secret, nil
}

// defineSentryConfigMap creates the desired ConfigMap object for Sentry.
func (r *ConfigReconciler) defineSentryConfigMap(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.ConfigMap {
	// Build the URL prefix
	protocol := "http"
	if sentryCluster.Spec.Ingress.TLS != nil && sentryCluster.Spec.Ingress.TLS.Enabled {
		protocol = "https"
	}
	path := ""
	if sentryCluster.Spec.Ingress.Path != "" {
		path = sentryCluster.Spec.Ingress.Path
	}
	urlPrefix := fmt.Sprintf("%s://%s%s", protocol, sentryCluster.Spec.Ingress.Host, path)

	// Build the config.yml content
	configYmlContent := fmt.Sprintf(`
system.url-prefix: '%s'
`, urlPrefix)

	// Add email configuration if provided
	if sentryCluster.Spec.Config.Email != nil {
		emailBackend := "django.core.mail.backends.console.EmailBackend" // Default to console
		if sentryCluster.Spec.Config.Email.Backend != "" {
			emailBackend = sentryCluster.Spec.Config.Email.Backend
		}
		configYmlContent += fmt.Sprintf("mail.backend: '%s'\n", emailBackend)
		
		if sentryCluster.Spec.Config.Email.From != "" {
			configYmlContent += fmt.Sprintf("mail.from: '%s'\n", sentryCluster.Spec.Config.Email.From)
		}
	}

	// Add feature flags if provided
	if sentryCluster.Spec.Config.Features != nil && len(sentryCluster.Spec.Config.Features) > 0 {
		configYmlContent += "features:\n"
		for feature, enabled := range sentryCluster.Spec.Config.Features {
			configYmlContent += fmt.Sprintf("  %s: %t\n", feature, enabled)
		}
	}

	// Add privacy settings if provided
	if sentryCluster.Spec.Config.Privacy != nil {
		if sentryCluster.Spec.Config.Privacy.IPAnonymization {
			configYmlContent += "ip-anonymization: true\n"
		}
		
		if len(sentryCluster.Spec.Config.Privacy.ExcludedIPs) > 0 {
			configYmlContent += "excluded-ips:\n"
			for _, ip := range sentryCluster.Spec.Config.Privacy.ExcludedIPs {
				configYmlContent += fmt.Sprintf("  - '%s'\n", ip)
			}
		}
		
		if sentryCluster.Spec.Config.Privacy.ScrubData {
			configYmlContent += "scrub-data: true\n"
		}
		
		if sentryCluster.Spec.Config.Privacy.ScrubDefaults {
			configYmlContent += "scrub-defaults: true\n"
		}
		
		if len(sentryCluster.Spec.Config.Privacy.ScrubFields) > 0 {
			configYmlContent += "scrub-fields:\n"
			for _, field := range sentryCluster.Spec.Config.Privacy.ScrubFields {
				configYmlContent += fmt.Sprintf("  - '%s'\n", field)
			}
		}
	}

	// Add performance settings if provided
	if sentryCluster.Spec.Config.Performance != nil && sentryCluster.Spec.Config.Performance.Enabled {
		log.info("Performance is configured but not used... ")
		// if sentryCluster.Spec.Config.Performance.SampleRate > 0 {
		// 	configYmlContent += fmt.Sprintf("performance.sample-rate: %f\n", sentryCluster.Spec.Config.Performance.SampleRate)
		// }
		
		// if sentryCluster.Spec.Config.Performance.TracesSampleRate > 0 {
		// 	configYmlContent += fmt.Sprintf("performance.traces-sample-rate: %f\n", sentryCluster.Spec.Config.Performance.TracesSampleRate)
		// }
	}

	// Basic sentry.conf.py content
	sentryConfPyContent := `
# Basic Sentry configuration
# Values like DB connection and secret key are primarily set via ENV VARS in the deployment
`

	// Add rate limiting settings if provided
	if sentryCluster.Spec.Config.RateLimiting != nil {
		if sentryCluster.Spec.Config.RateLimiting.EventsPerMinute > 0 {
			sentryConfPyContent += fmt.Sprintf("SENTRY_RATE_LIMIT = %d\n", sentryCluster.Spec.Config.RateLimiting.EventsPerMinute)
		}
		
		if sentryCluster.Spec.Config.RateLimiting.ErrorsPerMinute > 0 {
			sentryConfPyContent += fmt.Sprintf("SENTRY_ERROR_RATE_LIMIT = %d\n", sentryCluster.Spec.Config.RateLimiting.ErrorsPerMinute)
		}
		
		if sentryCluster.Spec.Config.RateLimiting.TransactionsPerMinute > 0 {
			sentryConfPyContent += fmt.Sprintf("SENTRY_TRANSACTION_RATE_LIMIT = %d\n", sentryCluster.Spec.Config.RateLimiting.TransactionsPerMinute)
		}
	}

	// Add data retention settings if provided
	if sentryCluster.Spec.Config.DataRetention != nil {
		if sentryCluster.Spec.Config.DataRetention.EventRetentionDays > 0 {
			sentryConfPyContent += fmt.Sprintf("SENTRY_EVENT_RETENTION_DAYS = %d\n", sentryCluster.Spec.Config.DataRetention.EventRetentionDays)
		}
		
		if sentryCluster.Spec.Config.DataRetention.IssueRetentionDays > 0 {
			sentryConfPyContent += fmt.Sprintf("SENTRY_ISSUE_RETENTION_DAYS = %d\n", sentryCluster.Spec.Config.DataRetention.IssueRetentionDays)
		}
	}

	// Add auth settings if provided
	if sentryCluster.Spec.Config.Auth != nil {
		sentryConfPyContent += fmt.Sprintf("SENTRY_ALLOW_REGISTRATION = %t\n", sentryCluster.Spec.Config.Auth.AllowRegistration)
		sentryConfPyContent += fmt.Sprintf("SENTRY_REQUIRE_EMAIL_VERIFICATION = %t\n", sentryCluster.Spec.Config.Auth.RequireEmailVerification)
	}

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-config",
			Namespace: sentryCluster.Namespace,
			Labels:    GetStandardLabels(sentryCluster),
		},
		Data: map[string]string{
			"config.yml":     configYmlContent,
			"sentry.conf.py": sentryConfPyContent,
		},
	}
	if err := controllerutil.SetControllerReference(sentryCluster, configMap, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on ConfigMap")
	}
	return configMap
}
