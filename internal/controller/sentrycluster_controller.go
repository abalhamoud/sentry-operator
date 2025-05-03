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

package controller

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	sentryv1alpha1 "github.com/abalhamoud/sentry-operator/api/v1alpha1"
)

// SentryClusterReconciler reconciles a SentryCluster object
type SentryClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const sentryFinalizer = "sentry.sentry.io/finalizer"
const postgresPort = 5432
const postgresUser = "sentry"
const postgresDB = "sentry"
const redisPort = 6379
const kafkaPort = 9092
const clickhousePort = 9000
const clickhouseHTTPPort = 8123
const snubaAPIPort = 1218
const relayPort = 3000
const symbolicatorPort = 3021

// +kubebuilder:rbac:groups=sentry.sentry.io,resources=sentryclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sentry.sentry.io,resources=sentryclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sentry.sentry.io,resources=sentryclusters/finalizers,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services;configmaps;secrets;persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *SentryClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. Fetch the SentryCluster resource
	var sentryCluster sentryv1alpha1.SentryCluster
	if err := r.Get(ctx, req.NamespacedName, &sentryCluster); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("SentryCluster resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get SentryCluster")
		return ctrl.Result{}, err
	}

	// 2. Handle finalizers
	isSentryClusterMarkedToBeDeleted := sentryCluster.GetDeletionTimestamp() != nil
	if isSentryClusterMarkedToBeDeleted {
		if controllerutil.ContainsFinalizer(&sentryCluster, sentryFinalizer) {
			// Run finalization logic for sentryFinalizer. If the
			// finalization logic fails, don't remove the finalizer so
			// that we can retry during the next reconciliation.
			if err := r.finalizeSentryCluster(ctx, &sentryCluster); err != nil {
				log.Error(err, "Failed to finalize SentryCluster")
				return ctrl.Result{}, err
			}

			// Remove sentryFinalizer. Once all finalizers have been
			// removed, the object will be deleted.
			controllerutil.RemoveFinalizer(&sentryCluster, sentryFinalizer)
			err := r.Update(ctx, &sentryCluster)
			if err != nil {
				log.Error(err, "Failed to remove finalizer from SentryCluster")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer for this CR if it doesn't exist
	if !controllerutil.ContainsFinalizer(&sentryCluster, sentryFinalizer) {
		controllerutil.AddFinalizer(&sentryCluster, sentryFinalizer)
		err := r.Update(ctx, &sentryCluster)
		if err != nil {
			log.Error(err, "Failed to add finalizer to SentryCluster")
			return ctrl.Result{}, err
		}
		// Requeue after adding finalizer to ensure it's processed
		return ctrl.Result{Requeue: true}, nil
	}

	// 3. Reconcile the Sentry deployment - This is the main logic
	result, err := r.reconcileSentry(ctx, &sentryCluster)
	if err != nil {
		// Set error status condition
		statusErr := r.setStatusError(ctx, &sentryCluster, errors.Wrap(err, "failed to reconcile Sentry"))
		if statusErr != nil {
			// Log the status update error but return the original reconcile error
			log.Error(statusErr, "Failed to update status with error")
		}
		// Return the original error to trigger requeue
		return result, err
	}

	// Set ready status condition if no error occurred
	statusErr := r.setStatusReady(ctx, &sentryCluster)
	if statusErr != nil {
		log.Error(statusErr, "Failed to update status to ready")
		// Even if status update fails, we might not want to requeue immediately
		// if the main reconciliation was successful. Return the original result.
		return result, statusErr // Or return result, nil depending on desired behavior
	}

	log.Info("Successfully reconciled SentryCluster")
	return result, nil
}

// finalizeSentryCluster performs cleanup tasks before the SentryCluster resource is deleted.
func (r *SentryClusterReconciler) finalizeSentryCluster(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) error {
	log := log.FromContext(ctx)
	log.Info("Finalizing SentryCluster resource", "Name", sentryCluster.Name)

	// TODO(user): Add finalization logic here.
	// This logic should delete any external resources associated with the SentryCluster
	// that are not automatically garbage-collected by Kubernetes (e.g., external load balancers, DNS records).
	// For resources owned by the CR (like Deployments, Services created by the operator),
	// Kubernetes garbage collection handles deletion automatically when the owner is deleted,
	// provided OwnerReferences are set correctly.

	log.Info("Successfully finalized SentryCluster resource", "Name", sentryCluster.Name)
	return nil
}

// reconcileSentry contains the core reconciliation logic.
func (r *SentryClusterReconciler) reconcileSentry(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling Sentry", "Name", sentryCluster.Name, "Namespace", sentryCluster.Namespace)

	// Reconcile dependent components in order
	// 1. Config (Secrets/ConfigMaps needed by others)
	configResult, err := r.reconcileConfig(ctx, sentryCluster)
	if err != nil {
		return configResult, errors.Wrap(err, "failed to reconcile Config")
	}
	if configResult.Requeue {
		return configResult, nil
	}

	// 2. Postgres
	postgresResult, err := r.reconcilePostgres(ctx, sentryCluster)
	if err != nil {
		return postgresResult, errors.Wrap(err, "failed to reconcile Postgres")
	}
	if postgresResult.Requeue {
		return postgresResult, nil
	}

	// 3. Redis
	redisResult, err := r.reconcileRedis(ctx, sentryCluster)
	if err != nil {
		return redisResult, errors.Wrap(err, "failed to reconcile Redis")
	}
	if redisResult.Requeue {
		return redisResult, nil
	}

	// 4. Kafka
	kafkaResult, err := r.reconcileKafka(ctx, sentryCluster)
	if err != nil {
		return kafkaResult, errors.Wrap(err, "failed to reconcile Kafka")
	}
	if kafkaResult.Requeue {
		return kafkaResult, nil
	}

	// 5. ClickHouse
	clickhouseResult, err := r.reconcileClickHouse(ctx, sentryCluster)
	if err != nil {
		return clickhouseResult, errors.Wrap(err, "failed to reconcile ClickHouse")
	}
	if clickhouseResult.Requeue {
		return clickhouseResult, nil
	}

	// 6. Snuba
	snubaResult, err := r.reconcileSnuba(ctx, sentryCluster)
	if err != nil {
		return snubaResult, errors.Wrap(err, "failed to reconcile Snuba")
	}
	if snubaResult.Requeue {
		return snubaResult, nil
	}

	// 7. Relay
	relayResult, err := r.reconcileRelay(ctx, sentryCluster)
	if err != nil {
		return relayResult, errors.Wrap(err, "failed to reconcile Relay")
	}
	if relayResult.Requeue {
		return relayResult, nil
	}

	// 8. Symbolicator
	symbolicatorResult, err := r.reconcileSymbolicator(ctx, sentryCluster)
	if err != nil {
		return symbolicatorResult, errors.Wrap(err, "failed to reconcile Symbolicator")
	}
	if symbolicatorResult.Requeue {
		return symbolicatorResult, nil
	}

	// 9. Sentry Worker
	workerResult, err := r.reconcileSentryWorker(ctx, sentryCluster)
	if err != nil {
		return workerResult, errors.Wrap(err, "failed to reconcile Sentry Worker")
	}
	if workerResult.Requeue {
		return workerResult, nil
	}

	// 10. Sentry Web
	webResult, err := r.reconcileSentryWeb(ctx, sentryCluster)
	if err != nil {
		return webResult, errors.Wrap(err, "failed to reconcile Sentry Web")
	}
	if webResult.Requeue {
		return webResult, nil
	}

	// 11. Ingress
	ingressResult, err := r.reconcileIngress(ctx, sentryCluster)
	if err != nil {
		return ingressResult, errors.Wrap(err, "failed to reconcile Ingress")
	}
	if ingressResult.Requeue {
		return ingressResult, nil
	}

	// Update Status field
	if err := r.updateStatus(ctx, sentryCluster); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to update status")
	}

	log.Info("Finished reconciling Sentry components", "Name", sentryCluster.Name)
	return ctrl.Result{}, nil
}

// updateStatus updates the status of the SentryCluster resource
func (r *SentryClusterReconciler) updateStatus(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) error {
	log := log.FromContext(ctx)
	log.Info("Updating SentryCluster status", "Name", sentryCluster.Name)

	// Set the status fields
	sentryCluster.Status.Version = sentryCluster.Spec.Version
	sentryCluster.Status.Phase = "Running" // This should be determined based on component status
	sentryCluster.Status.LastUpdated = metav1.Now()
	sentryCluster.Status.ObservedGeneration = sentryCluster.Generation

	// Set the URL
	if sentryCluster.Spec.Ingress.Enabled {
		protocol := "http"
		if sentryCluster.Spec.Ingress.TLS != nil && sentryCluster.Spec.Ingress.TLS.Enabled {
			protocol = "https"
		}
		path := ""
		if sentryCluster.Spec.Ingress.Path != "" {
			path = sentryCluster.Spec.Ingress.Path
		}
		sentryCluster.Status.URL = fmt.Sprintf("%s://%s%s", protocol, sentryCluster.Spec.Ingress.Host, path)
	}

	// Update the status
	if err := r.Status().Update(ctx, sentryCluster); err != nil {
		log.Error(err, "Failed to update SentryCluster status")
		return err
	}

	return nil
}

// reconcileConfig handles the ConfigMap and Secret for Sentry.
func (r *SentryClusterReconciler) reconcileConfig(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
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
func (r *SentryClusterReconciler) defineSentrySecret(sentryCluster *sentryv1alpha1.SentryCluster) (*corev1.Secret, error) {
	secretKey := sentryCluster.Spec.Config.SecretKey
	if secretKey == "" {
		log.FromContext(context.Background()).Info("Sentry secret key not provided in spec, generating a random one.")
		generatedKey, err := generateRandomString(64)
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
		postgresPassword, err := generateRandomString(32)
		if err != nil {
			return nil, errors.Wrap(err, "failed to generate postgres password")
		}
		secretData["postgres-password"] = postgresPassword
	}

	// Generate Redis password if managing internally
	if sentryCluster.Spec.Persistence.Redis.Managed != nil {
		redisPassword, err := generateRandomString(32)
		if err != nil {
			return nil, errors.Wrap(err, "failed to generate redis password")
		}
		secretData["redis-password"] = redisPassword
	}

	// Generate ClickHouse password if managing internally
	if sentryCluster.Spec.Persistence.ClickHouse != nil && sentryCluster.Spec.Persistence.ClickHouse.Managed != nil {
		clickhousePassword, err := generateRandomString(32)
		if err != nil {
			return nil, errors.Wrap(err, "failed to generate clickhouse password")
		}
		secretData["clickhouse-password"] = clickhousePassword
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-secret",
			Namespace: sentryCluster.Namespace,
			Labels:    getStandardLabels(sentryCluster),
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
func (r *SentryClusterReconciler) defineSentryConfigMap(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.ConfigMap {
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
		if sentryCluster.Spec.Config.Performance.SampleRate > 0 {
			configYmlContent += fmt.Sprintf("performance.sample-rate: %f\n", sentryCluster.Spec.Config.Performance.SampleRate)
		}
		
		if sentryCluster.Spec.Config.Performance.TracesSampleRate > 0 {
			configYmlContent += fmt.Sprintf("performance.traces-sample-rate: %f\n", sentryCluster.Spec.Config.Performance.TracesSampleRate)
		}
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
			Labels:    getStandardLabels(sentryCluster),
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

// reconcilePostgres handles the PostgreSQL deployment for Sentry.
func (r *SentryClusterReconciler) reconcilePostgres(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling Postgres", "SentryCluster", sentryCluster.Name)

	postgresPersistence := sentryCluster.Spec.Persistence.Postgresql

	// Check if external Postgres is configured
	if postgresPersistence.External != nil {
		// Handle external Postgres
		secretName := postgresPersistence.External.SecretName
		log.Info("Using external Postgres", "SecretName", secretName)

		// Fetch the Secret containing the connection details
		secret := &corev1.Secret{}
		err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: sentryCluster.Namespace}, secret)
		if err != nil {
			log.Error(err, "Failed to get external Postgres Secret", "SecretName", secretName)
			return ctrl.Result{}, errors.Wrap(err, "failed to get external Postgres Secret")
		}

		// Validate the Secret data
		host := string(secret.Data["host"])
		port := string(secret.Data["port"])
		user := string(secret.Data["user"])
		dbname := string(secret.Data["dbname"])
		// password := string(secret.Data["password"]) // Optional

		if host == "" || port == "" || user == "" || dbname == "" {
			err := fmt.Errorf("required keys 'host', 'port', 'user', and 'dbname' not found in Secret '%s'", secretName)
			log.Error(err, "Invalid external Postgres Secret")
			return ctrl.Result{}, err
		}

		log.Info("Successfully validated external Postgres connection details", "Host", host, "Port", port, "User", user, "DBName", dbname)
		// Skip creating managed resources (PVC, Service, StatefulSet)
		log.Info("Skipping managed Postgres resources because external Postgres is configured")
		return ctrl.Result{}, nil
	}

	// --- Assuming Managed Postgres ---

	// 1. Reconcile PVC
	if sentryCluster.Spec.Persistence.Postgresql.Managed != nil && sentryCluster.Spec.Persistence.Postgresql.Managed.Size != "" {
		pvcName := sentryCluster.Name + "-postgres-pvc"
		pvc := &corev1.PersistentVolumeClaim{}
		err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: sentryCluster.Namespace}, pvc)
		if err != nil && apierrors.IsNotFound(err) {
			desiredPVC := r.definePostgresPVC(sentryCluster)
			log.Info("Creating a new Postgres PVC", "PVC.Namespace", desiredPVC.Namespace, "PVC.Name", desiredPVC.Name)
			if err := r.Create(ctx, desiredPVC); err != nil {
				log.Error(err, "Failed to create new Postgres PVC", "PVC.Namespace", desiredPVC.Namespace, "PVC.Name", desiredPVC.Name)
				return ctrl.Result{}, errors.Wrap(err, "failed to create Postgres PVC")
			}
			return ctrl.Result{Requeue: true}, nil
		} else if err != nil {
			log.Error(err, "Failed to get Postgres PVC")
			return ctrl.Result{}, errors.Wrap(err, "failed to get Postgres PVC")
		} else {
			log.V(1).Info("Postgres PVC already exists", "PVC.Namespace", pvc.Namespace, "PVC.Name", pvc.Name)
		}
	}

	// 2. Reconcile Service
	serviceName := sentryCluster.Name + "-postgres"
	service := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: sentryCluster.Namespace}, service)
	if err != nil && apierrors.IsNotFound(err) {
		desiredService := r.definePostgresService(sentryCluster)
		log.Info("Creating a new Postgres Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
		if err := r.Create(ctx, desiredService); err != nil {
			log.Error(err, "Failed to create new Postgres Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Postgres Service")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Postgres Service")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Postgres Service")
	} else {
		log.V(1).Info("Postgres Service already exists", "Service.Namespace", service.Namespace, "Service.Name", service.Name)
	}

	// 3. Reconcile StatefulSet
	stsName := sentryCluster.Name + "-postgres"
	sts := &appsv1.StatefulSet{}
	err = r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: sentryCluster.Namespace}, sts)
	if err != nil && apierrors.IsNotFound(err) {
		desiredSts := r.definePostgresStatefulSet(sentryCluster)
		log.Info("Creating a new Postgres StatefulSet", "StatefulSet.Namespace", desiredSts.Namespace, "StatefulSet.Name", desiredSts.Name)
		if err := r.Create(ctx, desiredSts); err != nil {
			log.Error(err, "Failed to create new Postgres StatefulSet", "StatefulSet.Namespace", desiredSts.Namespace, "StatefulSet.Name", desiredSts.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Postgres StatefulSet")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Postgres StatefulSet")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Postgres StatefulSet")
	} else {
		log.V(1).Info("Postgres StatefulSet already exists", "StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
		
		// 4. Check StatefulSet readiness
		if sts.Status.ReadyReplicas < *sts.Spec.Replicas {
			log.Info("Postgres StatefulSet not yet ready", "ReadyReplicas", sts.Status.ReadyReplicas, "Replicas", *sts.Spec.Replicas)
			return ctrl.Result{RequeueAfter: time.Second * 30}, nil
		}
		
		// 5. Update StatefulSet if needed
		desiredSts := r.definePostgresStatefulSet(sentryCluster)
		if !reflect.DeepEqual(sts.Spec, desiredSts.Spec) {
			log.Info("Updating existing Postgres StatefulSet", "StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
			sts.Spec = desiredSts.Spec
			if err := r.Update(ctx, sts); err != nil {
				log.Error(err, "Failed to update Postgres StatefulSet", "StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
				return ctrl.Result{}, errors.Wrap(err, "failed to update Postgres StatefulSet")
			}
			return ctrl.Result{Requeue: true}, nil
		}
	}

	log.Info("Postgres reconciled successfully", "SentryCluster", sentryCluster.Name)
	return ctrl.Result{}, nil
}

// definePostgresPVC creates the desired PersistentVolumeClaim object for PostgreSQL.
func (r *SentryClusterReconciler) definePostgresPVC(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.PersistentVolumeClaim {
	labels := getPostgresLabels(sentryCluster)
	
	// Parse the storage size
	storageSize := resource.MustParse(sentryCluster.Spec.Persistence.Postgresql.Managed.Size)
	
	// Create the PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-postgres-pvc",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: storageSize,
				},
			},
			StorageClassName: &sentryCluster.Spec.Persistence.Postgresql.Managed.StorageClass,
		},
	}
	
	// If StorageClass is empty, set it to nil to use the default StorageClass
	if sentryCluster.Spec.Persistence.Postgresql.Managed.StorageClass == "" {
		pvc.Spec.StorageClassName = nil
	}
	
	if err := controllerutil.SetControllerReference(sentryCluster, pvc, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on PostgreSQL PVC")
	}
	return pvc
}

// getPostgresLabels returns the labels for PostgreSQL components
func getPostgresLabels(sentryCluster *sentryv1alpha1.SentryCluster) map[string]string {
	labels := getStandardLabels(sentryCluster)
	labels["app.kubernetes.io/component"] = "postgresql"
	return labels
}

// definePostgresService creates the desired Service object for PostgreSQL.
func (r *SentryClusterReconciler) definePostgresService(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.Service {
	labels := getPostgresLabels(sentryCluster)
	
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-postgres",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				Port:       postgresPort,
				TargetPort: intstr.FromInt(postgresPort),
				Name:       "postgresql",
			}},
			Selector: labels,
			Type:     corev1.ServiceTypeClusterIP,
		},
	}
	if err := controllerutil.SetControllerReference(sentryCluster, svc, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on PostgreSQL Service")
	}
	return svc
}

// definePostgresStatefulSet creates the desired StatefulSet object for PostgreSQL.
func (r *SentryClusterReconciler) definePostgresStatefulSet(sentryCluster *sentryv1alpha1.SentryCluster) *appsv1.StatefulSet {
	labels := getPostgresLabels(sentryCluster)
	
	// Set default values
	replicas := int32(1) // PostgreSQL is typically deployed as a single instance in this setup
	
	// Get the secret name
	secretName := sentryCluster.Name + "-secret"
	
	// Create the StatefulSet
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-postgres",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: sentryCluster.Name + "-postgres",
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "postgresql",
						Image: "postgres:13-alpine",
						Ports: []corev1.ContainerPort{{
							ContainerPort: postgresPort,
							Name:          "postgresql",
						}},
						Env: []corev1.EnvVar{
							{Name: "POSTGRES_USER", Value: postgresUser},
							{Name: "POSTGRES_DB", Value: postgresDB},
							{Name: "POSTGRES_PASSWORD", ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
									Key:                  "postgres-password",
								},
							}},
							{Name: "PGDATA", Value: "/var/lib/postgresql/data/pgdata"},
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "data",
							MountPath: "/var/lib/postgresql/data",
						}},
						Resources: sentryCluster.Spec.Resources.Postgresql,
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{
										"pg_isready",
										"-U", postgresUser,
									},
								},
							},
							InitialDelaySeconds: 5,
							TimeoutSeconds:      5,
							PeriodSeconds:       10,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{
										"pg_isready",
										"-U", postgresUser,
									},
								},
							},
							InitialDelaySeconds: 30,
							TimeoutSeconds:      5,
							PeriodSeconds:       15,
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: sentryCluster.Name + "-postgres-pvc",
							},
						},
					}},
				},
			},
		},
	}
	
	if err := controllerutil.SetControllerReference(sentryCluster, sts, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on PostgreSQL StatefulSet")
	}
	return sts
}

// reconcileRedis handles the Redis deployment for Sentry.
func (r *SentryClusterReconciler) reconcileRedis(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling Redis", "SentryCluster", sentryCluster.Name)

	redisPersistence := sentryCluster.Spec.Persistence.Redis

	// Check if external Redis is configured
	if redisPersistence.External != nil {
		// Handle external Redis
		secretName := redisPersistence.External.SecretName
		log.Info("Using external Redis", "SecretName", secretName)

		// Fetch the Secret containing the connection details
		secret := &corev1.Secret{}
		err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: sentryCluster.Namespace}, secret)
		if err != nil {
			log.Error(err, "Failed to get external Redis Secret", "SecretName", secretName)
			return ctrl.Result{}, errors.Wrap(err, "failed to get external Redis Secret")
		}

		// Validate the Secret data
		host := string(secret.Data["host"])
		port := string(secret.Data["port"])
		// password := string(secret.Data["password"]) // Optional

		if host == "" || port == "" {
			err := fmt.Errorf("required keys 'host' and 'port' not found in Secret '%s'", secretName)
			log.Error(err, "Invalid external Redis Secret")
			return ctrl.Result{}, err
		}

		log.Info("Successfully validated external Redis connection details", "Host", host, "Port", port)
		// Skip creating managed resources (PVC, Service, StatefulSet)
		log.Info("Skipping managed Redis resources because external Redis is configured")
		return ctrl.Result{}, nil
	}

	// --- Assuming Managed Redis ---

	// 1. Reconcile PVC
	if sentryCluster.Spec.Persistence.Redis.Managed != nil && sentryCluster.Spec.Persistence.Redis.Managed.Size != "" {
		pvcName := sentryCluster.Name + "-redis-pvc"
		pvc := &corev1.PersistentVolumeClaim{}
		err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: sentryCluster.Namespace}, pvc)
		if err != nil && apierrors.IsNotFound(err) {
			desiredPVC := r.defineRedisPVC(sentryCluster)
			log.Info("Creating a new Redis PVC", "PVC.Namespace", desiredPVC.Namespace, "PVC.Name", desiredPVC.Name)
			if err := r.Create(ctx, desiredPVC); err != nil {
				log.Error(err, "Failed to create new Redis PVC", "PVC.Namespace", desiredPVC.Namespace, "PVC.Name", desiredPVC.Name)
				return ctrl.Result{}, errors.Wrap(err, "failed to create Redis PVC")
			}
			return ctrl.Result{Requeue: true}, nil
		} else if err != nil {
			log.Error(err, "Failed to get Redis PVC")
			return ctrl.Result{}, errors.Wrap(err, "failed to get Redis PVC")
		} else {
			log.V(1).Info("Redis PVC already exists", "PVC.Namespace", pvc.Namespace, "PVC.Name", pvc.Name)
		}
	}

	// 2. Reconcile Service
	serviceName := sentryCluster.Name + "-redis"
	service := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: sentryCluster.Namespace}, service)
	if err != nil && apierrors.IsNotFound(err) {
		desiredService := r.defineRedisService(sentryCluster)
		log.Info("Creating a new Redis Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
		if err := r.Create(ctx, desiredService); err != nil {
			log.Error(err, "Failed to create new Redis Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Redis Service")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Redis Service")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Redis Service")
	} else {
		log.V(1).Info("Redis Service already exists", "Service.Namespace", service.Namespace, "Service.Name", service.Name)
	}

	// 3. Reconcile StatefulSet
	stsName := sentryCluster.Name + "-redis"
	sts := &appsv1.StatefulSet{}
	err = r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: sentryCluster.Namespace}, sts)
	if err != nil && apierrors.IsNotFound(err) {
		desiredSts := r.defineRedisStatefulSet(sentryCluster)
		log.Info("Creating a new Redis StatefulSet", "StatefulSet.Namespace", desiredSts.Namespace, "StatefulSet.Name", desiredSts.Name)
		if err := r.Create(ctx, desiredSts); err != nil {
			log.Error(err, "Failed to create new Redis StatefulSet", "StatefulSet.Namespace", desiredSts.Namespace, "StatefulSet.Name", desiredSts.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Redis StatefulSet")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Redis StatefulSet")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Redis StatefulSet")
	} else {
		log.V(1).Info("Redis StatefulSet already exists", "StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
		
		// 4. Check StatefulSet readiness
		if sts.Status.ReadyReplicas < *sts.Spec.Replicas {
			log.Info("Redis StatefulSet not yet ready", "ReadyReplicas", sts.Status.ReadyReplicas, "Replicas", *sts.Spec.Replicas)
			return ctrl.Result{RequeueAfter: time.Second * 30}, nil
		}
		
		// 5. Update StatefulSet if needed
		desiredSts := r.defineRedisStatefulSet(sentryCluster)
		if !reflect.DeepEqual(sts.Spec, desiredSts.Spec) {
			log.Info("Updating existing Redis StatefulSet", "StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
			sts.Spec = desiredSts.Spec
			if err := r.Update(ctx, sts); err != nil {
				log.Error(err, "Failed to update Redis StatefulSet", "StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
				return ctrl.Result{}, errors.Wrap(err, "failed to update Redis StatefulSet")
			}
			return ctrl.Result{Requeue: true}, nil
		}
	}

	log.Info("Redis reconciled successfully", "SentryCluster", sentryCluster.Name)
	return ctrl.Result{}, nil
}

// defineRedisPVC creates the desired PersistentVolumeClaim object for Redis.
func (r *SentryClusterReconciler) defineRedisPVC(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.PersistentVolumeClaim {
	labels := getRedisLabels(sentryCluster)
	
	// Parse the storage size
	storageSize := resource.MustParse(sentryCluster.Spec.Persistence.Redis.Managed.Size)
	
	// Create the PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-redis-pvc",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: storageSize,
				},
			},
			StorageClassName: &sentryCluster.Spec.Persistence.Redis.Managed.StorageClass,
		},
	}
	
	// If StorageClass is empty, set it to nil to use the default StorageClass
	if sentryCluster.Spec.Persistence.Redis.Managed.StorageClass == "" {
		pvc.Spec.StorageClassName = nil
	}
	
	if err := controllerutil.SetControllerReference(sentryCluster, pvc, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Redis PVC")
	}
	return pvc
}

// getRedisLabels returns the labels for Redis components
func getRedisLabels(sentryCluster *sentryv1alpha1.SentryCluster) map[string]string {
	labels := getStandardLabels(sentryCluster)
	labels["app.kubernetes.io/component"] = "redis"
	return labels
}

// defineRedisService creates the desired Service object for Redis.
func (r *SentryClusterReconciler) defineRedisService(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.Service {
	labels := getRedisLabels(sentryCluster)
	
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-redis",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				Port:       redisPort,
				TargetPort: intstr.FromInt(redisPort),
				Name:       "redis",
			}},
			Selector: labels,
			Type:     corev1.ServiceTypeClusterIP,
		},
	}
	if err := controllerutil.SetControllerReference(sentryCluster, svc, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Redis Service")
	}
	return svc
}

// defineRedisStatefulSet creates the desired StatefulSet object for Redis.
func (r *SentryClusterReconciler) defineRedisStatefulSet(sentryCluster *sentryv1alpha1.SentryCluster) *appsv1.StatefulSet {
	labels := getRedisLabels(sentryCluster)
	
	// Set default values
	replicas := int32(1) // Redis is typically deployed as a single instance in this setup
	
	// Get the secret name
	secretName := sentryCluster.Name + "-secret"
	
	// Create the StatefulSet
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-redis",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: sentryCluster.Name + "-redis",
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "redis",
						Image: "redis:6-alpine",
						Ports: []corev1.ContainerPort{{
							ContainerPort: redisPort,
							Name:          "redis",
						}},
						Command: []string{
							"redis-server",
							"--requirepass", "$(REDIS_PASSWORD)",
						},
						Env: []corev1.EnvVar{
							{Name: "REDIS_PASSWORD", ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
									Key:                  "redis-password",
								},
							}},
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "data",
							MountPath: "/data",
						}},
						Resources: sentryCluster.Spec.Resources.Redis,
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{
										"redis-cli",
										"ping",
									},
								},
							},
							InitialDelaySeconds: 5,
							TimeoutSeconds:      5,
							PeriodSeconds:       10,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{
										"redis-cli",
										"ping",
									},
								},
							},
							InitialDelaySeconds: 30,
							TimeoutSeconds:      5,
							PeriodSeconds:       15,
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: sentryCluster.Name + "-redis-pvc",
							},
						},
					}},
				},
			},
		},
	}
	
	if err := controllerutil.SetControllerReference(sentryCluster, sts, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Redis StatefulSet")
	}
	return sts
}
