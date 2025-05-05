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
	"fmt"
	"time"

	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	sentryv1alpha1 "github.com/abalhamoud/sentry-operator/api/v1alpha1"
	"github.com/abalhamoud/sentry-operator/internal/controller/components"
	"github.com/abalhamoud/sentry-operator/internal/controller/jobs"
)

// SentryClusterReconciler reconciles a SentryCluster object
type SentryClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Component reconcilers
	ConfigReconciler       *components.ConfigReconciler
	PostgresReconciler     *components.PostgresReconciler
	RedisReconciler        *components.RedisReconciler
	KafkaReconciler        *components.KafkaReconciler
	ClickHouseReconciler   *components.ClickHouseReconciler
	RelayReconciler        *components.RelayReconciler
	SnubaReconciler        *components.SnubaReconciler
	SymbolicatorReconciler *components.SymbolicatorReconciler
	SentryWebReconciler    *components.SentryWebReconciler
	SentryWorkerReconciler *components.SentryWorkerReconciler
	IngressReconciler      *components.IngressReconciler

	// Job reconcilers
	PostgresInitJobReconciler   *jobs.PostgresInitJobReconciler
	KafkaInitJobReconciler      *jobs.KafkaInitJobReconciler
	SnubaBootstrapJobReconciler *jobs.SnubaBootstrapJobReconciler
	SentryMigrationJobReconciler *jobs.SentryMigrationJobReconciler
}

// SetupWithManager sets up the controller with the Manager.
func (r *SentryClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Initialize component reconcilers
	r.ConfigReconciler = &components.ConfigReconciler{
		Client: r.Client,
		Scheme: r.Scheme,
	}

	r.PostgresReconciler = &components.PostgresReconciler{
		Client: r.Client,
		Scheme: r.Scheme,
	}

	r.RedisReconciler = &components.RedisReconciler{
		Client: r.Client,
		Scheme: r.Scheme,
	}

	r.KafkaReconciler = &components.KafkaReconciler{
		Client: r.Client,
		Scheme: r.Scheme,
	}

	r.ClickHouseReconciler = &components.ClickHouseReconciler{
		Client: r.Client,
		Scheme: r.Scheme,
	}

	r.RelayReconciler = &components.RelayReconciler{
		Client: r.Client,
		Scheme: r.Scheme,
	}

	r.SnubaReconciler = &components.SnubaReconciler{
		Client: r.Client,
		Scheme: r.Scheme,
	}

	r.SymbolicatorReconciler = &components.SymbolicatorReconciler{
		Client: r.Client,
		Scheme: r.Scheme,
	}

	r.SentryWebReconciler = &components.SentryWebReconciler{
		Client: r.Client,
		Scheme: r.Scheme,
	}

	r.SentryWorkerReconciler = &components.SentryWorkerReconciler{
		Client: r.Client,
		Scheme: r.Scheme,
	}

	r.IngressReconciler = &components.IngressReconciler{
		Client: r.Client,
		Scheme: r.Scheme,
	}

	// Initialize job reconcilers
	r.PostgresInitJobReconciler = &jobs.PostgresInitJobReconciler{
		BaseJobReconciler: jobs.BaseJobReconciler{
			Client: r.Client,
			Scheme: r.Scheme,
		},
	}

	r.KafkaInitJobReconciler = &jobs.KafkaInitJobReconciler{
		BaseJobReconciler: jobs.BaseJobReconciler{
			Client: r.Client,
			Scheme: r.Scheme,
		},
	}

	r.SnubaBootstrapJobReconciler = &jobs.SnubaBootstrapJobReconciler{
		BaseJobReconciler: jobs.BaseJobReconciler{
			Client: r.Client,
			Scheme: r.Scheme,
		},
	}

	r.SentryMigrationJobReconciler = &jobs.SentryMigrationJobReconciler{
		BaseJobReconciler: jobs.BaseJobReconciler{
			Client: r.Client,
			Scheme: r.Scheme,
		},
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&sentryv1alpha1.SentryCluster{}).
		Complete(r)
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
		log.Info("Trying to delete cluster...","Name", sentryCluster.Name)
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
			log.Info("Succesfully removed Finalizer from cluster")
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

	// Initialize component status if not already set
	if sentryCluster.Status.ComponentStatus == (sentryv1alpha1.ComponentStatus{}) {
		sentryCluster.Status.ComponentStatus = sentryv1alpha1.ComponentStatus{}
	}

	// Reconcile dependent components in order
	// 1. Config (Secrets/ConfigMaps needed by others)
	configResult, err := r.reconcileConfig(ctx, sentryCluster)
	if err != nil {
		r.updateComponentStatus(ctx, sentryCluster, "Config", false, err.Error())
		return configResult, errors.Wrap(err, "failed to reconcile Config")
	}
	if configResult.Requeue {
		return configResult, nil
	}
	r.updateComponentStatus(ctx, sentryCluster, "Config", true, "Config reconciled successfully")

	// 2. Postgres
	postgresResult, err := r.reconcilePostgres(ctx, sentryCluster)
	if err != nil {
		r.updateComponentStatus(ctx, sentryCluster, "Postgresql", false, err.Error())
		return postgresResult, errors.Wrap(err, "failed to reconcile Postgres")
	}
	if postgresResult.Requeue {
		return postgresResult, nil
	}
	r.updateComponentStatus(ctx, sentryCluster, "Postgresql", true, "PostgreSQL reconciled successfully")

	// 2.1 Postgres Initialization Job
	postgresInitResult, err := r.reconcilePostgresInitJob(ctx, sentryCluster)
	if err != nil {
		return postgresInitResult, errors.Wrap(err, "failed to reconcile Postgres initialization job")
	}
	if postgresInitResult.Requeue {
		return postgresInitResult, nil
	}

	// 3. Redis
	redisResult, err := r.reconcileRedis(ctx, sentryCluster)
	if err != nil {
		r.updateComponentStatus(ctx, sentryCluster, "Redis", false, err.Error())
		return redisResult, errors.Wrap(err, "failed to reconcile Redis")
	}
	if redisResult.Requeue {
		return redisResult, nil
	}
	r.updateComponentStatus(ctx, sentryCluster, "Redis", true, "Redis reconciled successfully")

	// 4. Kafka
	kafkaResult, err := r.reconcileKafka(ctx, sentryCluster)
	if err != nil {
		r.updateComponentStatus(ctx, sentryCluster, "Kafka", false, err.Error())
		return kafkaResult, errors.Wrap(err, "failed to reconcile Kafka")
	}
	if kafkaResult.Requeue {
		return kafkaResult, nil
	}
	r.updateComponentStatus(ctx, sentryCluster, "Kafka", true, "Kafka reconciled successfully")

	// 4.1 Kafka Initialization Job
	kafkaInitResult, err := r.reconcileKafkaInitJob(ctx, sentryCluster)
	if err != nil {
		return kafkaInitResult, errors.Wrap(err, "failed to reconcile Kafka initialization job")
	}
	if kafkaInitResult.Requeue {
		return kafkaInitResult, nil
	}

	// 5. ClickHouse
	clickhouseResult, err := r.reconcileClickHouse(ctx, sentryCluster)
	if err != nil {
		r.updateComponentStatus(ctx, sentryCluster, "ClickHouse", false, err.Error())
		return clickhouseResult, errors.Wrap(err, "failed to reconcile ClickHouse")
	}
	if clickhouseResult.Requeue {
		return clickhouseResult, nil
	}
	r.updateComponentStatus(ctx, sentryCluster, "ClickHouse", true, "ClickHouse reconciled successfully")

	// 6. Snuba - depends on Kafka and ClickHouse
	if !r.areComponentsReady(sentryCluster, []string{"Kafka", "ClickHouse"}) {
		log.Info("Waiting for Kafka and ClickHouse to be ready before reconciling Snuba")
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}
	
	snubaResult, err := r.reconcileSnuba(ctx, sentryCluster)
	if err != nil {
		r.updateComponentStatus(ctx, sentryCluster, "Snuba", false, err.Error())
		return snubaResult, errors.Wrap(err, "failed to reconcile Snuba")
	}
	if snubaResult.Requeue {
		return snubaResult, nil
	}
	r.updateComponentStatus(ctx, sentryCluster, "Snuba", true, "Snuba reconciled successfully")

	// 6.1 Snuba Bootstrap Job
	snubaBootstrapResult, err := r.reconcileSnubaBootstrapJob(ctx, sentryCluster)
	if err != nil {
		return snubaBootstrapResult, errors.Wrap(err, "failed to reconcile Snuba bootstrap job")
	}
	if snubaBootstrapResult.Requeue {
		return snubaBootstrapResult, nil
	}

	// 7. Symbolicator
	symbolicatorResult, err := r.reconcileSymbolicator(ctx, sentryCluster)
	if err != nil {
		r.updateComponentStatus(ctx, sentryCluster, "Symbolicator", false, err.Error())
		return symbolicatorResult, errors.Wrap(err, "failed to reconcile Symbolicator")
	}
	if symbolicatorResult.Requeue {
		return symbolicatorResult, nil
	}
	r.updateComponentStatus(ctx, sentryCluster, "Symbolicator", true, "Symbolicator reconciled successfully")

	// 8. Sentry Worker - depends on Postgres, Redis, Kafka, and Snuba
	if !r.areComponentsReady(sentryCluster, []string{"Postgresql", "Redis", "Kafka", "Snuba"}) {
		log.Info("Waiting for Postgres, Redis, Kafka, and Snuba to be ready before reconciling Sentry Worker")
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}
	
	workerResult, err := r.reconcileSentryWorker(ctx, sentryCluster)
	if err != nil {
		r.updateComponentStatus(ctx, sentryCluster, "Worker", false, err.Error())
		return workerResult, errors.Wrap(err, "failed to reconcile Sentry Worker")
	}
	if workerResult.Requeue {
		return workerResult, nil
	}
	r.updateComponentStatus(ctx, sentryCluster, "Worker", true, "Sentry Worker reconciled successfully")

	// 9. Sentry Web - depends on Postgres, Redis, Kafka, Snuba, and Symbolicator
	if !r.areComponentsReady(sentryCluster, []string{"Postgresql", "Redis", "Kafka", "Snuba", "Symbolicator"}) {
		log.Info("Waiting for Postgres, Redis, Kafka, Snuba, and Symbolicator to be ready before reconciling Sentry Web")
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}
	
	webResult, err := r.reconcileSentryWeb(ctx, sentryCluster)
	if err != nil {
		r.updateComponentStatus(ctx, sentryCluster, "Web", false, err.Error())
		return webResult, errors.Wrap(err, "failed to reconcile Sentry Web")
	}
	if webResult.Requeue {
		return webResult, nil
	}
	r.updateComponentStatus(ctx, sentryCluster, "Web", true, "Sentry Web reconciled successfully")

	// 9.1 Sentry Migration Job
	sentryMigrationResult, err := r.reconcileSentryMigrationJob(ctx, sentryCluster)
	if err != nil {
		return sentryMigrationResult, errors.Wrap(err, "failed to reconcile Sentry migration job")
	}
	if sentryMigrationResult.Requeue {
		return sentryMigrationResult, nil
	}

	// 10. Relay - depends on Sentry Web
	if !r.areComponentsReady(sentryCluster, []string{"Web"}) {
		log.Info("Waiting for Sentry Web to be ready before reconciling Relay")
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}
	
	relayResult, err := r.reconcileRelay(ctx, sentryCluster)
	if err != nil {
		r.updateComponentStatus(ctx, sentryCluster, "Relay", false, err.Error())
		return relayResult, errors.Wrap(err, "failed to reconcile Relay")
	}
	if relayResult.Requeue {
		return relayResult, nil
	}
	r.updateComponentStatus(ctx, sentryCluster, "Relay", true, "Relay reconciled successfully")

	// 11. Ingress - depends on Sentry Web
	if !r.areComponentsReady(sentryCluster, []string{"Web"}) {
		log.Info("Waiting for Sentry Web to be ready before reconciling Ingress")
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}
	
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

// updateComponentStatus updates the status of a specific component in the SentryCluster status
func (r *SentryClusterReconciler) updateComponentStatus(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster, componentName string, ready bool, message string) {
	log := log.FromContext(ctx)
	
	// Update the component status
	switch componentName {
	case "Config":
		// Config doesn't have a dedicated status field, so we just log it
		log.Info("Config status updated", "Ready", ready, "Message", message)
	case "Postgresql":
		sentryCluster.Status.ComponentStatus.Postgresql.Ready = ready
		sentryCluster.Status.ComponentStatus.Postgresql.Message = message
	case "Redis":
		sentryCluster.Status.ComponentStatus.Redis.Ready = ready
		sentryCluster.Status.ComponentStatus.Redis.Message = message
	case "Kafka":
		sentryCluster.Status.ComponentStatus.Kafka.Ready = ready
		sentryCluster.Status.ComponentStatus.Kafka.Message = message
	case "ClickHouse":
		sentryCluster.Status.ComponentStatus.ClickHouse.Ready = ready
		sentryCluster.Status.ComponentStatus.ClickHouse.Message = message
	case "Snuba":
		sentryCluster.Status.ComponentStatus.Snuba.Ready = ready
		sentryCluster.Status.ComponentStatus.Snuba.Message = message
	case "Symbolicator":
		sentryCluster.Status.ComponentStatus.Symbolicator.Ready = ready
		sentryCluster.Status.ComponentStatus.Symbolicator.Message = message
	case "Worker":
		sentryCluster.Status.ComponentStatus.Worker.Ready = ready
		sentryCluster.Status.ComponentStatus.Worker.Message = message
	case "Web":
		sentryCluster.Status.ComponentStatus.Web.Ready = ready
		sentryCluster.Status.ComponentStatus.Web.Message = message
	case "Relay":
		sentryCluster.Status.ComponentStatus.Relay.Ready = ready
		sentryCluster.Status.ComponentStatus.Relay.Message = message
	default:
		log.Info("Unknown component", "Component", componentName)
	}
	
	// Update the status
	if err := r.Status().Update(ctx, sentryCluster); err != nil {
		log.Error(err, "Failed to update component status", "Component", componentName)
	}
}

// areComponentsReady checks if all the specified components are ready
func (r *SentryClusterReconciler) areComponentsReady(sentryCluster *sentryv1alpha1.SentryCluster, componentNames []string) bool {
	for _, name := range componentNames {
		switch name {
		case "Postgresql":
			if !sentryCluster.Status.ComponentStatus.Postgresql.Ready {
				return false
			}
		case "Redis":
			if !sentryCluster.Status.ComponentStatus.Redis.Ready {
				return false
			}
		case "Kafka":
			if !sentryCluster.Status.ComponentStatus.Kafka.Ready {
				return false
			}
		case "ClickHouse":
			if !sentryCluster.Status.ComponentStatus.ClickHouse.Ready {
				return false
			}
		case "Snuba":
			if !sentryCluster.Status.ComponentStatus.Snuba.Ready {
				return false
			}
		case "Symbolicator":
			if !sentryCluster.Status.ComponentStatus.Symbolicator.Ready {
				return false
			}
		case "Worker":
			if !sentryCluster.Status.ComponentStatus.Worker.Ready {
				return false
			}
		case "Web":
			if !sentryCluster.Status.ComponentStatus.Web.Ready {
				return false
			}
		case "Relay":
			if !sentryCluster.Status.ComponentStatus.Relay.Ready {
				return false
			}
		}
	}
	return true
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
	return r.ConfigReconciler.Reconcile(ctx, sentryCluster)
}

// reconcilePostgres handles the PostgreSQL deployment for Sentry.
func (r *SentryClusterReconciler) reconcilePostgres(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	return r.PostgresReconciler.Reconcile(ctx, sentryCluster)
}

// reconcileKafka handles the Kafka deployment for Sentry.
func (r *SentryClusterReconciler) reconcileKafka(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	return r.KafkaReconciler.Reconcile(ctx, sentryCluster)
}

// reconcileClickHouse handles the ClickHouse deployment for Sentry.
func (r *SentryClusterReconciler) reconcileClickHouse(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	return r.ClickHouseReconciler.Reconcile(ctx, sentryCluster)
}

// setStatusError sets the status condition to error
func (r *SentryClusterReconciler) setStatusError(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster, err error) error {
	log := log.FromContext(ctx)
	log.Info("Setting error status", "Error", err.Error())

	// Create a new condition
	condition := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             "ReconciliationError",
		Message:            err.Error(),
		LastTransitionTime: metav1.Now(),
	}

	// Update the status
	apimeta.SetStatusCondition(&sentryCluster.Status.Conditions, condition)
	sentryCluster.Status.Phase = "Error"
	sentryCluster.Status.LastUpdated = metav1.Now()

	// Update the CR
	if updateErr := r.Status().Update(ctx, sentryCluster); updateErr != nil {
		log.Error(updateErr, "Failed to update status with error condition")
		return updateErr
	}

	return nil
}

// setStatusReady sets the status condition to ready
func (r *SentryClusterReconciler) setStatusReady(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) error {
	log := log.FromContext(ctx)
	log.Info("Setting ready status")

	// Create a new condition
	condition := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "ReconciliationSucceeded",
		Message:            "Sentry cluster is ready",
		LastTransitionTime: metav1.Now(),
	}

	// Update the status
	apimeta.SetStatusCondition(&sentryCluster.Status.Conditions, condition)
	sentryCluster.Status.Phase = "Running"
	sentryCluster.Status.LastUpdated = metav1.Now()

	// Update the CR
	if updateErr := r.Status().Update(ctx, sentryCluster); updateErr != nil {
		log.Error(updateErr, "Failed to update status with ready condition")
		return updateErr
	}

	return nil
}

// reconcileRedis handles the Redis deployment for Sentry.
func (r *SentryClusterReconciler) reconcileRedis(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	return r.RedisReconciler.Reconcile(ctx, sentryCluster)
}

// reconcileRelay handles the Relay deployment for Sentry.
func (r *SentryClusterReconciler) reconcileRelay(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	return r.RelayReconciler.Reconcile(ctx, sentryCluster)
}

// reconcileSymbolicator handles the Symbolicator deployment for Sentry.
func (r *SentryClusterReconciler) reconcileSymbolicator(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	return r.SymbolicatorReconciler.Reconcile(ctx, sentryCluster)
}

// reconcileSentryWeb handles the Sentry Web deployment for Sentry.
func (r *SentryClusterReconciler) reconcileSentryWeb(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	return r.SentryWebReconciler.Reconcile(ctx, sentryCluster)
}

// reconcileSentryWorker handles the Sentry Worker deployment for Sentry.
func (r *SentryClusterReconciler) reconcileSentryWorker(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	return r.SentryWorkerReconciler.Reconcile(ctx, sentryCluster)
}

// reconcileIngress handles the Ingress for Sentry.
func (r *SentryClusterReconciler) reconcileIngress(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	return r.IngressReconciler.Reconcile(ctx, sentryCluster)
}

// reconcileSnuba handles the Sentry Snuba deployment for Sentry.
func (r *SentryClusterReconciler) reconcileSnuba(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	return r.SnubaReconciler.Reconcile(ctx, sentryCluster)
}

// reconcilePostgresInitJob handles the PostgreSQL initialization job.
func (r *SentryClusterReconciler) reconcilePostgresInitJob(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	return r.PostgresInitJobReconciler.Reconcile(ctx, sentryCluster)
}

// reconcileKafkaInitJob handles the Kafka initialization job.
func (r *SentryClusterReconciler) reconcileKafkaInitJob(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	return r.KafkaInitJobReconciler.Reconcile(ctx, sentryCluster)
}

// reconcileSnubaBootstrapJob handles the Snuba bootstrap job.
func (r *SentryClusterReconciler) reconcileSnubaBootstrapJob(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	return r.SnubaBootstrapJobReconciler.Reconcile(ctx, sentryCluster)
}

// reconcileSentryMigrationJob handles the Sentry migration job.
func (r *SentryClusterReconciler) reconcileSentryMigrationJob(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	return r.SentryMigrationJobReconciler.Reconcile(ctx, sentryCluster)
}
