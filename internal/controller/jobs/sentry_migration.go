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

package jobs

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	sentryv1alpha1 "github.com/abalhamoud/sentry-operator/api/v1alpha1"
)

// SentryMigrationJobReconciler reconciles the Sentry migration job
type SentryMigrationJobReconciler struct {
	BaseJobReconciler
}

// Reconcile handles the Sentry migration job
func (r *SentryMigrationJobReconciler) Reconcile(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling Sentry migration job", "SentryCluster", sentryCluster.Name)

	// Check if PostgreSQL is ready and initialized
	if !sentryCluster.Status.ComponentStatus.Postgresql.Ready {
		log.Info("PostgreSQL is not ready yet, waiting")
		return ctrl.Result{Requeue: true}, nil
	}

	// Check if PostgreSQL initialization is complete
	if !r.IsJobComplete(sentryCluster, "PostgresInitialized") {
		log.Info("PostgreSQL initialization is not complete yet, waiting")
		return ctrl.Result{Requeue: true}, nil
	}

	// Check if Redis is ready
	if !sentryCluster.Status.ComponentStatus.Redis.Ready {
		log.Info("Redis is not ready yet, waiting")
		return ctrl.Result{Requeue: true}, nil
	}

	// Check if Kafka is ready and initialized
	if !sentryCluster.Status.ComponentStatus.Kafka.Ready {
		log.Info("Kafka is not ready yet, waiting")
		return ctrl.Result{Requeue: true}, nil
	}

	// Check if Kafka initialization is complete
	if !r.IsJobComplete(sentryCluster, "KafkaInitialized") {
		log.Info("Kafka initialization is not complete yet, waiting")
		return ctrl.Result{Requeue: true}, nil
	}

	// Check if ClickHouse is ready
	if !sentryCluster.Status.ComponentStatus.ClickHouse.Ready {
		log.Info("ClickHouse is not ready yet, waiting")
		return ctrl.Result{Requeue: true}, nil
	}

	// Check if Snuba is ready and bootstrapped
	if !sentryCluster.Status.ComponentStatus.Snuba.Ready {
		log.Info("Snuba is not ready yet, waiting")
		return ctrl.Result{Requeue: true}, nil
	}

	// Check if Snuba bootstrap is complete
	if !r.IsJobComplete(sentryCluster, "SnubaBootstrapped") {
		log.Info("Snuba bootstrap is not complete yet, waiting")
		return ctrl.Result{Requeue: true}, nil
	}

	return r.ReconcileJob(ctx, sentryCluster, r)
}

// GetJobName returns the name of the job
func (r *SentryMigrationJobReconciler) GetJobName(sentryCluster *sentryv1alpha1.SentryCluster) string {
	return fmt.Sprintf("%s-sentry-migration-%s", sentryCluster.Name, sentryCluster.Spec.Version)
}

// GetJobConditionType returns the condition type for the job
func (r *SentryMigrationJobReconciler) GetJobConditionType() string {
	return "SentryMigrated"
}

// ShouldRunJob determines if the job should be run
func (r *SentryMigrationJobReconciler) ShouldRunJob(sentryCluster *sentryv1alpha1.SentryCluster) bool {
	// Check if the job has already been completed for this version
	condition := sentryCluster.Status.GetCondition(r.GetJobConditionType())
	if condition != nil && condition.Status == metav1.ConditionTrue {
		// Check if the version has changed since the last migration
		if condition.Message == fmt.Sprintf("Sentry migration completed successfully for version %s", sentryCluster.Spec.Version) {
			return false
		}
	}
	return true
}

// CreateJob creates the job
func (r *SentryMigrationJobReconciler) CreateJob(sentryCluster *sentryv1alpha1.SentryCluster) *batchv1.Job {
	labels := GetJobLabels(sentryCluster, "sentry-migration")
	
	// Get the secret name
	secretName := sentryCluster.Name + "-secret"
	
	// Create the job
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.GetJobName(sentryCluster),
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: func() *int32 { i := int32(3); return &i }(),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:  "sentry-migration",
							Image: fmt.Sprintf("getsentry/sentry:%s", sentryCluster.Spec.Version),
							Command: []string{
								"bash",
								"-c",
								`
# Wait for PostgreSQL to be ready
until python -c "import psycopg2; psycopg2.connect(dbname='$POSTGRES_DB', user='$POSTGRES_USER', password='$POSTGRES_PASSWORD', host='$POSTGRES_HOST', port='$POSTGRES_PORT')"; do
  echo "Waiting for PostgreSQL to be ready..."
  sleep 5
done

# Wait for Redis to be ready
until python -c "import redis; redis.Redis(host='$REDIS_HOST', port=$REDIS_PORT).ping()"; do
  echo "Waiting for Redis to be ready..."
  sleep 5
done

# Run Sentry migrations
echo "Running Sentry migrations..."
sentry upgrade --noinput

# Create a default Sentry user if needed
if [ -n "$SENTRY_ADMIN_EMAIL" ] && [ -n "$SENTRY_ADMIN_PASSWORD" ]; then
  echo "Creating default Sentry admin user..."
  sentry createuser --email $SENTRY_ADMIN_EMAIL --password $SENTRY_ADMIN_PASSWORD --superuser --no-input || true
fi

echo "Sentry migration completed successfully for version $SENTRY_VERSION"
`,
							},
							Env: []corev1.EnvVar{
								{Name: "SENTRY_VERSION", Value: sentryCluster.Spec.Version},
								{Name: "POSTGRES_HOST", Value: fmt.Sprintf("%s-postgres", sentryCluster.Name)},
								{Name: "POSTGRES_PORT", Value: "5432"},
								{Name: "POSTGRES_DB", Value: "sentry"},
								{Name: "POSTGRES_USER", Value: "sentry"},
								{Name: "POSTGRES_PASSWORD", ValueFrom: &corev1.EnvVarSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
										Key:                  "postgres-password",
									},
								}},
								{Name: "REDIS_HOST", Value: fmt.Sprintf("%s-redis", sentryCluster.Name)},
								{Name: "REDIS_PORT", Value: "6379"},
								{Name: "SENTRY_ADMIN_EMAIL", Value: "admin@example.com"},
								{Name: "SENTRY_ADMIN_PASSWORD", Value: "admin"},
								{Name: "SENTRY_SECRET_KEY", ValueFrom: &corev1.EnvVarSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
										Key:                  "sentry-secret-key",
									},
								}},
								{Name: "SNUBA_HOST", Value: fmt.Sprintf("%s-snuba", sentryCluster.Name)},
								{Name: "SNUBA_PORT", Value: "1218"},
								{Name: "CLICKHOUSE_HOST", Value: fmt.Sprintf("%s-clickhouse", sentryCluster.Name)},
								{Name: "CLICKHOUSE_PORT", Value: "9000"},
								{Name: "KAFKA_HOST", Value: fmt.Sprintf("%s-kafka", sentryCluster.Name)},
								{Name: "KAFKA_PORT", Value: "9092"},
							},
						},
					},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(sentryCluster, job, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Sentry migration job")
	}
	return job
}
