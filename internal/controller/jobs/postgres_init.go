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
	"github.com/abalhamoud/sentry-operator/internal/controller/components"
)

// PostgresInitJobReconciler reconciles the PostgreSQL initialization job
type PostgresInitJobReconciler struct {
	BaseJobReconciler
}

// Reconcile handles the PostgreSQL initialization job
func (r *PostgresInitJobReconciler) Reconcile(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling PostgreSQL initialization job", "SentryCluster", sentryCluster.Name)

	// Skip if using external PostgreSQL
	if sentryCluster.Spec.Persistence.Postgresql.External != nil {
		log.Info("Using external PostgreSQL, skipping initialization job")
		return ctrl.Result{}, nil
	}

	// Check if PostgreSQL is ready
	if !sentryCluster.Status.ComponentStatus.Postgresql.Ready {
		log.Info("PostgreSQL is not ready yet, waiting")
		return ctrl.Result{Requeue: true}, nil
	}

	return r.ReconcileJob(ctx, sentryCluster, r)
}

// GetJobName returns the name of the job
func (r *PostgresInitJobReconciler) GetJobName(sentryCluster *sentryv1alpha1.SentryCluster) string {
	return fmt.Sprintf("%s-postgres-init", sentryCluster.Name)
}

// GetJobConditionType returns the condition type for the job
func (r *PostgresInitJobReconciler) GetJobConditionType() string {
	return "PostgresInitialized"
}

// ShouldRunJob determines if the job should be run
func (r *PostgresInitJobReconciler) ShouldRunJob(sentryCluster *sentryv1alpha1.SentryCluster) bool {
	// Check if the job has already been completed
	if r.IsJobComplete(sentryCluster, r.GetJobConditionType()) {
		return false
	}
	return true
}

// CreateJob creates the job
func (r *PostgresInitJobReconciler) CreateJob(sentryCluster *sentryv1alpha1.SentryCluster) *batchv1.Job {
	labels := GetJobLabels(sentryCluster, "postgres-init")
	
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
							Name:  "postgres-init",
							Image: "postgres:13",
							Command: []string{
								"sh",
								"-c",
								`
# Wait for PostgreSQL to be ready
until PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -U postgres -c '\l'; do
  echo "Waiting for PostgreSQL to be ready..."
  sleep 2
done

# Create the database if it doesn't exist
PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = '$SENTRY_DB_NAME'" | grep -q 1 || \
  PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -U postgres -c "CREATE DATABASE $SENTRY_DB_NAME"

# Create the user if it doesn't exist
PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -U postgres -tc "SELECT 1 FROM pg_roles WHERE rolname = '$SENTRY_DB_USER'" | grep -q 1 || \
  PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -U postgres -c "CREATE USER $SENTRY_DB_USER WITH PASSWORD '$SENTRY_DB_PASSWORD'"

# Grant privileges to the user
PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE $SENTRY_DB_NAME TO $SENTRY_DB_USER"
PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -U postgres -d $SENTRY_DB_NAME -c "ALTER SCHEMA public OWNER TO $SENTRY_DB_USER"

echo "PostgreSQL initialization completed successfully"
`,
							},
							Env: []corev1.EnvVar{
								{Name: "POSTGRES_HOST", Value: fmt.Sprintf("%s-postgres", sentryCluster.Name)},
								{Name: "POSTGRES_PASSWORD", ValueFrom: &corev1.EnvVarSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
										Key:                  "postgres-password",
									},
								}},
								{Name: "SENTRY_DB_NAME", Value: components.PostgresDB},
								{Name: "SENTRY_DB_USER", Value: components.PostgresUser},
								{Name: "SENTRY_DB_PASSWORD", ValueFrom: &corev1.EnvVarSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
										Key:                  "postgres-password",
									},
								}},
							},
						},
					},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(sentryCluster, job, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on PostgreSQL initialization job")
	}
	return job
}
