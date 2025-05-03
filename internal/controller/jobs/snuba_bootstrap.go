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

// SnubaBootstrapJobReconciler reconciles the Snuba bootstrap job
type SnubaBootstrapJobReconciler struct {
	BaseJobReconciler
}

// Reconcile handles the Snuba bootstrap job
func (r *SnubaBootstrapJobReconciler) Reconcile(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling Snuba bootstrap job", "SentryCluster", sentryCluster.Name)

	// Skip if using external ClickHouse with pre-created tables
	if sentryCluster.Spec.Persistence.ClickHouse != nil && sentryCluster.Spec.Persistence.ClickHouse.External != nil {
		log.Info("Using external ClickHouse, skipping bootstrap job")
		return ctrl.Result{}, nil
	}

	// Check if ClickHouse and Kafka are ready
	if !sentryCluster.Status.ComponentStatus.ClickHouse.Ready {
		log.Info("ClickHouse is not ready yet, waiting")
		return ctrl.Result{Requeue: true}, nil
	}

	if !sentryCluster.Status.ComponentStatus.Kafka.Ready {
		log.Info("Kafka is not ready yet, waiting")
		return ctrl.Result{Requeue: true}, nil
	}

	// Check if Kafka initialization is complete
	if !r.IsJobComplete(sentryCluster, "KafkaInitialized") {
		log.Info("Kafka initialization is not complete yet, waiting")
		return ctrl.Result{Requeue: true}, nil
	}

	return r.ReconcileJob(ctx, sentryCluster, r)
}

// GetJobName returns the name of the job
func (r *SnubaBootstrapJobReconciler) GetJobName(sentryCluster *sentryv1alpha1.SentryCluster) string {
	return fmt.Sprintf("%s-snuba-bootstrap", sentryCluster.Name)
}

// GetJobConditionType returns the condition type for the job
func (r *SnubaBootstrapJobReconciler) GetJobConditionType() string {
	return "SnubaBootstrapped"
}

// ShouldRunJob determines if the job should be run
func (r *SnubaBootstrapJobReconciler) ShouldRunJob(sentryCluster *sentryv1alpha1.SentryCluster) bool {
	// Check if the job has already been completed
	if r.IsJobComplete(sentryCluster, r.GetJobConditionType()) {
		return false
	}
	return true
}

// CreateJob creates the job
func (r *SnubaBootstrapJobReconciler) CreateJob(sentryCluster *sentryv1alpha1.SentryCluster) *batchv1.Job {
	labels := GetJobLabels(sentryCluster, "snuba-bootstrap")
	
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
							Name:  "snuba-bootstrap",
							Image: fmt.Sprintf("getsentry/snuba:%s", sentryCluster.Spec.Version),
							Command: []string{
								"bash",
								"-c",
								`
# Wait for ClickHouse to be ready
until clickhouse-client --host $CLICKHOUSE_HOST --port $CLICKHOUSE_PORT --user $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD -q "SELECT 1"; do
  echo "Waiting for ClickHouse to be ready..."
  sleep 5
done

# Wait for Kafka to be ready
until python -c "from confluent_kafka import Producer; Producer({'bootstrap.servers': '$KAFKA_BOOTSTRAP_SERVER'}).flush()"; do
  echo "Waiting for Kafka to be ready..."
  sleep 5
done

# Bootstrap Snuba
echo "Bootstrapping Snuba..."
snuba bootstrap --force

# Verify tables were created
echo "Verifying ClickHouse tables..."
clickhouse-client --host $CLICKHOUSE_HOST --port $CLICKHOUSE_PORT --user $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD -q "SHOW TABLES FROM sentry"

echo "Snuba bootstrap completed successfully"
`,
							},
							Env: []corev1.EnvVar{
								{Name: "CLICKHOUSE_HOST", Value: fmt.Sprintf("%s-clickhouse", sentryCluster.Name)},
								{Name: "CLICKHOUSE_PORT", Value: "9000"},
								{Name: "CLICKHOUSE_USER", Value: "default"},
								{Name: "CLICKHOUSE_PASSWORD", ValueFrom: &corev1.EnvVarSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
										Key:                  "clickhouse-password",
									},
								}},
								{Name: "KAFKA_BOOTSTRAP_SERVER", Value: fmt.Sprintf("%s-kafka:9092", sentryCluster.Name)},
								{Name: "SNUBA_SETTINGS", Value: "docker"},
								{Name: "CLICKHOUSE_DATABASE", Value: "sentry"},
								{Name: "DEFAULT_BROKERS", Value: fmt.Sprintf("%s-kafka:9092", sentryCluster.Name)},
							},
						},
					},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(sentryCluster, job, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Snuba bootstrap job")
	}
	return job
}
