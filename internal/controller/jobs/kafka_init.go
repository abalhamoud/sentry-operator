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

// KafkaInitJobReconciler reconciles the Kafka initialization job
type KafkaInitJobReconciler struct {
	BaseJobReconciler
}

// Reconcile handles the Kafka initialization job
func (r *KafkaInitJobReconciler) Reconcile(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling Kafka initialization job", "SentryCluster", sentryCluster.Name)

	// Skip if using external Kafka with pre-created topics
	if sentryCluster.Spec.Persistence.Kafka != nil && sentryCluster.Spec.Persistence.Kafka.External != nil {
		log.Info("Using external Kafka, skipping initialization job")
		return ctrl.Result{}, nil
	}

	// Check if Kafka is ready
	if !sentryCluster.Status.ComponentStatus.Kafka.Ready {
		log.Info("Kafka is not ready yet, waiting")
		return ctrl.Result{Requeue: true}, nil
	}

	return r.ReconcileJob(ctx, sentryCluster, r)
}

// GetJobName returns the name of the job
func (r *KafkaInitJobReconciler) GetJobName(sentryCluster *sentryv1alpha1.SentryCluster) string {
	return fmt.Sprintf("%s-kafka-init", sentryCluster.Name)
}

// GetJobConditionType returns the condition type for the job
func (r *KafkaInitJobReconciler) GetJobConditionType() string {
	return "KafkaInitialized"
}

// ShouldRunJob determines if the job should be run
func (r *KafkaInitJobReconciler) ShouldRunJob(sentryCluster *sentryv1alpha1.SentryCluster) bool {
	// Check if the job has already been completed
	if r.IsJobComplete(sentryCluster, r.GetJobConditionType()) {
		return false
	}
	return true
}

// CreateJob creates the job
func (r *KafkaInitJobReconciler) CreateJob(sentryCluster *sentryv1alpha1.SentryCluster) *batchv1.Job {
	labels := GetJobLabels(sentryCluster, "kafka-init")
	
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
							Name:  "kafka-init",
							Image: "bitnami/kafka:3.3.2",
							Command: []string{
								"sh",
								"-c",
								`
# Wait for Kafka to be ready
until kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --list; do
  echo "Waiting for Kafka to be ready..."
  sleep 5
done

# Create required topics
echo "Creating Sentry Kafka topics..."

# Ingest topics
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --create --if-not-exists --topic ingest-events --partitions 8 --replication-factor 1
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --create --if-not-exists --topic ingest-attachments --partitions 8 --replication-factor 1
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --create --if-not-exists --topic ingest-transactions --partitions 8 --replication-factor 1

# Snuba topics
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --create --if-not-exists --topic events --partitions 8 --replication-factor 1
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --create --if-not-exists --topic transactions --partitions 8 --replication-factor 1
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --create --if-not-exists --topic outcomes --partitions 8 --replication-factor 1
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --create --if-not-exists --topic sessions --partitions 8 --replication-factor 1
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --create --if-not-exists --topic metrics --partitions 8 --replication-factor 1
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --create --if-not-exists --topic snuba-commit-log --partitions 8 --replication-factor 1

# Verify topics were created
echo "Verifying topics..."
kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVER --list

echo "Kafka initialization completed successfully"
`,
							},
							Env: []corev1.EnvVar{
								{Name: "KAFKA_BOOTSTRAP_SERVER", Value: fmt.Sprintf("%s-kafka:9092", sentryCluster.Name)},
							},
						},
					},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(sentryCluster, job, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Kafka initialization job")
	}
	return job
}
