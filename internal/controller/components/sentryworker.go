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
	"reflect"
	"time"

	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
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

// SentryWorkerReconciler reconciles SentryWorker for SentryCluster
type SentryWorkerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile handles the Sentry Worker deployment for Sentry.
func (r *SentryWorkerReconciler) Reconcile(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling Sentry Worker", "SentryCluster", sentryCluster.Name)
	
	// Check if dependencies are ready
	if !r.areDependenciesReady(sentryCluster) {
		log.Info("Dependencies for Sentry Worker are not ready yet, requeuing")
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}

	// Reconcile Deployment
	deploymentName := sentryCluster.Name + "-worker"
	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: deploymentName, Namespace: sentryCluster.Namespace}, deployment)
	if err != nil && apierrors.IsNotFound(err) {
		desiredDeployment := r.defineSentryWorkerDeployment(sentryCluster)
		log.Info("Creating a new Sentry Worker Deployment", "Deployment.Namespace", desiredDeployment.Namespace, "Deployment.Name", desiredDeployment.Name)
		if err := r.Create(ctx, desiredDeployment); err != nil {
			log.Error(err, "Failed to create new Sentry Worker Deployment", "Deployment.Namespace", desiredDeployment.Namespace, "Deployment.Name", desiredDeployment.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Sentry Worker Deployment")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Sentry Worker Deployment")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Sentry Worker Deployment")
	} else {
		log.V(1).Info("Sentry Worker Deployment already exists", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)

		// Check Deployment readiness
		if deployment.Status.ReadyReplicas < *deployment.Spec.Replicas {
			log.Info("Sentry Worker Deployment not yet ready", "ReadyReplicas", deployment.Status.ReadyReplicas, "Replicas", *deployment.Spec.Replicas)
			return ctrl.Result{RequeueAfter: time.Second * 30}, nil
		}

		// Update Deployment if needed
		desiredDeployment := r.defineSentryWorkerDeployment(sentryCluster)
		if !reflect.DeepEqual(deployment.Spec, desiredDeployment.Spec) {
			log.Info("Updating existing Sentry Worker Deployment", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
			deployment.Spec = desiredDeployment.Spec
			if err := r.Update(ctx, deployment); err != nil {
				log.Error(err, "Failed to update Sentry Worker Deployment", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
				return ctrl.Result{}, errors.Wrap(err, "failed to update Sentry Worker Deployment")
			}
			return ctrl.Result{Requeue: true}, nil
		}
	}

	log.Info("Sentry Worker reconciled successfully", "SentryCluster", sentryCluster.Name)
	return ctrl.Result{}, nil
}

// defineSentryWorkerDeployment creates the desired Deployment object for Sentry Worker.
func (r *SentryWorkerReconciler) defineSentryWorkerDeployment(sentryCluster *sentryv1alpha1.SentryCluster) *appsv1.Deployment {
	labels := GetComponentLabels(sentryCluster, "worker")

	// Set default values
	replicas := int32(1)
	if sentryCluster.Spec.Replica.Worker > 0 {
		replicas = sentryCluster.Spec.Replica.Worker
	}

	// Get the secret name
	secretName := sentryCluster.Name + "-secret"

	// Create the Deployment
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-worker",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:    "sentry-worker",
						Image:   fmt.Sprintf("getsentry/sentry:%s", sentryCluster.Spec.Version),
						Command: []string{"sentry", "run", "worker"},
						Env: []corev1.EnvVar{
							{Name: "SENTRY_SECRET_KEY", ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
									Key:                  "sentry-secret-key",
								},
							}},
							{Name: "SENTRY_POSTGRES_HOST", Value: fmt.Sprintf("%s-postgres", sentryCluster.Name)},
							{Name: "SENTRY_POSTGRES_PORT", Value: fmt.Sprintf("%d", PostgresPort)},
							{Name: "SENTRY_DB_USER", Value: PostgresUser},
							{Name: "SENTRY_DB_NAME", Value: PostgresDB},
							{Name: "SENTRY_DB_PASSWORD", ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
									Key:                  "postgres-password",
								},
							}},
							{Name: "SENTRY_REDIS_HOST", Value: fmt.Sprintf("%s-redis", sentryCluster.Name)},
							{Name: "SENTRY_REDIS_PORT", Value: fmt.Sprintf("%d", RedisPort)},
							{Name: "SENTRY_REDIS_PASSWORD", ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
									Key:                  "redis-password",
								},
							}},
							{Name: "SENTRY_KAFKA_HOST", Value: fmt.Sprintf("%s-kafka", sentryCluster.Name)},
							{Name: "SENTRY_KAFKA_PORT", Value: fmt.Sprintf("%d", KafkaPort)},
							{Name: "SENTRY_SNUBA_HOST", Value: fmt.Sprintf("%s-snuba", sentryCluster.Name)},
							{Name: "SENTRY_SNUBA_PORT", Value: fmt.Sprintf("%d", SnubaAPIPort)},
							{Name: "SENTRY_RELAY_HOST", Value: fmt.Sprintf("%s-relay", sentryCluster.Name)},
							{Name: "SENTRY_RELAY_PORT", Value: fmt.Sprintf("%d", RelayPort)},
							{Name: "SENTRY_SYMBOLICATOR_HOST", Value: fmt.Sprintf("%s-symbolicator", sentryCluster.Name)},
							{Name: "SENTRY_SYMBOLICATOR_PORT", Value: fmt.Sprintf("%d", SymbolicatorPort)},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "config",
								MountPath: "/etc/sentry",
							},
						},
						Resources: sentryCluster.Spec.Resources.Worker,
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{
										"sh",
										"-c",
										"ps aux | grep -v grep | grep 'sentry run worker'",
									},
								},
							},
							InitialDelaySeconds: 30,
							TimeoutSeconds:      5,
							PeriodSeconds:       15,
						},
					}},
					Volumes: []corev1.Volume{
						{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: sentryCluster.Name + "-config",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Add environment variables for external PostgreSQL if configured
	if sentryCluster.Spec.Persistence.Postgresql.External != nil {
		secretName := sentryCluster.Spec.Persistence.Postgresql.External.SecretName

		// Replace the static environment variables with ones from the secret
		for i, env := range deployment.Spec.Template.Spec.Containers[0].Env {
			if env.Name == "SENTRY_POSTGRES_HOST" {
				deployment.Spec.Template.Spec.Containers[0].Env[i].ValueFrom = &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						Key:                  "host",
					},
				}
				deployment.Spec.Template.Spec.Containers[0].Env[i].Value = ""
			} else if env.Name == "SENTRY_POSTGRES_PORT" {
				deployment.Spec.Template.Spec.Containers[0].Env[i].ValueFrom = &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						Key:                  "port",
					},
				}
				deployment.Spec.Template.Spec.Containers[0].Env[i].Value = ""
			} else if env.Name == "SENTRY_DB_USER" {
				deployment.Spec.Template.Spec.Containers[0].Env[i].ValueFrom = &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						Key:                  "user",
					},
				}
				deployment.Spec.Template.Spec.Containers[0].Env[i].Value = ""
			} else if env.Name == "SENTRY_DB_PASSWORD" {
				deployment.Spec.Template.Spec.Containers[0].Env[i].ValueFrom = &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						Key:                  "password",
					},
				}
			} else if env.Name == "SENTRY_DB_NAME" {
				deployment.Spec.Template.Spec.Containers[0].Env[i].ValueFrom = &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						Key:                  "database",
					},
				}
				deployment.Spec.Template.Spec.Containers[0].Env[i].Value = ""
			}
		}
	}

	// Add environment variables for external Redis if configured
	if sentryCluster.Spec.Persistence.Redis.External != nil {
		secretName := sentryCluster.Spec.Persistence.Redis.External.SecretName

		// Replace the static environment variables with ones from the secret
		for i, env := range deployment.Spec.Template.Spec.Containers[0].Env {
			if env.Name == "SENTRY_REDIS_HOST" {
				deployment.Spec.Template.Spec.Containers[0].Env[i].ValueFrom = &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						Key:                  "host",
					},
				}
				deployment.Spec.Template.Spec.Containers[0].Env[i].Value = ""
			} else if env.Name == "SENTRY_REDIS_PORT" {
				deployment.Spec.Template.Spec.Containers[0].Env[i].ValueFrom = &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						Key:                  "port",
					},
				}
				deployment.Spec.Template.Spec.Containers[0].Env[i].Value = ""
			} else if env.Name == "SENTRY_REDIS_PASSWORD" {
				deployment.Spec.Template.Spec.Containers[0].Env[i].ValueFrom = &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						Key:                  "password",
					},
				}
			}
		}
	}

	// Add environment variables for external Kafka if configured
	if sentryCluster.Spec.Persistence.Kafka != nil && sentryCluster.Spec.Persistence.Kafka.External != nil {
		secretName := sentryCluster.Spec.Persistence.Kafka.External.SecretName

		// Replace the static environment variables with ones from the secret
		for i, env := range deployment.Spec.Template.Spec.Containers[0].Env {
			if env.Name == "SENTRY_KAFKA_HOST" {
				deployment.Spec.Template.Spec.Containers[0].Env[i].ValueFrom = &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						Key:                  "host",
					},
				}
				deployment.Spec.Template.Spec.Containers[0].Env[i].Value = ""
			} else if env.Name == "SENTRY_KAFKA_PORT" {
				deployment.Spec.Template.Spec.Containers[0].Env[i].ValueFrom = &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						Key:                  "port",
					},
				}
				deployment.Spec.Template.Spec.Containers[0].Env[i].Value = ""
			}
		}
	}

	if err := controllerutil.SetControllerReference(sentryCluster, deployment, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Sentry Worker Deployment")
	}
	return deployment
}

// areDependenciesReady checks if all dependencies for Sentry Worker are ready
func (r *SentryWorkerReconciler) areDependenciesReady(sentryCluster *sentryv1alpha1.SentryCluster) bool {
	// Check if Postgres is ready
	if !sentryCluster.Status.ComponentStatus.Postgresql.Ready {
		return false
	}
	
	// Check if Redis is ready
	if !sentryCluster.Status.ComponentStatus.Redis.Ready {
		return false
	}
	
	// Check if Kafka is ready (if used)
	if sentryCluster.Spec.Persistence.Kafka != nil && !sentryCluster.Status.ComponentStatus.Kafka.Ready {
		return false
	}
	
	// Check if Snuba is ready (if ClickHouse is configured)
	if sentryCluster.Spec.Persistence.ClickHouse != nil && !sentryCluster.Status.ComponentStatus.Snuba.Ready {
		return false
	}
	
	// Check if Sentry migration job has completed
	condition := sentryCluster.Status.GetCondition("SentryMigrated")
	if condition == nil || condition.Status != metav1.ConditionTrue {
		return false
	}
	
	return true
}
