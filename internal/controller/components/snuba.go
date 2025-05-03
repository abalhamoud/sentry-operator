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
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	sentryv1alpha1 "github.com/abalhamoud/sentry-operator/api/v1alpha1"
)

// SnubaReconciler reconciles Snuba for SentryCluster
type SnubaReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile handles the Snuba deployment for Sentry.
func (r *SnubaReconciler) Reconcile(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling Snuba", "SentryCluster", sentryCluster.Name)

	// Check if ClickHouse is enabled, as Snuba requires ClickHouse
	if sentryCluster.Spec.Persistence.ClickHouse == nil {
		log.Info("ClickHouse is not enabled, skipping Snuba")
		return ctrl.Result{}, nil
	}

	// 1. Reconcile ConfigMap for Snuba configuration
	configMapName := sentryCluster.Name + "-snuba-config"
	configMap := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: sentryCluster.Namespace}, configMap)
	if err != nil && apierrors.IsNotFound(err) {
		desiredConfigMap := r.defineSnubaConfigMap(sentryCluster)
		log.Info("Creating a new Snuba ConfigMap", "ConfigMap.Namespace", desiredConfigMap.Namespace, "ConfigMap.Name", desiredConfigMap.Name)
		if err := r.Create(ctx, desiredConfigMap); err != nil {
			log.Error(err, "Failed to create new Snuba ConfigMap", "ConfigMap.Namespace", desiredConfigMap.Namespace, "ConfigMap.Name", desiredConfigMap.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Snuba ConfigMap")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Snuba ConfigMap")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Snuba ConfigMap")
	} else {
		log.V(1).Info("Snuba ConfigMap already exists", "ConfigMap.Namespace", configMap.Namespace, "ConfigMap.Name", configMap.Name)
	}

	// 2. Reconcile Service
	serviceName := sentryCluster.Name + "-snuba"
	service := &corev1.Service{}
	err = r.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: sentryCluster.Namespace}, service)
	if err != nil && apierrors.IsNotFound(err) {
		desiredService := r.defineSnubaService(sentryCluster)
		log.Info("Creating a new Snuba Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
		if err := r.Create(ctx, desiredService); err != nil {
			log.Error(err, "Failed to create new Snuba Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Snuba Service")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Snuba Service")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Snuba Service")
	} else {
		log.V(1).Info("Snuba Service already exists", "Service.Namespace", service.Namespace, "Service.Name", service.Name)
	}

	// 3. Reconcile Deployment
	deploymentName := sentryCluster.Name + "-snuba"
	deployment := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: deploymentName, Namespace: sentryCluster.Namespace}, deployment)
	if err != nil && apierrors.IsNotFound(err) {
		desiredDeployment := r.defineSnubaDeployment(sentryCluster)
		log.Info("Creating a new Snuba Deployment", "Deployment.Namespace", desiredDeployment.Namespace, "Deployment.Name", desiredDeployment.Name)
		if err := r.Create(ctx, desiredDeployment); err != nil {
			log.Error(err, "Failed to create new Snuba Deployment", "Deployment.Namespace", desiredDeployment.Namespace, "Deployment.Name", desiredDeployment.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Snuba Deployment")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Snuba Deployment")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Snuba Deployment")
	} else {
		log.V(1).Info("Snuba Deployment already exists", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
		
		// 4. Check Deployment readiness
		if deployment.Status.ReadyReplicas < *deployment.Spec.Replicas {
			log.Info("Snuba Deployment not yet ready", "ReadyReplicas", deployment.Status.ReadyReplicas, "Replicas", *deployment.Spec.Replicas)
			return ctrl.Result{RequeueAfter: time.Second * 30}, nil
		}
		
		// 5. Update Deployment if needed
		desiredDeployment := r.defineSnubaDeployment(sentryCluster)
		if !reflect.DeepEqual(deployment.Spec, desiredDeployment.Spec) {
			log.Info("Updating existing Snuba Deployment", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
			deployment.Spec = desiredDeployment.Spec
			if err := r.Update(ctx, deployment); err != nil {
				log.Error(err, "Failed to update Snuba Deployment", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
				return ctrl.Result{}, errors.Wrap(err, "failed to update Snuba Deployment")
			}
			return ctrl.Result{Requeue: true}, nil
		}
	}

	log.Info("Snuba reconciled successfully", "SentryCluster", sentryCluster.Name)
	return ctrl.Result{}, nil
}

// defineSnubaConfigMap creates the desired ConfigMap object for Snuba configuration.
func (r *SnubaReconciler) defineSnubaConfigMap(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.ConfigMap {
	labels := GetComponentLabels(sentryCluster, "snuba")
	
	// Basic Snuba configuration
	snubaConfig := `
[snuba]
auto_migrations = true
`

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-snuba-config",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			"config.py": snubaConfig,
		},
	}
	if err := controllerutil.SetControllerReference(sentryCluster, configMap, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Snuba ConfigMap")
	}
	return configMap
}

// defineSnubaService creates the desired Service object for Snuba.
func (r *SnubaReconciler) defineSnubaService(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.Service {
	labels := GetComponentLabels(sentryCluster, "snuba")
	
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-snuba",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Port:       SnubaAPIPort,
					TargetPort: intstr.FromInt(SnubaAPIPort),
					Name:       "api",
				},
			},
			Selector: labels,
			Type:     corev1.ServiceTypeClusterIP,
		},
	}
	if err := controllerutil.SetControllerReference(sentryCluster, svc, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Snuba Service")
	}
	return svc
}

// defineSnubaDeployment creates the desired Deployment object for Snuba.
func (r *SnubaReconciler) defineSnubaDeployment(sentryCluster *sentryv1alpha1.SentryCluster) *appsv1.Deployment {
	labels := GetComponentLabels(sentryCluster, "snuba")
	
	// Set default values
	replicas := int32(1)
	if sentryCluster.Spec.Replica.Snuba > 0 {
		replicas = sentryCluster.Spec.Replica.Snuba
	}
	
	// Get the secret name
	secretName := sentryCluster.Name + "-secret"
	
	// Determine ClickHouse connection details
	var clickhouseHost, clickhousePort, clickhouseUser, clickhousePassword, clickhouseDB string
	
	if sentryCluster.Spec.Persistence.ClickHouse.External != nil {
		// Use external ClickHouse
		clickhouseHost = fmt.Sprintf("$(CLICKHOUSE_HOST)")
		clickhousePort = fmt.Sprintf("$(CLICKHOUSE_PORT)")
		clickhouseUser = fmt.Sprintf("$(CLICKHOUSE_USER)")
		clickhousePassword = fmt.Sprintf("$(CLICKHOUSE_PASSWORD)")
		clickhouseDB = fmt.Sprintf("$(CLICKHOUSE_DATABASE)")
	} else {
		// Use managed ClickHouse
		clickhouseHost = fmt.Sprintf("%s-clickhouse", sentryCluster.Name)
		clickhousePort = fmt.Sprintf("%d", ClickHousePort)
		clickhouseUser = "sentry"
		clickhousePassword = fmt.Sprintf("$(CLICKHOUSE_PASSWORD)")
		clickhouseDB = "sentry"
	}
	
	// Determine Kafka connection details
	var kafkaHost string
	
	if sentryCluster.Spec.Persistence.Kafka != nil && sentryCluster.Spec.Persistence.Kafka.External != nil {
		// Use external Kafka
		kafkaHost = fmt.Sprintf("$(KAFKA_HOST)")
	} else {
		// Use managed Kafka
		kafkaHost = fmt.Sprintf("%s-kafka:%d", sentryCluster.Name, KafkaPort)
	}
	
	// Create the Deployment
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-snuba",
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
						Name:  "snuba",
						Image: fmt.Sprintf("getsentry/snuba:%s", sentryCluster.Spec.Version),
						Ports: []corev1.ContainerPort{
							{
								ContainerPort: SnubaAPIPort,
								Name:          "api",
							},
						},
						Env: []corev1.EnvVar{
							{Name: "CLICKHOUSE_HOST", Value: clickhouseHost},
							{Name: "CLICKHOUSE_PORT", Value: clickhousePort},
							{Name: "CLICKHOUSE_USER", Value: clickhouseUser},
							{Name: "CLICKHOUSE_PASSWORD", ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
									Key:                  "clickhouse-password",
								},
							}},
							{Name: "CLICKHOUSE_DATABASE", Value: clickhouseDB},
							{Name: "KAFKA_BOOTSTRAP_SERVER", Value: kafkaHost},
							{Name: "DEFAULT_BROKERS", Value: kafkaHost},
							{Name: "REDIS_HOST", Value: fmt.Sprintf("%s-redis", sentryCluster.Name)},
							{Name: "REDIS_PORT", Value: fmt.Sprintf("%d", RedisPort)},
							{Name: "REDIS_PASSWORD", ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
									Key:                  "redis-password",
								},
							}},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "config",
								MountPath: "/etc/snuba",
							},
						},
						Resources: sentryCluster.Spec.Resources.Snuba,
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/health",
									Port: intstr.FromInt(SnubaAPIPort),
								},
							},
							InitialDelaySeconds: 10,
							TimeoutSeconds:      5,
							PeriodSeconds:       10,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/health",
									Port: intstr.FromInt(SnubaAPIPort),
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
										Name: sentryCluster.Name + "-snuba-config",
									},
								},
							},
						},
					},
				},
			},
		},
	}
	
	// Add environment variables for external ClickHouse if configured
	if sentryCluster.Spec.Persistence.ClickHouse.External != nil {
		secretName := sentryCluster.Spec.Persistence.ClickHouse.External.SecretName
		
		// Replace the static environment variables with ones from the secret
		for i, env := range deployment.Spec.Template.Spec.Containers[0].Env {
			if env.Name == "CLICKHOUSE_HOST" {
				deployment.Spec.Template.Spec.Containers[0].Env[i].ValueFrom = &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						Key:                  "host",
					},
				}
				deployment.Spec.Template.Spec.Containers[0].Env[i].Value = ""
			} else if env.Name == "CLICKHOUSE_PORT" {
				deployment.Spec.Template.Spec.Containers[0].Env[i].ValueFrom = &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						Key:                  "port",
					},
				}
				deployment.Spec.Template.Spec.Containers[0].Env[i].Value = ""
			} else if env.Name == "CLICKHOUSE_USER" {
				deployment.Spec.Template.Spec.Containers[0].Env[i].ValueFrom = &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						Key:                  "user",
					},
				}
				deployment.Spec.Template.Spec.Containers[0].Env[i].Value = ""
			} else if env.Name == "CLICKHOUSE_PASSWORD" {
				deployment.Spec.Template.Spec.Containers[0].Env[i].ValueFrom = &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						Key:                  "password",
					},
				}
			} else if env.Name == "CLICKHOUSE_DATABASE" {
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
	
	// Add environment variables for external Kafka if configured
	if sentryCluster.Spec.Persistence.Kafka != nil && sentryCluster.Spec.Persistence.Kafka.External != nil {
		secretName := sentryCluster.Spec.Persistence.Kafka.External.SecretName
		
		// Replace the static environment variables with ones from the secret
		for i, env := range deployment.Spec.Template.Spec.Containers[0].Env {
			if env.Name == "KAFKA_BOOTSTRAP_SERVER" || env.Name == "DEFAULT_BROKERS" {
				deployment.Spec.Template.Spec.Containers[0].Env[i].ValueFrom = &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
						Key:                  "bootstrap.servers",
					},
				}
				deployment.Spec.Template.Spec.Containers[0].Env[i].Value = ""
			}
		}
	}
	
	if err := controllerutil.SetControllerReference(sentryCluster, deployment, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Snuba Deployment")
	}
	return deployment
}
