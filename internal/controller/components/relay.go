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

// RelayReconciler reconciles Relay for SentryCluster
type RelayReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile handles the Relay deployment for Sentry.
func (r *RelayReconciler) Reconcile(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling Relay", "SentryCluster", sentryCluster.Name)

	// 1. Reconcile ConfigMap for Relay configuration
	configMapName := sentryCluster.Name + "-relay-config"
	configMap := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: sentryCluster.Namespace}, configMap)
	if err != nil && apierrors.IsNotFound(err) {
		desiredConfigMap := r.defineRelayConfigMap(sentryCluster)
		log.Info("Creating a new Relay ConfigMap", "ConfigMap.Namespace", desiredConfigMap.Namespace, "ConfigMap.Name", desiredConfigMap.Name)
		if err := r.Create(ctx, desiredConfigMap); err != nil {
			log.Error(err, "Failed to create new Relay ConfigMap", "ConfigMap.Namespace", desiredConfigMap.Namespace, "ConfigMap.Name", desiredConfigMap.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Relay ConfigMap")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Relay ConfigMap")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Relay ConfigMap")
	} else {
		log.V(1).Info("Relay ConfigMap already exists", "ConfigMap.Namespace", configMap.Namespace, "ConfigMap.Name", configMap.Name)
	}

	// 2. Reconcile Service
	serviceName := sentryCluster.Name + "-relay"
	service := &corev1.Service{}
	err = r.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: sentryCluster.Namespace}, service)
	if err != nil && apierrors.IsNotFound(err) {
		desiredService := r.defineRelayService(sentryCluster)
		log.Info("Creating a new Relay Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
		if err := r.Create(ctx, desiredService); err != nil {
			log.Error(err, "Failed to create new Relay Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Relay Service")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Relay Service")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Relay Service")
	} else {
		log.V(1).Info("Relay Service already exists", "Service.Namespace", service.Namespace, "Service.Name", service.Name)
	}

	// 3. Reconcile Deployment
	deploymentName := sentryCluster.Name + "-relay"
	deployment := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: deploymentName, Namespace: sentryCluster.Namespace}, deployment)
	if err != nil && apierrors.IsNotFound(err) {
		desiredDeployment := r.defineRelayDeployment(sentryCluster)
		log.Info("Creating a new Relay Deployment", "Deployment.Namespace", desiredDeployment.Namespace, "Deployment.Name", desiredDeployment.Name)
		if err := r.Create(ctx, desiredDeployment); err != nil {
			log.Error(err, "Failed to create new Relay Deployment", "Deployment.Namespace", desiredDeployment.Namespace, "Deployment.Name", desiredDeployment.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Relay Deployment")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Relay Deployment")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Relay Deployment")
	} else {
		log.V(1).Info("Relay Deployment already exists", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
		
		// 4. Check Deployment readiness
		if deployment.Status.ReadyReplicas < *deployment.Spec.Replicas {
			log.Info("Relay Deployment not yet ready", "ReadyReplicas", deployment.Status.ReadyReplicas, "Replicas", *deployment.Spec.Replicas)
			return ctrl.Result{RequeueAfter: time.Second * 30}, nil
		}
		
		// 5. Update Deployment if needed
		desiredDeployment := r.defineRelayDeployment(sentryCluster)
		if !reflect.DeepEqual(deployment.Spec, desiredDeployment.Spec) {
			log.Info("Updating existing Relay Deployment", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
			deployment.Spec = desiredDeployment.Spec
			if err := r.Update(ctx, deployment); err != nil {
				log.Error(err, "Failed to update Relay Deployment", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
				return ctrl.Result{}, errors.Wrap(err, "failed to update Relay Deployment")
			}
			return ctrl.Result{Requeue: true}, nil
		}
	}

	log.Info("Relay reconciled successfully", "SentryCluster", sentryCluster.Name)
	return ctrl.Result{}, nil
}

// defineRelayConfigMap creates the desired ConfigMap object for Relay configuration.
func (r *RelayReconciler) defineRelayConfigMap(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.ConfigMap {
	labels := GetComponentLabels(sentryCluster, "relay")
	
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
	
	// Basic Relay configuration
	relayConfig := fmt.Sprintf(`
[relay]
upstream.dsn = "%s/"
mode = "managed"
processing.enabled = true
`, urlPrefix)

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-relay-config",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			"config.yml": relayConfig,
		},
	}
	if err := controllerutil.SetControllerReference(sentryCluster, configMap, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Relay ConfigMap")
	}
	return configMap
}

// defineRelayService creates the desired Service object for Relay.
func (r *RelayReconciler) defineRelayService(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.Service {
	labels := GetComponentLabels(sentryCluster, "relay")
	
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-relay",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Port:       RelayPort,
					TargetPort: intstr.FromInt(RelayPort),
					Name:       "http",
				},
			},
			Selector: labels,
			Type:     corev1.ServiceTypeClusterIP,
		},
	}
	if err := controllerutil.SetControllerReference(sentryCluster, svc, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Relay Service")
	}
	return svc
}

// defineRelayDeployment creates the desired Deployment object for Relay.
func (r *RelayReconciler) defineRelayDeployment(sentryCluster *sentryv1alpha1.SentryCluster) *appsv1.Deployment {
	labels := GetComponentLabels(sentryCluster, "relay")
	
	// Set default values
	replicas := int32(1)
	if sentryCluster.Spec.Replica.Relay > 0 {
		replicas = sentryCluster.Spec.Replica.Relay
	}
	
	// Get the secret name
	secretName := sentryCluster.Name + "-secret"
	
	// Create the Deployment
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-relay",
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
						Name:  "relay",
						Image: fmt.Sprintf("getsentry/relay:%s", sentryCluster.Spec.Version),
						Ports: []corev1.ContainerPort{
							{
								ContainerPort: RelayPort,
								Name:          "http",
							},
						},
						Env: []corev1.EnvVar{
							{Name: "RELAY_SECRET_KEY", ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
									Key:                  "sentry-secret-key",
								},
							}},
							{Name: "RELAY_KAFKA_BROKERS", Value: fmt.Sprintf("%s-kafka:%d", sentryCluster.Name, KafkaPort)},
							{Name: "RELAY_REDIS_URL", Value: fmt.Sprintf("redis://:%s@%s-redis:%d", "$(REDIS_PASSWORD)", sentryCluster.Name, RedisPort)},
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
								MountPath: "/etc/relay",
							},
						},
						Resources: sentryCluster.Spec.Resources.Relay,
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/api/relay/healthcheck/ready/",
									Port: intstr.FromInt(RelayPort),
								},
							},
							InitialDelaySeconds: 10,
							TimeoutSeconds:      5,
							PeriodSeconds:       10,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/api/relay/healthcheck/live/",
									Port: intstr.FromInt(RelayPort),
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
										Name: sentryCluster.Name + "-relay-config",
									},
								},
							},
						},
					},
				},
			},
		},
	}
	
	// Add environment variables for external Kafka if configured
	if sentryCluster.Spec.Persistence.Kafka != nil && sentryCluster.Spec.Persistence.Kafka.External != nil {
		secretName := sentryCluster.Spec.Persistence.Kafka.External.SecretName
		
		// Replace the static environment variables with ones from the secret
		for i, env := range deployment.Spec.Template.Spec.Containers[0].Env {
			if env.Name == "RELAY_KAFKA_BROKERS" {
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
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Relay Deployment")
	}
	return deployment
}
