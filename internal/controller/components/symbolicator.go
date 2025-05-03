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

// SymbolicatorReconciler reconciles Symbolicator for SentryCluster
type SymbolicatorReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile handles the Symbolicator deployment for Sentry.
func (r *SymbolicatorReconciler) Reconcile(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling Symbolicator", "SentryCluster", sentryCluster.Name)
	
	// Check if dependencies are ready
	if !r.areDependenciesReady(sentryCluster) {
		log.Info("Dependencies for Symbolicator are not ready yet, requeuing")
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}

	// 1. Reconcile ConfigMap for Symbolicator configuration
	configMapName := sentryCluster.Name + "-symbolicator-config"
	configMap := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: sentryCluster.Namespace}, configMap)
	if err != nil && apierrors.IsNotFound(err) {
		desiredConfigMap := r.defineSymbolicatorConfigMap(sentryCluster)
		log.Info("Creating a new Symbolicator ConfigMap", "ConfigMap.Namespace", desiredConfigMap.Namespace, "ConfigMap.Name", desiredConfigMap.Name)
		if err := r.Create(ctx, desiredConfigMap); err != nil {
			log.Error(err, "Failed to create new Symbolicator ConfigMap", "ConfigMap.Namespace", desiredConfigMap.Namespace, "ConfigMap.Name", desiredConfigMap.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Symbolicator ConfigMap")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Symbolicator ConfigMap")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Symbolicator ConfigMap")
	} else {
		log.V(1).Info("Symbolicator ConfigMap already exists", "ConfigMap.Namespace", configMap.Namespace, "ConfigMap.Name", configMap.Name)
	}

	// 2. Reconcile Service
	serviceName := sentryCluster.Name + "-symbolicator"
	service := &corev1.Service{}
	err = r.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: sentryCluster.Namespace}, service)
	if err != nil && apierrors.IsNotFound(err) {
		desiredService := r.defineSymbolicatorService(sentryCluster)
		log.Info("Creating a new Symbolicator Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
		if err := r.Create(ctx, desiredService); err != nil {
			log.Error(err, "Failed to create new Symbolicator Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Symbolicator Service")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Symbolicator Service")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Symbolicator Service")
	} else {
		log.V(1).Info("Symbolicator Service already exists", "Service.Namespace", service.Namespace, "Service.Name", service.Name)
	}

	// 3. Reconcile Deployment
	deploymentName := sentryCluster.Name + "-symbolicator"
	deployment := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: deploymentName, Namespace: sentryCluster.Namespace}, deployment)
	if err != nil && apierrors.IsNotFound(err) {
		desiredDeployment := r.defineSymbolicatorDeployment(sentryCluster)
		log.Info("Creating a new Symbolicator Deployment", "Deployment.Namespace", desiredDeployment.Namespace, "Deployment.Name", desiredDeployment.Name)
		if err := r.Create(ctx, desiredDeployment); err != nil {
			log.Error(err, "Failed to create new Symbolicator Deployment", "Deployment.Namespace", desiredDeployment.Namespace, "Deployment.Name", desiredDeployment.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Symbolicator Deployment")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Symbolicator Deployment")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Symbolicator Deployment")
	} else {
		log.V(1).Info("Symbolicator Deployment already exists", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)

		// 4. Check Deployment readiness
		if deployment.Status.ReadyReplicas < *deployment.Spec.Replicas {
			log.Info("Symbolicator Deployment not yet ready", "ReadyReplicas", deployment.Status.ReadyReplicas, "Replicas", *deployment.Spec.Replicas)
			return ctrl.Result{RequeueAfter: time.Second * 30}, nil
		}

		// 5. Update Deployment if needed
		desiredDeployment := r.defineSymbolicatorDeployment(sentryCluster)
		if !reflect.DeepEqual(deployment.Spec, desiredDeployment.Spec) {
			log.Info("Updating existing Symbolicator Deployment", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
			deployment.Spec = desiredDeployment.Spec
			if err := r.Update(ctx, deployment); err != nil {
				log.Error(err, "Failed to update Symbolicator Deployment", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
				return ctrl.Result{}, errors.Wrap(err, "failed to update Symbolicator Deployment")
			}
			return ctrl.Result{Requeue: true}, nil
		}
	}

	log.Info("Symbolicator reconciled successfully", "SentryCluster", sentryCluster.Name)
	return ctrl.Result{}, nil
}

// defineSymbolicatorConfigMap creates the desired ConfigMap object for Symbolicator configuration.
func (r *SymbolicatorReconciler) defineSymbolicatorConfigMap(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.ConfigMap {
	labels := GetComponentLabels(sentryCluster, "symbolicator")

	// Basic Symbolicator configuration
	symbolicatorConfig := `
cache_dir: "/data"
bind: "0.0.0.0:3021"
logging:
  level: "info"
metrics:
  statsd: null
`

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-symbolicator-config",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			"config.yml": symbolicatorConfig,
		},
	}
	if err := controllerutil.SetControllerReference(sentryCluster, configMap, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Symbolicator ConfigMap")
	}
	return configMap
}

// defineSymbolicatorService creates the desired Service object for Symbolicator.
func (r *SymbolicatorReconciler) defineSymbolicatorService(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.Service {
	labels := GetComponentLabels(sentryCluster, "symbolicator")

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-symbolicator",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Port:       SymbolicatorPort,
					TargetPort: intstr.FromInt(SymbolicatorPort),
					Name:       "http",
				},
			},
			Selector: labels,
			Type:     corev1.ServiceTypeClusterIP,
		},
	}
	if err := controllerutil.SetControllerReference(sentryCluster, svc, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Symbolicator Service")
	}
	return svc
}

// defineSymbolicatorDeployment creates the desired Deployment object for Symbolicator.
func (r *SymbolicatorReconciler) defineSymbolicatorDeployment(sentryCluster *sentryv1alpha1.SentryCluster) *appsv1.Deployment {
	labels := GetComponentLabels(sentryCluster, "symbolicator")

	// Set default values
	replicas := int32(1)
	if sentryCluster.Spec.Replica.Symbolicator > 0 {
		replicas = sentryCluster.Spec.Replica.Symbolicator
	}

	// Create the Deployment
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-symbolicator",
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
						Name:  "symbolicator",
						Image: fmt.Sprintf("getsentry/symbolicator:%s", sentryCluster.Spec.Version),
						Ports: []corev1.ContainerPort{
							{
								ContainerPort: SymbolicatorPort,
								Name:          "http",
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "config",
								MountPath: "/etc/symbolicator",
							},
							{
								Name:      "data",
								MountPath: "/data",
							},
						},
						Resources: sentryCluster.Spec.Resources.Symbolicator,
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/healthcheck",
									Port: intstr.FromInt(SymbolicatorPort),
								},
							},
							InitialDelaySeconds: 10,
							TimeoutSeconds:      5,
							PeriodSeconds:       10,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/healthcheck",
									Port: intstr.FromInt(SymbolicatorPort),
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
										Name: sentryCluster.Name + "-symbolicator-config",
									},
								},
							},
						},
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(sentryCluster, deployment, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Symbolicator Deployment")
	}
	return deployment
}

// areDependenciesReady checks if all dependencies for Symbolicator are ready
func (r *SymbolicatorReconciler) areDependenciesReady(sentryCluster *sentryv1alpha1.SentryCluster) bool {
	// Symbolicator primarily depends on Redis for caching
	if !sentryCluster.Status.ComponentStatus.Redis.Ready {
		return false
	}
	
	return true
}
