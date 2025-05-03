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

// RedisReconciler reconciles Redis for SentryCluster
type RedisReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile handles the Redis deployment for Sentry.
func (r *RedisReconciler) Reconcile(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
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
func (r *RedisReconciler) defineRedisPVC(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.PersistentVolumeClaim {
	labels := GetComponentLabels(sentryCluster, "redis")

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
			Resources: corev1.VolumeResourceRequirements{
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

// defineRedisService creates the desired Service object for Redis.
func (r *RedisReconciler) defineRedisService(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.Service {
	labels := GetComponentLabels(sentryCluster, "redis")

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-redis",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				Port:       RedisPort,
				TargetPort: intstr.FromInt(RedisPort),
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
func (r *RedisReconciler) defineRedisStatefulSet(sentryCluster *sentryv1alpha1.SentryCluster) *appsv1.StatefulSet {
	labels := GetComponentLabels(sentryCluster, "redis")

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
							ContainerPort: RedisPort,
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
