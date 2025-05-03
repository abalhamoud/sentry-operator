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

// KafkaReconciler reconciles Kafka for SentryCluster
type KafkaReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile handles the Kafka deployment for Sentry.
func (r *KafkaReconciler) Reconcile(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling Kafka", "SentryCluster", sentryCluster.Name)

	// Check if Kafka is enabled
	if sentryCluster.Spec.Persistence.Kafka == nil {
		log.Info("Kafka is not enabled, skipping")
		return ctrl.Result{}, nil
	}

	kafkaPersistence := sentryCluster.Spec.Persistence.Kafka

	// Check if external Kafka is configured
	if kafkaPersistence.External != nil {
		// Handle external Kafka
		secretName := kafkaPersistence.External.SecretName
		log.Info("Using external Kafka", "SecretName", secretName)

		// Fetch the Secret containing the connection details
		secret := &corev1.Secret{}
		err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: sentryCluster.Namespace}, secret)
		if err != nil {
			log.Error(err, "Failed to get external Kafka Secret", "SecretName", secretName)
			return ctrl.Result{}, errors.Wrap(err, "failed to get external Kafka Secret")
		}

		// Validate the Secret data
		bootstrapServers := string(secret.Data["bootstrap.servers"])
		if bootstrapServers == "" {
			err := fmt.Errorf("required key 'bootstrap.servers' not found in Secret '%s'", secretName)
			log.Error(err, "Invalid external Kafka Secret")
			return ctrl.Result{}, err
		}

		log.Info("Successfully validated external Kafka connection details", "BootstrapServers", bootstrapServers)
		// Skip creating managed resources
		log.Info("Skipping managed Kafka resources because external Kafka is configured")
		return ctrl.Result{}, nil
	}

	// --- Assuming Managed Kafka ---

	// 1. Reconcile PVC
	if kafkaPersistence.Managed != nil && kafkaPersistence.Managed.Storage.Size != "" {
		pvcName := sentryCluster.Name + "-kafka-pvc"
		pvc := &corev1.PersistentVolumeClaim{}
		err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: sentryCluster.Namespace}, pvc)
		if err != nil && apierrors.IsNotFound(err) {
			desiredPVC := r.defineKafkaPVC(sentryCluster)
			log.Info("Creating a new Kafka PVC", "PVC.Namespace", desiredPVC.Namespace, "PVC.Name", desiredPVC.Name)
			if err := r.Create(ctx, desiredPVC); err != nil {
				log.Error(err, "Failed to create new Kafka PVC", "PVC.Namespace", desiredPVC.Namespace, "PVC.Name", desiredPVC.Name)
				return ctrl.Result{}, errors.Wrap(err, "failed to create Kafka PVC")
			}
			return ctrl.Result{Requeue: true}, nil
		} else if err != nil {
			log.Error(err, "Failed to get Kafka PVC")
			return ctrl.Result{}, errors.Wrap(err, "failed to get Kafka PVC")
		} else {
			log.V(1).Info("Kafka PVC already exists", "PVC.Namespace", pvc.Namespace, "PVC.Name", pvc.Name)
		}
	}

	// 2. Reconcile Service
	serviceName := sentryCluster.Name + "-kafka"
	service := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: sentryCluster.Namespace}, service)
	if err != nil && apierrors.IsNotFound(err) {
		desiredService := r.defineKafkaService(sentryCluster)
		log.Info("Creating a new Kafka Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
		if err := r.Create(ctx, desiredService); err != nil {
			log.Error(err, "Failed to create new Kafka Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Kafka Service")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Kafka Service")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Kafka Service")
	} else {
		log.V(1).Info("Kafka Service already exists", "Service.Namespace", service.Namespace, "Service.Name", service.Name)
	}

	// 3. Reconcile StatefulSet
	stsName := sentryCluster.Name + "-kafka"
	sts := &appsv1.StatefulSet{}
	err = r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: sentryCluster.Namespace}, sts)
	if err != nil && apierrors.IsNotFound(err) {
		desiredSts := r.defineKafkaStatefulSet(sentryCluster)
		log.Info("Creating a new Kafka StatefulSet", "StatefulSet.Namespace", desiredSts.Namespace, "StatefulSet.Name", desiredSts.Name)
		if err := r.Create(ctx, desiredSts); err != nil {
			log.Error(err, "Failed to create new Kafka StatefulSet", "StatefulSet.Namespace", desiredSts.Namespace, "StatefulSet.Name", desiredSts.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Kafka StatefulSet")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Kafka StatefulSet")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Kafka StatefulSet")
	} else {
		log.V(1).Info("Kafka StatefulSet already exists", "StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)

		// 4. Check StatefulSet readiness
		if sts.Status.ReadyReplicas < *sts.Spec.Replicas {
			log.Info("Kafka StatefulSet not yet ready", "ReadyReplicas", sts.Status.ReadyReplicas, "Replicas", *sts.Spec.Replicas)
			return ctrl.Result{RequeueAfter: time.Second * 30}, nil
		}

		// 5. Update StatefulSet if needed
		desiredSts := r.defineKafkaStatefulSet(sentryCluster)
		if !reflect.DeepEqual(sts.Spec, desiredSts.Spec) {
			log.Info("Updating existing Kafka StatefulSet", "StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
			sts.Spec = desiredSts.Spec
			if err := r.Update(ctx, sts); err != nil {
				log.Error(err, "Failed to update Kafka StatefulSet", "StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
				return ctrl.Result{}, errors.Wrap(err, "failed to update Kafka StatefulSet")
			}
			return ctrl.Result{Requeue: true}, nil
		}
	}

	log.Info("Kafka reconciled successfully", "SentryCluster", sentryCluster.Name)
	return ctrl.Result{}, nil
}

// defineKafkaPVC creates the desired PersistentVolumeClaim object for Kafka.
func (r *KafkaReconciler) defineKafkaPVC(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.PersistentVolumeClaim {
	labels := GetComponentLabels(sentryCluster, "kafka")

	// Parse the storage size
	storageSize := resource.MustParse(sentryCluster.Spec.Persistence.Kafka.Managed.Storage.Size)

	// Create the PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-kafka-pvc",
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
			StorageClassName: &sentryCluster.Spec.Persistence.Kafka.Managed.Storage.StorageClass,
		},
	}

	// If StorageClass is empty, set it to nil to use the default StorageClass
	if sentryCluster.Spec.Persistence.Kafka.Managed.Storage.StorageClass == "" {
		pvc.Spec.StorageClassName = nil
	}

	if err := controllerutil.SetControllerReference(sentryCluster, pvc, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Kafka PVC")
	}
	return pvc
}

// defineKafkaService creates the desired Service object for Kafka.
func (r *KafkaReconciler) defineKafkaService(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.Service {
	labels := GetComponentLabels(sentryCluster, "kafka")

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-kafka",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				Port:       KafkaPort,
				TargetPort: intstr.FromInt(KafkaPort),
				Name:       "kafka",
			}},
			Selector: labels,
			Type:     corev1.ServiceTypeClusterIP,
		},
	}
	if err := controllerutil.SetControllerReference(sentryCluster, svc, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Kafka Service")
	}
	return svc
}

// defineKafkaStatefulSet creates the desired StatefulSet object for Kafka.
func (r *KafkaReconciler) defineKafkaStatefulSet(sentryCluster *sentryv1alpha1.SentryCluster) *appsv1.StatefulSet {
	labels := GetComponentLabels(sentryCluster, "kafka")

	// Set default values
	replicas := int32(1) // Kafka is typically deployed as a single instance in this setup

	// Create the StatefulSet
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-kafka",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: sentryCluster.Name + "-kafka",
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "kafka",
						Image: "bitnami/kafka:3.3.1",
						Ports: []corev1.ContainerPort{{
							ContainerPort: KafkaPort,
							Name:          "kafka",
						}},
						Env: []corev1.EnvVar{
							{Name: "KAFKA_CFG_ZOOKEEPER_CONNECT", Value: sentryCluster.Name + "-zookeeper:2181"},
							{Name: "KAFKA_CFG_ADVERTISED_LISTENERS", Value: fmt.Sprintf("PLAINTEXT://%s-kafka:%d", sentryCluster.Name, KafkaPort)},
							{Name: "KAFKA_CFG_LISTENERS", Value: fmt.Sprintf("PLAINTEXT://:%d", KafkaPort)},
							{Name: "ALLOW_PLAINTEXT_LISTENER", Value: "yes"},
							{Name: "KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE", Value: "true"},
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "data",
							MountPath: "/bitnami/kafka",
						}},
						Resources: sentryCluster.Spec.Resources.Kafka,
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intstr.FromInt(KafkaPort),
								},
							},
							InitialDelaySeconds: 10,
							TimeoutSeconds:      5,
							PeriodSeconds:       10,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intstr.FromInt(KafkaPort),
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
								ClaimName: sentryCluster.Name + "-kafka-pvc",
							},
						},
					}},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(sentryCluster, sts, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Kafka StatefulSet")
	}
	return sts
}
