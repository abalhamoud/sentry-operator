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

// PostgresReconciler reconciles PostgreSQL for SentryCluster
type PostgresReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile handles the PostgreSQL deployment for Sentry.
func (r *PostgresReconciler) Reconcile(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling Postgres", "SentryCluster", sentryCluster.Name)

	postgresPersistence := sentryCluster.Spec.Persistence.Postgresql

	// Check if external Postgres is configured
	if postgresPersistence.External != nil {
		// Handle external Postgres
		secretName := postgresPersistence.External.SecretName
		log.Info("Using external Postgres", "SecretName", secretName)

		// Fetch the Secret containing the connection details
		secret := &corev1.Secret{}
		err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: sentryCluster.Namespace}, secret)
		if err != nil {
			log.Error(err, "Failed to get external Postgres Secret", "SecretName", secretName)
			return ctrl.Result{}, errors.Wrap(err, "failed to get external Postgres Secret")
		}

		// Validate the Secret data
		host := string(secret.Data["host"])
		port := string(secret.Data["port"])
		user := string(secret.Data["user"])
		dbname := string(secret.Data["dbname"])
		// password := string(secret.Data["password"]) // Optional

		if host == "" || port == "" || user == "" || dbname == "" {
			err := fmt.Errorf("required keys 'host', 'port', 'user', and 'dbname' not found in Secret '%s'", secretName)
			log.Error(err, "Invalid external Postgres Secret")
			return ctrl.Result{}, err
		}

		log.Info("Successfully validated external Postgres connection details", "Host", host, "Port", port, "User", user, "DBName", dbname)
		// Skip creating managed resources (PVC, Service, StatefulSet)
		log.Info("Skipping managed Postgres resources because external Postgres is configured")
		return ctrl.Result{}, nil
	}

	// --- Assuming Managed Postgres ---

	// 1. Reconcile PVC
	if sentryCluster.Spec.Persistence.Postgresql.Managed != nil && sentryCluster.Spec.Persistence.Postgresql.Managed.Size != "" {
		pvcName := sentryCluster.Name + "-postgres-pvc"
		pvc := &corev1.PersistentVolumeClaim{}
		err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: sentryCluster.Namespace}, pvc)
		if err != nil && apierrors.IsNotFound(err) {
			desiredPVC := r.definePostgresPVC(sentryCluster)
			log.Info("Creating a new Postgres PVC", "PVC.Namespace", desiredPVC.Namespace, "PVC.Name", desiredPVC.Name)
			if err := r.Create(ctx, desiredPVC); err != nil {
				log.Error(err, "Failed to create new Postgres PVC", "PVC.Namespace", desiredPVC.Namespace, "PVC.Name", desiredPVC.Name)
				return ctrl.Result{}, errors.Wrap(err, "failed to create Postgres PVC")
			}
			return ctrl.Result{Requeue: true}, nil
		} else if err != nil {
			log.Error(err, "Failed to get Postgres PVC")
			return ctrl.Result{}, errors.Wrap(err, "failed to get Postgres PVC")
		} else {
			log.V(1).Info("Postgres PVC already exists", "PVC.Namespace", pvc.Namespace, "PVC.Name", pvc.Name)
		}
	}

	// 2. Reconcile Service
	serviceName := sentryCluster.Name + "-postgres"
	service := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: sentryCluster.Namespace}, service)
	if err != nil && apierrors.IsNotFound(err) {
		desiredService := r.definePostgresService(sentryCluster)
		log.Info("Creating a new Postgres Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
		if err := r.Create(ctx, desiredService); err != nil {
			log.Error(err, "Failed to create new Postgres Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Postgres Service")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Postgres Service")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Postgres Service")
	} else {
		log.V(1).Info("Postgres Service already exists", "Service.Namespace", service.Namespace, "Service.Name", service.Name)
	}

	// 3. Reconcile StatefulSet
	stsName := sentryCluster.Name + "-postgres"
	sts := &appsv1.StatefulSet{}
	err = r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: sentryCluster.Namespace}, sts)
	if err != nil && apierrors.IsNotFound(err) {
		desiredSts := r.definePostgresStatefulSet(sentryCluster)
		log.Info("Creating a new Postgres StatefulSet", "StatefulSet.Namespace", desiredSts.Namespace, "StatefulSet.Name", desiredSts.Name)
		if err := r.Create(ctx, desiredSts); err != nil {
			log.Error(err, "Failed to create new Postgres StatefulSet", "StatefulSet.Namespace", desiredSts.Namespace, "StatefulSet.Name", desiredSts.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Postgres StatefulSet")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Postgres StatefulSet")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Postgres StatefulSet")
	} else {
		log.V(1).Info("Postgres StatefulSet already exists", "StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)

		// 4. Check StatefulSet readiness
		if sts.Status.ReadyReplicas < *sts.Spec.Replicas {
			log.Info("Postgres StatefulSet not yet ready", "ReadyReplicas", sts.Status.ReadyReplicas, "Replicas", *sts.Spec.Replicas)
			return ctrl.Result{RequeueAfter: time.Second * 30}, nil
		}

		// 5. Update StatefulSet if needed
		desiredSts := r.definePostgresStatefulSet(sentryCluster)
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
			sts.Spec = desiredSts.Spec
			return ctrl.Result{}, nil
		})
		if err != nil {
			log.Error(err, "Failed to update Postgres StatefulSet")
			return ctrl.Result{}, errors.Wrap(err, "failed to update Postgres StatefulSet")
		}
	}

	log.Info("Postgres reconciled successfully", "SentryCluster", sentryCluster.Name)
	return ctrl.Result{}, nil
}

// definePostgresPVC creates the desired PersistentVolumeClaim object for PostgreSQL.
func (r *PostgresReconciler) definePostgresPVC(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.PersistentVolumeClaim {
	labels := GetComponentLabels(sentryCluster, "postgresql")

	// Parse the storage size
	storageSize := resource.MustParse(sentryCluster.Spec.Persistence.Postgresql.Managed.Size)

	// Create the PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-postgres-pvc",
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
			StorageClassName: &sentryCluster.Spec.Persistence.Postgresql.Managed.StorageClass,
		},
	}

	// If StorageClass is empty, set it to nil to use the default StorageClass
	if sentryCluster.Spec.Persistence.Postgresql.Managed.StorageClass == "" {
		pvc.Spec.StorageClassName = nil
	}

	if err := controllerutil.SetControllerReference(sentryCluster, pvc, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on PostgreSQL PVC")
	}
	return pvc
}

// definePostgresService creates the desired Service object for PostgreSQL.
func (r *PostgresReconciler) definePostgresService(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.Service {
	labels := GetComponentLabels(sentryCluster, "postgresql")

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-postgres",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				Port:       PostgresPort,
				TargetPort: intstr.FromInt(PostgresPort),
				Name:       "postgresql",
			}},
			Selector: labels,
			Type:     corev1.ServiceTypeClusterIP,
		},
	}
	if err := controllerutil.SetControllerReference(sentryCluster, svc, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on PostgreSQL Service")
	}
	return svc
}

// definePostgresStatefulSet creates the desired StatefulSet object for PostgreSQL.
func (r *PostgresReconciler) definePostgresStatefulSet(sentryCluster *sentryv1alpha1.SentryCluster) *appsv1.StatefulSet {
	labels := GetComponentLabels(sentryCluster, "postgresql")

	// Set default values
	replicas := int32(1) // PostgreSQL is typically deployed as a single instance in this setup

	// Get the secret name
	secretName := sentryCluster.Name + "-secret"

	// Create the StatefulSet
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-postgres",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: sentryCluster.Name + "-postgres",
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "postgresql",
						Image: "postgres:13-alpine",
						Ports: []corev1.ContainerPort{{
							ContainerPort: PostgresPort,
							Name:          "postgresql",
						}},
						Env: []corev1.EnvVar{
							{Name: "POSTGRES_USER", Value: PostgresUser},
							{Name: "POSTGRES_DB", Value: PostgresDB},
							{Name: "POSTGRES_PASSWORD", ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
									Key:                  "postgres-password",
								},
							}},
							{Name: "PGDATA", Value: "/var/lib/postgresql/data/pgdata"},
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "data",
							MountPath: "/var/lib/postgresql/data",
						}},
						Resources: sentryCluster.Spec.Resources.Postgresql,
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec: &corev1.ExecAction{
									Command: []string{
										"pg_isready",
										"-U", PostgresUser,
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
										"pg_isready",
										"-U", PostgresUser,
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
								ClaimName: sentryCluster.Name + "-postgres-pvc",
							},
						},
					}},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(sentryCluster, sts, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on PostgreSQL StatefulSet")
	}
	return sts
}
