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

// ClickHouseReconciler reconciles ClickHouse for SentryCluster
type ClickHouseReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile handles the ClickHouse deployment for Sentry.
func (r *ClickHouseReconciler) Reconcile(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling ClickHouse", "SentryCluster", sentryCluster.Name)

	// Check if ClickHouse is enabled
	if sentryCluster.Spec.Persistence.ClickHouse == nil {
		log.Info("ClickHouse is not enabled, skipping")
		return ctrl.Result{}, nil
	}

	clickHousePersistence := sentryCluster.Spec.Persistence.ClickHouse

	// Check if external ClickHouse is configured
	if clickHousePersistence.External != nil {
		// Handle external ClickHouse
		secretName := clickHousePersistence.External.SecretName
		log.Info("Using external ClickHouse", "SecretName", secretName)

		// Fetch the Secret containing the connection details
		secret := &corev1.Secret{}
		err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: sentryCluster.Namespace}, secret)
		if err != nil {
			log.Error(err, "Failed to get external ClickHouse Secret", "SecretName", secretName)
			return ctrl.Result{}, errors.Wrap(err, "failed to get external ClickHouse Secret")
		}

		// Validate the Secret data
		host := string(secret.Data["host"])
		port := string(secret.Data["port"])
		user := string(secret.Data["user"])
		// password := string(secret.Data["password"]) // Optional
		database := string(secret.Data["database"])

		if host == "" || port == "" || user == "" || database == "" {
			err := fmt.Errorf("required keys 'host', 'port', 'user', and 'database' not found in Secret '%s'", secretName)
			log.Error(err, "Invalid external ClickHouse Secret")
			return ctrl.Result{}, err
		}

		log.Info("Successfully validated external ClickHouse connection details", "Host", host, "Port", port, "User", user, "Database", database)
		// Skip creating managed resources
		log.Info("Skipping managed ClickHouse resources because external ClickHouse is configured")
		return ctrl.Result{}, nil
	}

	// --- Assuming Managed ClickHouse ---

	// 1. Reconcile PVC
	if clickHousePersistence.Managed != nil && clickHousePersistence.Managed.Storage.Size != "" {
		pvcName := sentryCluster.Name + "-clickhouse-pvc"
		pvc := &corev1.PersistentVolumeClaim{}
		err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: sentryCluster.Namespace}, pvc)
		if err != nil && apierrors.IsNotFound(err) {
			desiredPVC := r.defineClickHousePVC(sentryCluster)
			log.Info("Creating a new ClickHouse PVC", "PVC.Namespace", desiredPVC.Namespace, "PVC.Name", desiredPVC.Name)
			if err := r.Create(ctx, desiredPVC); err != nil {
				log.Error(err, "Failed to create new ClickHouse PVC", "PVC.Namespace", desiredPVC.Namespace, "PVC.Name", desiredPVC.Name)
				return ctrl.Result{}, errors.Wrap(err, "failed to create ClickHouse PVC")
			}
			return ctrl.Result{Requeue: true}, nil
		} else if err != nil {
			log.Error(err, "Failed to get ClickHouse PVC")
			return ctrl.Result{}, errors.Wrap(err, "failed to get ClickHouse PVC")
		} else {
			log.V(1).Info("ClickHouse PVC already exists", "PVC.Namespace", pvc.Namespace, "PVC.Name", pvc.Name)
		}
	}

	// 2. Reconcile ConfigMap for ClickHouse configuration
	configMapName := sentryCluster.Name + "-clickhouse-config"
	configMap := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: sentryCluster.Namespace}, configMap)
	if err != nil && apierrors.IsNotFound(err) {
		desiredConfigMap := r.defineClickHouseConfigMap(sentryCluster)
		log.Info("Creating a new ClickHouse ConfigMap", "ConfigMap.Namespace", desiredConfigMap.Namespace, "ConfigMap.Name", desiredConfigMap.Name)
		if err := r.Create(ctx, desiredConfigMap); err != nil {
			log.Error(err, "Failed to create new ClickHouse ConfigMap", "ConfigMap.Namespace", desiredConfigMap.Namespace, "ConfigMap.Name", desiredConfigMap.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create ClickHouse ConfigMap")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get ClickHouse ConfigMap")
		return ctrl.Result{}, errors.Wrap(err, "failed to get ClickHouse ConfigMap")
	} else {
		log.V(1).Info("ClickHouse ConfigMap already exists", "ConfigMap.Namespace", configMap.Namespace, "ConfigMap.Name", configMap.Name)
	}

	// 3. Reconcile Service
	serviceName := sentryCluster.Name + "-clickhouse"
	service := &corev1.Service{}
	err = r.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: sentryCluster.Namespace}, service)
	if err != nil && apierrors.IsNotFound(err) {
		desiredService := r.defineClickHouseService(sentryCluster)
		log.Info("Creating a new ClickHouse Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
		if err := r.Create(ctx, desiredService); err != nil {
			log.Error(err, "Failed to create new ClickHouse Service", "Service.Namespace", desiredService.Namespace, "Service.Name", desiredService.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create ClickHouse Service")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get ClickHouse Service")
		return ctrl.Result{}, errors.Wrap(err, "failed to get ClickHouse Service")
	} else {
		log.V(1).Info("ClickHouse Service already exists", "Service.Namespace", service.Namespace, "Service.Name", service.Name)
	}

	// 4. Reconcile StatefulSet
	stsName := sentryCluster.Name + "-clickhouse"
	sts := &appsv1.StatefulSet{}
	err = r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: sentryCluster.Namespace}, sts)
	if err != nil && apierrors.IsNotFound(err) {
		desiredSts := r.defineClickHouseStatefulSet(sentryCluster)
		log.Info("Creating a new ClickHouse StatefulSet", "StatefulSet.Namespace", desiredSts.Namespace, "StatefulSet.Name", desiredSts.Name)
		if err := r.Create(ctx, desiredSts); err != nil {
			log.Error(err, "Failed to create new ClickHouse StatefulSet", "StatefulSet.Namespace", desiredSts.Namespace, "StatefulSet.Name", desiredSts.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create ClickHouse StatefulSet")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get ClickHouse StatefulSet")
		return ctrl.Result{}, errors.Wrap(err, "failed to get ClickHouse StatefulSet")
	} else {
		log.V(1).Info("ClickHouse StatefulSet already exists", "StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)

		// 5. Check StatefulSet readiness
		if sts.Status.ReadyReplicas < *sts.Spec.Replicas {
			log.Info("ClickHouse StatefulSet not yet ready", "ReadyReplicas", sts.Status.ReadyReplicas, "Replicas", *sts.Spec.Replicas)
			return ctrl.Result{RequeueAfter: time.Second * 30}, nil
		}

		// 6. Update StatefulSet if needed
		desiredSts := r.defineClickHouseStatefulSet(sentryCluster)
		if !reflect.DeepEqual(sts.Spec, desiredSts.Spec) {
			log.Info("Updating existing ClickHouse StatefulSet", "StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
			sts.Spec = desiredSts.Spec
			if err := r.Update(ctx, sts); err != nil {
				log.Error(err, "Failed to update ClickHouse StatefulSet", "StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
				return ctrl.Result{}, errors.Wrap(err, "failed to update ClickHouse StatefulSet")
			}
			return ctrl.Result{Requeue: true}, nil
		}
	}

	log.Info("ClickHouse reconciled successfully", "SentryCluster", sentryCluster.Name)
	return ctrl.Result{}, nil
}

// defineClickHousePVC creates the desired PersistentVolumeClaim object for ClickHouse.
func (r *ClickHouseReconciler) defineClickHousePVC(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.PersistentVolumeClaim {
	labels := GetComponentLabels(sentryCluster, "clickhouse")

	// Parse the storage size
	storageSize := resource.MustParse(sentryCluster.Spec.Persistence.ClickHouse.Managed.Storage.Size)

	// Create the PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-clickhouse-pvc",
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
			StorageClassName: &sentryCluster.Spec.Persistence.ClickHouse.Managed.Storage.StorageClass,
		},
	}

	// If StorageClass is empty, set it to nil to use the default StorageClass
	if sentryCluster.Spec.Persistence.ClickHouse.Managed.Storage.StorageClass == "" {
		pvc.Spec.StorageClassName = nil
	}

	if err := controllerutil.SetControllerReference(sentryCluster, pvc, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on ClickHouse PVC")
	}
	return pvc
}

// defineClickHouseConfigMap creates the desired ConfigMap object for ClickHouse configuration.
func (r *ClickHouseReconciler) defineClickHouseConfigMap(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.ConfigMap {
	labels := GetComponentLabels(sentryCluster, "clickhouse")

	// Basic ClickHouse configuration
	usersXML := `
<clickhouse>
    <users>
        <default>
            <password>default</password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
        <sentry>
            <password>sentry</password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </sentry>
    </users>
    <profiles>
        <default>
            <max_memory_usage>10000000000</max_memory_usage>
            <use_uncompressed_cache>0</use_uncompressed_cache>
            <load_balancing>random</load_balancing>
        </default>
    </profiles>
    <quotas>
        <default>
            <interval>
                <duration>3600</duration>
                <queries>0</queries>
                <errors>0</errors>
                <result_rows>0</result_rows>
                <read_rows>0</read_rows>
                <execution_time>0</execution_time>
            </interval>
        </default>
    </quotas>
</clickhouse>
`

	configXML := `
<clickhouse>
    <logger>
        <level>information</level>
        <console>1</console>
    </logger>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <listen_host>0.0.0.0</listen_host>
    <max_connections>4096</max_connections>
    <keep_alive_timeout>3</keep_alive_timeout>
    <max_concurrent_queries>100</max_concurrent_queries>
    <uncompressed_cache_size>8589934592</uncompressed_cache_size>
    <mark_cache_size>5368709120</mark_cache_size>
    <path>/var/lib/clickhouse/</path>
    <tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
    <users_config>users.xml</users_config>
    <default_profile>default</default_profile>
    <default_database>default</default_database>
    <timezone>UTC</timezone>
    <mlock_executable>false</mlock_executable>
    <zookeeper>
        <node>
            <host>localhost</host>
            <port>2181</port>
        </node>
    </zookeeper>
    <distributed_ddl>
        <path>/clickhouse/task_queue/ddl</path>
    </distributed_ddl>
    <format_schema_path>/var/lib/clickhouse/format_schemas/</format_schema_path>
</clickhouse>
`

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-clickhouse-config",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Data: map[string]string{
			"users.xml":  usersXML,
			"config.xml": configXML,
		},
	}
	if err := controllerutil.SetControllerReference(sentryCluster, configMap, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on ClickHouse ConfigMap")
	}
	return configMap
}

// defineClickHouseService creates the desired Service object for ClickHouse.
func (r *ClickHouseReconciler) defineClickHouseService(sentryCluster *sentryv1alpha1.SentryCluster) *corev1.Service {
	labels := GetComponentLabels(sentryCluster, "clickhouse")

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-clickhouse",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Port:       ClickHousePort,
					TargetPort: intstr.FromInt(ClickHousePort),
					Name:       "clickhouse",
				},
				{
					Port:       ClickHouseHTTPPort,
					TargetPort: intstr.FromInt(ClickHouseHTTPPort),
					Name:       "clickhouse-http",
				},
			},
			Selector: labels,
			Type:     corev1.ServiceTypeClusterIP,
		},
	}
	if err := controllerutil.SetControllerReference(sentryCluster, svc, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on ClickHouse Service")
	}
	return svc
}

// defineClickHouseStatefulSet creates the desired StatefulSet object for ClickHouse.
func (r *ClickHouseReconciler) defineClickHouseStatefulSet(sentryCluster *sentryv1alpha1.SentryCluster) *appsv1.StatefulSet {
	labels := GetComponentLabels(sentryCluster, "clickhouse")

	// Set default values
	replicas := int32(1) // ClickHouse is typically deployed as a single instance in this setup

	// Get the secret name
	secretName := sentryCluster.Name + "-secret"

	// Create the StatefulSet
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sentryCluster.Name + "-clickhouse",
			Namespace: sentryCluster.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: sentryCluster.Name + "-clickhouse",
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "clickhouse",
						Image: "clickhouse/clickhouse-server:22.3",
						Ports: []corev1.ContainerPort{
							{
								ContainerPort: ClickHousePort,
								Name:          "clickhouse",
							},
							{
								ContainerPort: ClickHouseHTTPPort,
								Name:          "clickhouse-http",
							},
						},
						Env: []corev1.EnvVar{
							{Name: "CLICKHOUSE_USER", Value: "sentry"},
							{Name: "CLICKHOUSE_PASSWORD", ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
									Key:                  "clickhouse-password",
								},
							}},
							{Name: "CLICKHOUSE_DB", Value: "sentry"},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "data",
								MountPath: "/var/lib/clickhouse",
							},
							{
								Name:      "config",
								MountPath: "/etc/clickhouse-server/config.d",
							},
							{
								Name:      "users",
								MountPath: "/etc/clickhouse-server/users.d",
							},
						},
						Resources: sentryCluster.Spec.Resources.ClickHouse,
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intstr.FromInt(ClickHousePort),
								},
							},
							InitialDelaySeconds: 10,
							TimeoutSeconds:      5,
							PeriodSeconds:       10,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intstr.FromInt(ClickHousePort),
								},
							},
							InitialDelaySeconds: 30,
							TimeoutSeconds:      5,
							PeriodSeconds:       15,
						},
					}},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: sentryCluster.Name + "-clickhouse-pvc",
								},
							},
						},
						{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: sentryCluster.Name + "-clickhouse-config",
									},
									Items: []corev1.KeyToPath{
										{
											Key:  "config.xml",
											Path: "config.xml",
										},
									},
								},
							},
						},
						{
							Name: "users",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: sentryCluster.Name + "-clickhouse-config",
									},
									Items: []corev1.KeyToPath{
										{
											Key:  "users.xml",
											Path: "users.xml",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(sentryCluster, sts, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on ClickHouse StatefulSet")
	}
	return sts
}
