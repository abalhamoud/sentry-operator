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

	"github.com/pkg/errors"
	networkingv1 "k8s.io/api/networking/v1"
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

// IngressReconciler reconciles Ingress for SentryCluster
type IngressReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Reconcile handles the Ingress for Sentry.
func (r *IngressReconciler) Reconcile(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling Ingress", "SentryCluster", sentryCluster.Name)

	// Skip if ingress is not enabled
	if !sentryCluster.Spec.Ingress.Enabled {
		log.Info("Ingress is not enabled, skipping")
		return ctrl.Result{}, nil
	}

	// Reconcile Ingress
	ingressName := sentryCluster.Name
	ingress := &networkingv1.Ingress{}
	err := r.Get(ctx, types.NamespacedName{Name: ingressName, Namespace: sentryCluster.Namespace}, ingress)
	if err != nil && apierrors.IsNotFound(err) {
		desiredIngress := r.defineIngress(sentryCluster)
		log.Info("Creating a new Ingress", "Ingress.Namespace", desiredIngress.Namespace, "Ingress.Name", desiredIngress.Name)
		if err := r.Create(ctx, desiredIngress); err != nil {
			log.Error(err, "Failed to create new Ingress", "Ingress.Namespace", desiredIngress.Namespace, "Ingress.Name", desiredIngress.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Ingress")
		}
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Ingress")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Ingress")
	} else {
		log.V(1).Info("Ingress already exists", "Ingress.Namespace", ingress.Namespace, "Ingress.Name", ingress.Name)

		// Update Ingress if needed
		desiredIngress := r.defineIngress(sentryCluster)
		if !reflect.DeepEqual(ingress.Spec, desiredIngress.Spec) || !reflect.DeepEqual(ingress.Annotations, desiredIngress.Annotations) {
			log.Info("Updating existing Ingress", "Ingress.Namespace", ingress.Namespace, "Ingress.Name", ingress.Name)
			ingress.Spec = desiredIngress.Spec
			ingress.Annotations = desiredIngress.Annotations
			if err := r.Update(ctx, ingress); err != nil {
				log.Error(err, "Failed to update Ingress", "Ingress.Namespace", ingress.Namespace, "Ingress.Name", ingress.Name)
				return ctrl.Result{}, errors.Wrap(err, "failed to update Ingress")
			}
			return ctrl.Result{Requeue: true}, nil
		}
	}

	log.Info("Ingress reconciled successfully", "SentryCluster", sentryCluster.Name)
	return ctrl.Result{}, nil
}

// defineIngress creates the desired Ingress object for Sentry.
func (r *IngressReconciler) defineIngress(sentryCluster *sentryv1alpha1.SentryCluster) *networkingv1.Ingress {
	labels := GetComponentLabels(sentryCluster, "ingress")

	// Set up annotations
	annotations := make(map[string]string)

	// Add custom annotations if provided
	if sentryCluster.Spec.Ingress.Annotations != nil {
		for k, v := range sentryCluster.Spec.Ingress.Annotations {
			annotations[k] = v
		}
	}

	// Set up path type
	pathType := networkingv1.PathTypePrefix
	if sentryCluster.Spec.Ingress.PathType != "" {
		switch sentryCluster.Spec.Ingress.PathType {
		case "Exact":
			pathType = networkingv1.PathTypeExact
		case "Prefix":
			pathType = networkingv1.PathTypePrefix
		case "ImplementationSpecific":
			pathType = networkingv1.PathTypeImplementationSpecific
		}
	}

	// Set up path
	path := "/"
	if sentryCluster.Spec.Ingress.Path != "" {
		path = sentryCluster.Spec.Ingress.Path
	}

	// Create the Ingress
	ingress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:        sentryCluster.Name,
			Namespace:   sentryCluster.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{
				{
					Host: sentryCluster.Spec.Ingress.Host,
					IngressRuleValue: networkingv1.IngressRuleValue{
						HTTP: &networkingv1.HTTPIngressRuleValue{
							Paths: []networkingv1.HTTPIngressPath{
								{
									Path:     path,
									PathType: &pathType,
									Backend: networkingv1.IngressBackend{
										Service: &networkingv1.IngressServiceBackend{
											Name: fmt.Sprintf("%s-web", sentryCluster.Name),
											Port: networkingv1.ServiceBackendPort{
												Number: 80,
											},
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

	// Add TLS if enabled
	if sentryCluster.Spec.Ingress.TLS != nil && sentryCluster.Spec.Ingress.TLS.Enabled {
		secretName := sentryCluster.Spec.Ingress.TLS.SecretName
		if secretName == "" {
			secretName = fmt.Sprintf("%s-tls", sentryCluster.Name)
		}

		ingress.Spec.TLS = []networkingv1.IngressTLS{
			{
				Hosts:      []string{sentryCluster.Spec.Ingress.Host},
				SecretName: secretName,
			},
		}
	}

	// Add ingress class name if provided
	if sentryCluster.Spec.Ingress.ClassName != "" {
		ingress.Spec.IngressClassName = &sentryCluster.Spec.Ingress.ClassName
	}

	if err := controllerutil.SetControllerReference(sentryCluster, ingress, r.Scheme); err != nil {
		log.FromContext(context.Background()).Error(err, "Failed to set controller reference on Ingress")
	}
	return ingress
}
