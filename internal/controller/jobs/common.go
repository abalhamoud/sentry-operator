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
	"time"

	"github.com/pkg/errors"
	batchv1 "k8s.io/api/batch/v1"
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

// JobReconciler is the base interface for all job reconcilers
type JobReconciler interface {
	// Reconcile reconciles the job
	Reconcile(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error)
	// GetJobName returns the name of the job
	GetJobName(sentryCluster *sentryv1alpha1.SentryCluster) string
	// GetJobConditionType returns the condition type for the job
	GetJobConditionType() string
	// ShouldRunJob determines if the job should be run
	ShouldRunJob(sentryCluster *sentryv1alpha1.SentryCluster) bool
	// CreateJob creates the job
	CreateJob(sentryCluster *sentryv1alpha1.SentryCluster) *batchv1.Job
}

// BaseJobReconciler provides common functionality for job reconcilers
type BaseJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// ReconcileJob handles the reconciliation of a job
func (r *BaseJobReconciler) ReconcileJob(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster, jobReconciler JobReconciler) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	jobName := jobReconciler.GetJobName(sentryCluster)
	conditionType := jobReconciler.GetJobConditionType()

	// Check if the job should be run
	if !jobReconciler.ShouldRunJob(sentryCluster) {
		log.Info("Job not needed, skipping", "Job", jobName)
		return ctrl.Result{}, nil
	}

	// Check if the job already exists
	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: sentryCluster.Namespace}, job)
	if err != nil && apierrors.IsNotFound(err) {
		// Create the job
		job = jobReconciler.CreateJob(sentryCluster)
		log.Info("Creating a new Job", "Job.Namespace", job.Namespace, "Job.Name", job.Name)
		if err := r.Create(ctx, job); err != nil {
			log.Error(err, "Failed to create new Job", "Job.Namespace", job.Namespace, "Job.Name", job.Name)
			return ctrl.Result{}, errors.Wrap(err, "failed to create Job")
		}
		
		// Update the condition to indicate the job is running
		r.setJobCondition(ctx, sentryCluster, conditionType, metav1.ConditionFalse, "JobStarted", "Job has been started")
		
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		log.Error(err, "Failed to get Job")
		return ctrl.Result{}, errors.Wrap(err, "failed to get Job")
	}

	// Check the job status
	if job.Status.Succeeded > 0 {
		log.Info("Job completed successfully", "Job.Namespace", job.Namespace, "Job.Name", job.Name)
		// Update the condition to indicate the job is complete
		r.setJobCondition(ctx, sentryCluster, conditionType, metav1.ConditionTrue, "JobCompleted", "Job has completed successfully")
		return ctrl.Result{}, nil
	} else if job.Status.Failed > 0 {
		log.Info("Job failed", "Job.Namespace", job.Namespace, "Job.Name", job.Name, "Failed", job.Status.Failed)
		// If the job has failed too many times, update the condition to indicate failure
		if job.Status.Failed >= *job.Spec.BackoffLimit {
			r.setJobCondition(ctx, sentryCluster, conditionType, metav1.ConditionFalse, "JobFailed", fmt.Sprintf("Job has failed %d times", job.Status.Failed))
			return ctrl.Result{}, errors.New("job failed too many times")
		}
		// Otherwise, wait for the job to retry
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}

	// Job is still running
	log.Info("Job is still running", "Job.Namespace", job.Namespace, "Job.Name", job.Name)
	return ctrl.Result{RequeueAfter: time.Second * 30}, nil
}

// setJobCondition updates the condition for a job in the SentryCluster status
func (r *BaseJobReconciler) setJobCondition(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster, conditionType string, status metav1.ConditionStatus, reason, message string) {
	log := log.FromContext(ctx)
	
	// Create a new condition
	condition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}

	// Update the status
	sentryCluster.Status.SetCondition(condition)
	if err := r.Status().Update(ctx, sentryCluster); err != nil {
		log.Error(err, "Failed to update job condition", "Condition", conditionType)
	}
}

// IsJobComplete checks if a job has completed successfully
func (r *BaseJobReconciler) IsJobComplete(sentryCluster *sentryv1alpha1.SentryCluster, conditionType string) bool {
	condition := sentryCluster.Status.GetCondition(conditionType)
	return condition != nil && condition.Status == metav1.ConditionTrue && condition.Reason == "JobCompleted"
}

// GetJobLabels returns the labels for a job
func GetJobLabels(sentryCluster *sentryv1alpha1.SentryCluster, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "sentry",
		"app.kubernetes.io/instance":   sentryCluster.Name,
		"app.kubernetes.io/component":  component,
		"app.kubernetes.io/managed-by": "sentry-operator",
	}
}
