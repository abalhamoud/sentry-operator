// reconcilePostgres handles the PostgreSQL deployment for Sentry.
func (r *SentryClusterReconciler) reconcilePostgres(ctx context.Context, sentryCluster *sentryv1alpha1.SentryCluster) (ctrl.Result, error) {
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
		if !reflect.DeepEqual(sts.Spec, desiredSts.Spec) {
			log.Info("Updating existing Postgres StatefulSet", "StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
			sts.Spec = desiredSts.Spec
			if err := r.Update(ctx, sts); err != nil {
				log.Error(err, "Failed to update Postgres StatefulSet", "StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
				return ctrl.Result{}, errors.Wrap(err, "failed to update Postgres StatefulSet")
			}
			return ctrl.Result{Requeue: true}, nil
		}
	}

	log.Info("Postgres reconciled successfully", "SentryCluster", sentryCluster.Name)
	return ctrl.Result{}, nil
}
