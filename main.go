// Command cluster-api-k3s-suc-extension is a Cluster API Runtime Extension
// that updates k3s control plane nodes in place, through
// system-upgrade-controller in the workload cluster.
package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimecatalog "sigs.k8s.io/cluster-api/api/runtime/catalog"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/cluster-api/controllers/remote"
	"sigs.k8s.io/cluster-api/exp/runtime/server"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"capi-k3s-suc-extension/internal/inplace"
)

const name = "cluster-api-k3s-suc-extension"

func main() {
	var (
		certDir    string
		port       int
		plan       inplace.PlanSettings
		retryAfter time.Duration
	)
	flag.StringVar(&certDir, "webhook-cert-dir", "/certs", "Directory of the TLS certificate (tls.crt, tls.key)")
	flag.IntVar(&port, "webhook-port", 9443, "Port of the Runtime Extension server")
	flag.StringVar(&plan.Namespace, "suc-namespace", "cattle-system", "Namespace of system-upgrade-controller in the workload clusters")
	flag.StringVar(&plan.ServiceAccountName, "suc-service-account", "system-upgrade-controller", "ServiceAccount of the upgrade jobs")
	flag.StringVar(&plan.UpgradeImage, "upgrade-image", "rancher/k3s-upgrade", "Image replacing the k3s binary; SUC appends the version as tag")
	flag.DurationVar(&retryAfter, "retry-after", 15*time.Second, "Delay before Cluster API polls an ongoing upgrade again")
	logOptions := zap.Options{}
	logOptions.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&logOptions)))
	log := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = clusterv1.AddToScheme(scheme)
	catalog := runtimecatalog.New()
	_ = runtimehooksv1.AddToCatalog(catalog)

	webhookServer, err := server.New(server.Options{
		Port:     port,
		CertDir:  certDir,
		CertName: "tls.crt",
		KeyName:  "tls.key",
		Catalog:  catalog,
	})
	if err != nil {
		log.Error(err, "Unable to create the Runtime Extension server")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		HealthProbeBindAddress: ":9440",
		Metrics:                metricsserver.Options{BindAddress: "0"},
		WebhookServer:          webhookServer,
		// Machines are read on each call only: no cache to fill.
		Client: client.Options{Cache: &client.CacheOptions{DisableFor: []client.Object{&clusterv1.Machine{}, &corev1.Secret{}}}},
	})
	if err != nil {
		log.Error(err, "Unable to create the manager")
		os.Exit(1)
	}

	reader := mgr.GetAPIReader()
	handlers := &inplace.Handlers{
		Client: mgr.GetClient(),
		Workload: func(ctx context.Context, cluster types.NamespacedName) (client.Client, error) {
			// A fresh client per call: calls are rare and the workload API
			// restarts during the upgrade.
			cfg, err := remote.RESTConfig(ctx, name, reader, cluster)
			if err != nil {
				return nil, err
			}
			cfg.Timeout = 10 * time.Second
			return client.New(cfg, client.Options{Scheme: scheme})
		},
		Plan:       plan,
		RetryAfter: retryAfter,
	}

	for _, h := range []server.ExtensionHandler{
		{Hook: runtimehooksv1.CanUpdateMachine, Name: "can-update-machine", HandlerFunc: handlers.DoCanUpdateMachine},
		{Hook: runtimehooksv1.UpdateMachine, Name: "update-machine", HandlerFunc: handlers.DoUpdateMachine},
	} {
		if err := webhookServer.AddExtensionHandler(h); err != nil {
			log.Error(err, "Unable to add the extension handler", "hook", h.Name)
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("ping", func(_ *http.Request) error { return nil }); err != nil {
		log.Error(err, "Unable to add the health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("webhook", webhookServer.StartedChecker()); err != nil {
		log.Error(err, "Unable to add the ready check")
		os.Exit(1)
	}

	log.Info("Starting", "sucNamespace", plan.Namespace, "upgradeImage", plan.UpgradeImage)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "Manager stopped")
		os.Exit(1)
	}
}
