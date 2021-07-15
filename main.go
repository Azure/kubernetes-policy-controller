/*

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

package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/go-logr/zapr"
	"github.com/open-policy-agent/cert-controller/pkg/rotator"
	opa "github.com/open-policy-agent/frameworks/constraint/pkg/client"
	"github.com/open-policy-agent/frameworks/constraint/pkg/client/drivers/local"
	api "github.com/open-policy-agent/gatekeeper/apis"
	configv1alpha1 "github.com/open-policy-agent/gatekeeper/apis/config/v1alpha1"
	mutationsv1alpha1 "github.com/open-policy-agent/gatekeeper/apis/mutations/v1alpha1"
	statusv1beta1 "github.com/open-policy-agent/gatekeeper/apis/status/v1beta1"
	"github.com/open-policy-agent/gatekeeper/pkg/audit"
	"github.com/open-policy-agent/gatekeeper/pkg/controller"
	"github.com/open-policy-agent/gatekeeper/pkg/controller/config/process"
	"github.com/open-policy-agent/gatekeeper/pkg/metrics"
	"github.com/open-policy-agent/gatekeeper/pkg/mutation"
	"github.com/open-policy-agent/gatekeeper/pkg/operations"
	"github.com/open-policy-agent/gatekeeper/pkg/readiness"
	"github.com/open-policy-agent/gatekeeper/pkg/target"
	"github.com/open-policy-agent/gatekeeper/pkg/upgrade"
	"github.com/open-policy-agent/gatekeeper/pkg/util"
	"github.com/open-policy-agent/gatekeeper/pkg/version"
	"github.com/open-policy-agent/gatekeeper/pkg/watch"
	"github.com/open-policy-agent/gatekeeper/pkg/webhook"
	"github.com/open-policy-agent/gatekeeper/third_party/sigs.k8s.io/controller-runtime/pkg/dynamiccache"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	crzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
	// +kubebuilder:scaffold:imports
)

var webhooks = []rotator.WebhookInfo{
	{
		Name: webhook.VwhName,
		Type: rotator.Validating,
	},
}

const (
	secretName     = "gatekeeper-webhook-server-cert"
	serviceName    = "gatekeeper-webhook-service"
	caName         = "gatekeeper-ca"
	caOrganization = "gatekeeper"
)

var (
	// DNSName is <service name>.<namespace>.svc.
	dnsName          = fmt.Sprintf("%s.%s.svc", serviceName, util.GetNamespace())
	scheme           = runtime.NewScheme()
	setupLog         = ctrl.Log.WithName("setup")
	logLevelEncoders = map[string]zapcore.LevelEncoder{
		"lower":        zapcore.LowercaseLevelEncoder,
		"capital":      zapcore.CapitalLevelEncoder,
		"color":        zapcore.LowercaseColorLevelEncoder,
		"capitalcolor": zapcore.CapitalColorLevelEncoder,
	}
)

var (
	logLevel            = flag.String("log-level", "INFO", "Minimum log level. For example, DEBUG, INFO, WARNING, ERROR. Defaulted to INFO if unspecified.")
	logLevelKey         = flag.String("log-level-key", "level", "JSON key for the log level field, defaults to `level`")
	logLevelEncoder     = flag.String("log-level-encoder", "lower", "Encoder for the value of the log level field. Valid values: [`lower`, `capital`, `color`, `capitalcolor`], default: `lower`")
	healthAddr          = flag.String("health-addr", ":9090", "The address to which the health endpoint binds.")
	metricsAddr         = flag.String("metrics-addr", "0", "The address the metric endpoint binds to.")
	port                = flag.Int("port", 443, "port for the server. defaulted to 443 if unspecified ")
	certDir             = flag.String("cert-dir", "/certs", "The directory where certs are stored, defaults to /certs")
	disableCertRotation = flag.Bool("disable-cert-rotation", false, "disable automatic generation and rotation of webhook TLS certificates/keys")
	enableProfile       = flag.Bool("enable-pprof", false, "enable pprof profiling")
	profilePort         = flag.Int("pprof-port", 6060, "port for pprof profiling. defaulted to 6060 if unspecified")
	disabledBuiltins    = util.NewFlagSet()
)

func init() {
	_ = clientgoscheme.AddToScheme(scheme)

	_ = api.AddToScheme(scheme)

	_ = configv1alpha1.AddToScheme(scheme)
	_ = statusv1beta1.AddToScheme(scheme)
	_ = mutationsv1alpha1.AddToScheme(scheme)
	// +kubebuilder:scaffold:scheme
	flag.Var(disabledBuiltins, "disable-opa-builtin", "disable opa built-in function, this flag can be declared more than once.")
}

func main() {
	flag.Parse()
	encoder, ok := logLevelEncoders[*logLevelEncoder]
	if !ok {
		setupLog.Error(fmt.Errorf("invalid log level encoder: %v", *logLevelEncoder), "Invalid log level encoder")
		os.Exit(1)
	}

	if *enableProfile {
		setupLog.Info("Starting profiling on port %s", *profilePort)
		go func() {
			addr := fmt.Sprintf("%s:%d", "localhost", *profilePort)
			setupLog.Error(http.ListenAndServe(addr, nil), "unable to start profiling server")
		}()
	}

	switch *logLevel {
	case "DEBUG":
		eCfg := zap.NewDevelopmentEncoderConfig()
		eCfg.LevelKey = *logLevelKey
		eCfg.EncodeLevel = encoder
		logger := crzap.New(crzap.UseDevMode(true), crzap.Encoder(zapcore.NewConsoleEncoder(eCfg)))
		ctrl.SetLogger(logger)
		klog.SetLogger(logger)
	case "WARNING", "ERROR":
		setLoggerForProduction(encoder)
	case "INFO":
		fallthrough
	default:
		eCfg := zap.NewProductionEncoderConfig()
		eCfg.LevelKey = *logLevelKey
		eCfg.EncodeLevel = encoder
		logger := crzap.New(crzap.UseDevMode(false), crzap.Encoder(zapcore.NewJSONEncoder(eCfg)))
		ctrl.SetLogger(logger)
		klog.SetLogger(logger)
	}
	config := ctrl.GetConfigOrDie()
	config.UserAgent = version.GetUserAgent()

	webhooks = webhook.AppendMutationWebhookIfEnabled(webhooks)

	mgr, err := ctrl.NewManager(config, ctrl.Options{
		NewCache:               dynamiccache.New,
		Scheme:                 scheme,
		MetricsBindAddress:     *metricsAddr,
		LeaderElection:         false,
		Port:                   *port,
		CertDir:                *certDir,
		HealthProbeBindAddress: *healthAddr,
		MapperProvider: func(c *rest.Config) (meta.RESTMapper, error) {
			return apiutil.NewDynamicRESTMapper(c)
		},
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Make sure certs are generated and valid if cert rotation is enabled.
	setupFinished := make(chan struct{})
	if !*disableCertRotation && operations.IsAssigned(operations.Webhook) {
		setupLog.Info("setting up cert rotation")
		if err := rotator.AddRotator(mgr, &rotator.CertRotator{
			SecretKey: types.NamespacedName{
				Namespace: util.GetNamespace(),
				Name:      secretName,
			},
			CertDir:        *certDir,
			CAName:         caName,
			CAOrganization: caOrganization,
			DNSName:        dnsName,
			IsReady:        setupFinished,
			Webhooks:       webhooks,
		}); err != nil {
			setupLog.Error(err, "unable to set up cert rotation")
			os.Exit(1)
		}
	} else {
		close(setupFinished)
	}

	// ControllerSwitch will be used to disable controllers during our teardown process,
	// avoiding conflicts in finalizer cleanup.
	sw := watch.NewSwitch()

	// Setup tracker and register readiness probe.
	tracker, err := readiness.SetupTracker(mgr, *mutation.MutationEnabled)
	if err != nil {
		setupLog.Error(err, "unable to register readiness tracker")
		os.Exit(1)
	}

	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("default", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to create health check")
		os.Exit(1)
	}
	// Setup controllers asynchronously, they will block for certificate generation if needed.
	go setupControllers(mgr, sw, tracker, setupFinished)

	setupLog.Info("starting manager")
	hadError := false
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		hadError = true
	}

	// Manager stops controllers asynchronously.
	// Instead, we use ControllerSwitch to synchronously prevent them from doing more work.
	// This can be removed when finalizer and status teardown is removed.
	setupLog.Info("disabling controllers...")
	sw.Stop()

	if hadError {
		os.Exit(1)
	}
}

func setupControllers(mgr ctrl.Manager, sw *watch.ControllerSwitch, tracker *readiness.Tracker, setupFinished chan struct{}) {
	// Block until the setup (certificate generation) finishes.
	<-setupFinished

	// initialize OPA
	driver := local.New(local.Tracing(false), local.DisableBuiltins(disabledBuiltins.ToSlice()...))
	backend, err := opa.NewBackend(opa.Driver(driver))
	if err != nil {
		setupLog.Error(err, "unable to set up OPA backend")
		os.Exit(1)
	}
	client, err := backend.NewClient(opa.Targets(&target.K8sValidationTarget{}))
	if err != nil {
		setupLog.Error(err, "unable to set up OPA client")
	}

	mutationCache := mutation.NewSystem()

	c := mgr.GetCache()
	dc, ok := c.(watch.RemovableCache)
	if !ok {
		err := fmt.Errorf("expected dynamic cache, got: %T", c)
		setupLog.Error(err, "fetching dynamic cache")
		os.Exit(1)
	}
	wm, err := watch.New(dc)
	if err != nil {
		setupLog.Error(err, "unable to create watch manager")
		os.Exit(1)
	}
	if err := mgr.Add(wm); err != nil {
		setupLog.Error(err, "unable to register watch manager with the manager")
		os.Exit(1)
	}

	// processExcluder is used for namespace exclusion for specified processes in config
	processExcluder := process.Get()

	// Setup all Controllers
	setupLog.Info("setting up controllers")
	opts := controller.Dependencies{
		Opa:              client,
		WatchManger:      wm,
		ControllerSwitch: sw,
		Tracker:          tracker,
		ProcessExcluder:  processExcluder,
		MutationCache:    mutationCache,
	}
	if err := controller.AddToManager(mgr, opts); err != nil {
		setupLog.Error(err, "unable to register controllers with the manager")
		os.Exit(1)
	}

	if operations.IsAssigned(operations.Webhook) {
		setupLog.Info("setting up webhooks")
		if err := webhook.AddToManager(mgr, client, processExcluder, mutationCache); err != nil {
			setupLog.Error(err, "unable to register webhooks with the manager")
			os.Exit(1)
		}
	}
	if operations.IsAssigned(operations.Audit) {
		setupLog.Info("setting up audit")
		if err := audit.AddToManager(mgr, client, processExcluder); err != nil {
			setupLog.Error(err, "unable to register audit with the manager")
			os.Exit(1)
		}
	}

	setupLog.Info("setting up upgrade")
	if err := upgrade.AddToManager(mgr); err != nil {
		setupLog.Error(err, "unable to register upgrade with the manager")
		os.Exit(1)
	}

	setupLog.Info("setting up metrics")
	if err := metrics.AddToManager(mgr); err != nil {
		setupLog.Error(err, "unable to register metrics with the manager")
		os.Exit(1)
	}
}

func setLoggerForProduction(encoder zapcore.LevelEncoder) {
	sink := zapcore.AddSync(os.Stderr)
	var opts []zap.Option
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.LevelKey = *logLevelKey
	encCfg.EncodeLevel = encoder
	enc := zapcore.NewJSONEncoder(encCfg)
	lvl := zap.NewAtomicLevelAt(zap.WarnLevel)
	opts = append(opts, zap.AddStacktrace(zap.ErrorLevel),
		zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			return zapcore.NewSamplerWithOptions(core, time.Second, 100, 100)
		}),
		zap.AddCallerSkip(1), zap.ErrorOutput(sink))
	zlog := zap.New(zapcore.NewCore(&crzap.KubeAwareEncoder{Encoder: enc, Verbose: false}, sink, lvl))
	zlog = zlog.WithOptions(opts...)
	newlogger := zapr.NewLogger(zlog)
	ctrl.SetLogger(newlogger)
	klog.SetLogger(newlogger)
}
