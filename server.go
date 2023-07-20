package kafexp

import (
	"context"
	prometheusExporter "contrib.go.opencensus.io/exporter/prometheus"
	"fmt"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/hellofresh/health-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/stats/view"
	"go.uber.org/zap"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func StartConsumerGroupAsService(cfg Config, processor Processor) {
	logger, err := GetZapLogger(cfg.Env)
	routes, err := initRoutes(cfg)
	if err != nil {
		logger.Fatal("failed to setup routes", zap.Error(err))
	}

	ctx, cancelConsumerContext := context.WithCancel(context.Background())

	server, serverErrors := startServer(cfg, routes, logger)
	cGroup, wg, consumerErrChan := startKafkaConsumer(cfg, ctx, processor, logger)

	waitForShutdownSignal(consumerErrChan, serverErrors, cancelConsumerContext, wg, logger)
	if err = cGroup.Close(); err != nil {
		log.Panicf("Error closing consumer group: %v", err)
	}
	if err = server.Close(); err != nil {
		log.Panicf("Error closing server: %v", err)
	}
}

func startKafkaConsumer(cfg Config, ctx context.Context, processor Processor, logger *zap.Logger) (*ConsumerGroup, *sync.WaitGroup, chan error) {
	cGroup, err := NewConsumerGroup(cfg, processor, logger)
	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	consumerErrChan := make(chan error)

	go func() {
		defer wg.Done()
		cGroup.Start(ctx, consumerErrChan)
	}()

	return cGroup, wg, consumerErrChan
}

func startServer(cfg Config, routes *chi.Mux, logger *zap.Logger) (*http.Server, chan error) {

	server := http.Server{
		Addr:    cfg.ServerPort,
		Handler: &ochttp.Handler{Handler: routes},
	}

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info(fmt.Sprintf("starting %s %s", cfg.ServerName, cfg.ServerPort))
		serverErrors <- server.ListenAndServe()
	}()
	return &server, serverErrors
}

func waitForShutdownSignal(errChan, serverErrors chan error, cancelConsumerContext context.CancelFunc, wg *sync.WaitGroup, logger *zap.Logger) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		os.Interrupt,
	)

	select {
	case cErr := <-errChan:
		logger.Error("shutting down consumer due to consumer error...", zap.Error(cErr))
	case <-sigCh:
		logger.Error("shutting down consumer due to OS signal...")
	case sErr := <-serverErrors:
		logger.Error("shutting down consumer due to server error...", zap.Error(sErr))
	}
	cancelConsumerContext()
	wg.Wait()
}

func initRoutes(cfg Config) (*chi.Mux, error) {
	routes := chi.NewRouter()
	routes.Mount("/debug", middleware.Profiler())
	err := health.Register(health.Config{
		Name:      cfg.DeploymentName,
		Timeout:   cfg.HealthcheckTimeout,
		SkipOnErr: false,
		Check: func() error {
			return nil
		},
	})
	if err != nil {
		return nil, errors.WithMessage(err, "failed to register health check config")
	}
	routes.Get("/status", health.HandlerFunc)

	pe, err := initPrometheus(cfg.UserName)

	routes.Handle("/metrics", pe)

	return routes, nil
}

func initPrometheus(namespace string) (*prometheusExporter.Exporter, error) {
	pe, err := prometheusExporter.NewExporter(prometheusExporter.Options{
		Namespace: namespace,
		Registry:  prometheus.DefaultRegisterer.(*prometheus.Registry),
	})
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create Prometheus exporter")
	}
	view.RegisterExporter(pe)
	err = registerMonitoringViews()
	if err != nil {
		return nil, errors.WithMessage(err, "failed to register monitoring views")
	}
	return pe, nil
}
