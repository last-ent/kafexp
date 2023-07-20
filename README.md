# KafExp: Lightweight wrapper over Sarama

This library can be used in two ways

1. As a Kafka Consumer, that can be plugged into existing service/system
2. As a standalone service, that provides health check, metrics etc.

## As a Kafka Consumer

 ```go
package main

import (
	"context"
	"fmt"
	"kafexp"
	"github.com/kelseyhightower/envconfig"
	"sync"
	"os/signal"
	"go.uber.org/zap"
	"log"
	"syscall"
	"os"
)

func main() {
	// Kafka ConsumerGroup setup
	var cfg kafexp.Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger %q", err))
	}

	logger, err := kafexp.GetZapLogger(cfg.Env)
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger %q", err))
	}
	stdLogger, err := kafexp.GetStdLogger(cfg.Env)
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger %q", err))
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	processor := kafexp.NewPrintProcessor(logger)
	cGroup, err := kafexp.NewConsumerGroup(cfg, processor, logger)
	if err != nil {
		panic(err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	errChan := make(chan error)

	go func() {
		defer wg.Done()
		cGroup.Start(ctx, errChan) // Start consumer
	}()

	// Setup rest of application
	// Setup server

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
	cancelCtx()

	wg.Wait()
	if err = cGroup.Close(); err != nil {
		log.Panicf("Error closing consumer group: %v", err)
	}
	if err = server.Close(); err != nil {
		log.Panicf("Error closing server: %v", err)
	}
	
}


```