package kafexp

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"time"

	"github.com/IBM/sarama"
)

// kafkaOffsetEarliest stands for the log head offset, i.e. the offset that will be
// assigned to the next message that will be produced to the partition.
const kafkaOffsetEarliest = "earliest"

// Config defines configuration to setup Kafka Consumers
type Config struct {
	Env              string   `envconfig:"ENV" required:"true"`
	Addresses        []string `envconfig:"KAFKA_DSN" required:"true"`
	Topics           []string `envconfig:"KAFKA_CONSUMER_TOPICS" required:"true"`
	GroupID          string   `envconfig:"KAFKA_CONSUMER_GROUP_ID" required:"true"`
	ClientID         string   `envconfig:"KAFKA_CLIENT_ID" required:"true"`
	OffsetReset      string   `envconfig:"KAFKA_CONSUMER_AUTO_OFFSET_RESET" required:"true"`
	UserName         string   `envconfig:"KAFKA_USERNAME" required:"true"`
	Password         string   `envconfig:"KAFKA_PASSWORD" required:"true"`
	SASLEnable       bool     `envconfig:"KAFKA_SASL_ENABLE" required:"true"`
	SecurityProtocol string   `default:"SASL_SSL" envconfig:"KAFKA_SECURITY_PROTOCOL" required:"true"`
	SASLMechanism    string   `default:"SCRAM-SHA-256" envconfig:"KAFKA_SASL_MECHANISM" required:"true"`
	CaCert           string   `envconfig:"KAFKA_CA_CERT"`

	// Sarama should be initialized by calling setupSarama method
	Sarama *sarama.Config

	// Server config
	DeploymentName     string        `envconfig:"DEPLOYMENT_NAME" required:"true"`
	HealthcheckTimeout time.Duration `default:"2s" envconfig:"HEALTHCHECK_TIMEOUT"`
	ServerPort         string        `default:":8080" envconfig:"SERVER_PORT" required:"true"`
	ServerName         string        `envconfig:"DEPLOYMENT_NAME" required:"true"`
}

// setupSarama is responsible for initializing Config.Sarama field.
func setupSarama(cfg Config, logger *log.Logger) Config {
	saramaConfig := sarama.NewConfig()

	sarama.Logger = logger
	sarama.DebugLogger = logger

	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	if cfg.OffsetReset == kafkaOffsetEarliest {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	saramaConfig.ClientID = cfg.ClientID

	if cfg.SASLEnable {
		saramaConfig.Net.SASL.Enable = cfg.SASLEnable
		saramaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(cfg.SASLMechanism)
		saramaConfig.Net.SASL.User = cfg.UserName
		saramaConfig.Net.SASL.Password = cfg.Password
		saramaConfig.Net.SASL.Handshake = true
		saramaConfig.Net.TLS.Enable = true

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(cfg.CaCert))

		tlsConfig := &tls.Config{
			RootCAs:            caCertPool,
			InsecureSkipVerify: true,
		}
		saramaConfig.Net.TLS.Config = tlsConfig
	}
	cfg.Sarama = saramaConfig
	return cfg
}

// NewKConfig returns
func NewKConfig(cfg Config, logger *log.Logger) Config {
	return setupSarama(cfg, logger)
}
