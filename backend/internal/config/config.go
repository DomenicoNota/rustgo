package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port                    int
	WorkerObservabilityPort int
	DatabaseURL             string
	KafkaBrokers            []string
	KafkaTopic              string
	KafkaDLQTopic           string
	KafkaGroupID            string
	APIKeys                 []string

	MaxBatchSize   int
	MaxBodyBytes   int64
	DefaultPage    int
	MaxPageSize    int
	MaxFilterBytes int

	DBMaxConns          int32
	DBMinConns          int32
	DBConnectTimeout    time.Duration
	DBMaxConnLifetime   time.Duration
	DBMaxConnIdleTime   time.Duration
	DBHealthCheckPeriod time.Duration

	HTTPReadHeaderTimeout time.Duration
	HTTPReadTimeout       time.Duration
	HTTPWriteTimeout      time.Duration
	HTTPIdleTimeout       time.Duration
	HTTPHandlerTimeout    time.Duration
	HTTPShutdownTimeout   time.Duration
	HTTPMaxHeaderBytes    int

	KafkaReadTimeout     time.Duration
	KafkaWriteTimeout    time.Duration
	WorkerRetryAttempts  int
	WorkerRetryBaseDelay time.Duration
	WorkerRetryMaxDelay  time.Duration
	WorkerAttemptTimeout time.Duration
}

func Load() (Config, error) {
	return load(os.LookupEnv)
}

type lookupFunc func(string) (string, bool)

func load(lookup lookupFunc) (Config, error) {
	port, err := positiveInt(lookup, "PORT", 8080, 65_535)
	if err != nil {
		return Config{}, err
	}
	workerObservabilityPort, err := positiveInt(lookup, "WORKER_OBSERVABILITY_PORT", 9091, 65_535)
	if err != nil {
		return Config{}, err
	}
	maxBatchSize, err := positiveInt(lookup, "MAX_BATCH_SIZE", 500, 10_000)
	if err != nil {
		return Config{}, err
	}
	maxBodyBytes, err := positiveInt64(lookup, "MAX_BODY_BYTES", 2<<20, 64<<20)
	if err != nil {
		return Config{}, err
	}
	dbMaxConns, err := positiveInt(lookup, "DB_MAX_CONNS", 10, 1_000)
	if err != nil {
		return Config{}, err
	}
	dbMinConns, err := nonNegativeInt(lookup, "DB_MIN_CONNS", 1, dbMaxConns)
	if err != nil {
		return Config{}, err
	}
	workerRetryAttempts, err := positiveInt(lookup, "WORKER_RETRY_MAX_ATTEMPTS", 5, 100)
	if err != nil {
		return Config{}, err
	}
	workerRetryBaseDelayMS, err := positiveInt(lookup, "WORKER_RETRY_BASE_DELAY_MS", 250, 60_000)
	if err != nil {
		return Config{}, err
	}
	workerRetryMaxDelayMS, err := positiveInt(lookup, "WORKER_RETRY_MAX_DELAY_MS", 5_000, 300_000)
	if err != nil {
		return Config{}, err
	}
	if workerRetryMaxDelayMS < workerRetryBaseDelayMS {
		return Config{}, fmt.Errorf("WORKER_RETRY_MAX_DELAY_MS must be at least WORKER_RETRY_BASE_DELAY_MS")
	}
	workerAttemptTimeoutMS, err := positiveInt(lookup, "WORKER_ATTEMPT_TIMEOUT_MS", 10_000, 300_000)
	if err != nil {
		return Config{}, err
	}

	apiKeys := splitList(value(lookup, "API_KEYS", ""))
	if len(apiKeys) > 32 {
		return Config{}, fmt.Errorf("API_KEYS must contain at most 32 keys")
	}
	for _, key := range apiKeys {
		if len(key) > 256 {
			return Config{}, fmt.Errorf("each API_KEYS value must be at most 256 bytes")
		}
	}
	brokers := splitList(value(lookup, "KAFKA_BROKERS", "localhost:29092"))
	if len(brokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}
	databaseURL, err := requiredValue(lookup, "DATABASE_URL", "postgres://logstream:local-only-example-password@localhost:5432/logstream?sslmode=disable")
	if err != nil {
		return Config{}, err
	}
	kafkaTopic, err := requiredValue(lookup, "KAFKA_TOPIC", "logs.raw")
	if err != nil {
		return Config{}, err
	}
	kafkaDLQTopic, err := requiredValue(lookup, "KAFKA_DLQ_TOPIC", "logs.dlq")
	if err != nil {
		return Config{}, err
	}
	kafkaGroupID, err := requiredValue(lookup, "KAFKA_GROUP_ID", "logstream-persistence")
	if err != nil {
		return Config{}, err
	}

	return Config{
		Port:                    port,
		WorkerObservabilityPort: workerObservabilityPort,
		DatabaseURL:             databaseURL,
		KafkaBrokers:            brokers,
		KafkaTopic:              kafkaTopic,
		KafkaDLQTopic:           kafkaDLQTopic,
		KafkaGroupID:            kafkaGroupID,
		APIKeys:                 apiKeys,

		MaxBatchSize:   maxBatchSize,
		MaxBodyBytes:   maxBodyBytes,
		DefaultPage:    100,
		MaxPageSize:    500,
		MaxFilterBytes: 256,

		DBMaxConns:          int32(dbMaxConns),
		DBMinConns:          int32(dbMinConns),
		DBConnectTimeout:    5 * time.Second,
		DBMaxConnLifetime:   30 * time.Minute,
		DBMaxConnIdleTime:   5 * time.Minute,
		DBHealthCheckPeriod: 30 * time.Second,

		HTTPReadHeaderTimeout: 5 * time.Second,
		HTTPReadTimeout:       15 * time.Second,
		HTTPWriteTimeout:      15 * time.Second,
		HTTPIdleTimeout:       60 * time.Second,
		HTTPHandlerTimeout:    12 * time.Second,
		HTTPShutdownTimeout:   10 * time.Second,
		HTTPMaxHeaderBytes:    1 << 20,

		KafkaReadTimeout:     10 * time.Second,
		KafkaWriteTimeout:    10 * time.Second,
		WorkerRetryAttempts:  workerRetryAttempts,
		WorkerRetryBaseDelay: time.Duration(workerRetryBaseDelayMS) * time.Millisecond,
		WorkerRetryMaxDelay:  time.Duration(workerRetryMaxDelayMS) * time.Millisecond,
		WorkerAttemptTimeout: time.Duration(workerAttemptTimeoutMS) * time.Millisecond,
	}, nil
}

// ValidateAPI checks configuration needed only by the HTTP API. Keeping this
// separate avoids making the worker and migration commands require API keys.
func (c Config) ValidateAPI() error {
	if len(c.APIKeys) == 0 {
		return fmt.Errorf("API_KEYS must contain at least one non-empty key")
	}
	return nil
}

func value(lookup lookupFunc, key, fallback string) string {
	raw, ok := lookup(key)
	if !ok {
		return fallback
	}
	return strings.TrimSpace(raw)
}

func requiredValue(lookup lookupFunc, key, fallback string) (string, error) {
	result := value(lookup, key, fallback)
	if result == "" {
		return "", fmt.Errorf("%s must not be empty", key)
	}
	return result, nil
}

func positiveInt(lookup lookupFunc, key string, fallback, maximum int) (int, error) {
	raw, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || parsed <= 0 || parsed > maximum {
		return 0, fmt.Errorf("%s must be an integer between 1 and %d", key, maximum)
	}
	return parsed, nil
}

func nonNegativeInt(lookup lookupFunc, key string, fallback, maximum int) (int, error) {
	raw, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || parsed < 0 || parsed > maximum {
		return 0, fmt.Errorf("%s must be an integer between 0 and %d", key, maximum)
	}
	return parsed, nil
}

func positiveInt64(lookup lookupFunc, key string, fallback, maximum int64) (int64, error) {
	raw, ok := lookup(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || parsed <= 0 || parsed > maximum {
		return 0, fmt.Errorf("%s must be an integer between 1 and %d", key, maximum)
	}
	return parsed, nil
}

func splitList(raw string) []string {
	values := make([]string, 0)
	for _, part := range strings.Split(raw, ",") {
		if item := strings.TrimSpace(part); item != "" {
			values = append(values, item)
		}
	}
	return values
}
