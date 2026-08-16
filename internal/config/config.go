// Package config loads runtime configuration from the environment once at
// startup. Both binaries share the type but read only the sections they need.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Env      string
	LogLevel string

	HTTP      HTTPConfig
	Postgres  PostgresConfig
	NATS      NATSConfig
	Storage   StorageConfig
	Worker    WorkerConfig
	Telemetry TelemetryConfig
}

type HTTPConfig struct {
	Addr            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}

type PostgresConfig struct {
	DSN             string
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
}

type NATSConfig struct {
	URL     string
	Stream  string
	Subject string
	Durable string
	// MaxDeliver caps redeliveries. Kept above Worker.MaxAttempts so the DB
	// attempt count is the authority, not JetStream.
	MaxDeliver int
	AckWait    time.Duration
}

type StorageConfig struct {
	Endpoint string
	// PublicEndpoint signs presigned URLs for clients outside the container
	// network (e.g. "localhost:9000"); falls back to Endpoint.
	PublicEndpoint string
	Region         string
	AccessKey      string
	SecretKey      string
	Bucket         string
	UseSSL         bool
	PresignExpiry  time.Duration
	MaxUploadBytes int64
}

type WorkerConfig struct {
	Concurrency     int
	JobTimeout      time.Duration
	ShutdownTimeout time.Duration
	MaxAttempts     int
	MetricsAddr     string
}

type TelemetryConfig struct {
	OTLPEndpoint string // empty disables tracing
	ServiceName  string
}

// Load reads the environment and applies defaults. serviceName names the caller
// in telemetry.
func Load(serviceName string) (Config, error) {
	cfg := Config{
		Env:      env("APP_ENV", "dev"),
		LogLevel: env("LOG_LEVEL", "info"),
		HTTP: HTTPConfig{
			Addr:            env("HTTP_ADDR", ":8080"),
			ReadTimeout:     envDuration("HTTP_READ_TIMEOUT", 15*time.Second),
			WriteTimeout:    envDuration("HTTP_WRITE_TIMEOUT", 30*time.Second),
			ShutdownTimeout: envDuration("HTTP_SHUTDOWN_TIMEOUT", 20*time.Second),
		},
		Postgres: PostgresConfig{
			DSN:             env("POSTGRES_DSN", "postgres://portrait:portrait@localhost:5432/portrait?sslmode=disable"),
			MaxConns:        int32(envInt("POSTGRES_MAX_CONNS", 10)),
			MinConns:        int32(envInt("POSTGRES_MIN_CONNS", 2)),
			MaxConnLifetime: envDuration("POSTGRES_MAX_CONN_LIFETIME", time.Hour),
		},
		NATS: NATSConfig{
			URL:        env("NATS_URL", "nats://localhost:4222"),
			Stream:     env("NATS_STREAM", "JOBS"),
			Subject:    env("NATS_SUBJECT", "jobs.process"),
			Durable:    env("NATS_DURABLE", "job-workers"),
			MaxDeliver: envInt("NATS_MAX_DELIVER", 10),
			AckWait:    envDuration("NATS_ACK_WAIT", 2*time.Minute),
		},
		Storage: StorageConfig{
			Endpoint:       env("S3_ENDPOINT", "localhost:9000"),
			PublicEndpoint: env("S3_PUBLIC_ENDPOINT", ""),
			Region:         env("S3_REGION", "us-east-1"),
			AccessKey:      env("S3_ACCESS_KEY", "minioadmin"),
			SecretKey:      env("S3_SECRET_KEY", "minioadmin"),
			Bucket:         env("S3_BUCKET", "portraits"),
			UseSSL:         envBool("S3_USE_SSL", false),
			PresignExpiry:  envDuration("S3_PRESIGN_EXPIRY", 15*time.Minute),
			MaxUploadBytes: int64(envInt("S3_MAX_UPLOAD_BYTES", 15<<20)),
		},
		Worker: WorkerConfig{
			Concurrency:     envInt("WORKER_CONCURRENCY", 4),
			JobTimeout:      envDuration("WORKER_JOB_TIMEOUT", 90*time.Second),
			ShutdownTimeout: envDuration("WORKER_SHUTDOWN_TIMEOUT", 30*time.Second),
			MaxAttempts:     envInt("WORKER_MAX_ATTEMPTS", 5),
			MetricsAddr:     env("WORKER_METRICS_ADDR", ":8081"),
		},
		Telemetry: TelemetryConfig{
			OTLPEndpoint: env("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
			ServiceName:  serviceName,
		},
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if c.Worker.Concurrency < 1 {
		return fmt.Errorf("worker concurrency must be >= 1, got %d", c.Worker.Concurrency)
	}
	if c.Worker.MaxAttempts < 1 {
		return fmt.Errorf("worker max attempts must be >= 1, got %d", c.Worker.MaxAttempts)
	}
	if c.NATS.MaxDeliver < 1 {
		return fmt.Errorf("nats max deliver must be >= 1, got %d", c.NATS.MaxDeliver)
	}
	return nil
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
