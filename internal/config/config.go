package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

// Config represents the service configuration
type Config struct {
	Server    Server    `mapstructure:"server"`
	AMQP      AMQP      `mapstructure:"amqp"`
	OrgEvents OrgEvents `mapstructure:"org_events"`
	Db        Db        `mapstructure:"db"`
}

// OrgEvents contains organization events configuration
type OrgEvents struct {
	Exchange    string `mapstructure:"exchange"`
	Queue       string `mapstructure:"queue"`
	RoutingKey  string `mapstructure:"routing_key"`
	ConsumerTag string `mapstructure:"consumer_tag"`
}

// Server contains server configuration
type Server struct {
	LogLevel string `mapstructure:"log_level"`
	APIPort  int    `mapstructure:"api_port"`
}

// AMQP contains RabbitMQ configuration
type AMQP struct {
	BrokerURL      string        `mapstructure:"broker_url"`
	ConsumerTag    string        `mapstructure:"consumer_tag"`
	PrefetchCount  int           `mapstructure:"prefetch_count"`
	AllowedVHosts  []string      `mapstructure:"allowed_vhosts"`
	ReconnectDelay time.Duration `mapstructure:"reconnect_delay"`
}

// Db contains Db configuration
type Db struct {
	Name           string        `mapstructure:"name"`
	Username       string        `mapstructure:"username"`
	Password       string        `mapstructure:"password"`
	Host           string        `mapstructure:"host"`
	Port           int           `mapstructure:"port"`
	BatchSize      int           `mapstructure:"batch_size"`
	FlushInterval  time.Duration `mapstructure:"flush_interval"`
	MaxConnections int           `mapstructure:"max_connections"`
	MaxIdleConns   int           `mapstructure:"max_idle_conns"`
}

// LoadConfig loads configuration from file and environment variables
func LoadConfig() (*Config, error) {
	_ = godotenv.Load(".env")

	var cfg Config
	vp := viper.New()

	vp.SetConfigFile("configs/config.yaml")
	vp.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	vp.AutomaticEnv()

	if err := vp.ReadInConfig(); err != nil {
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if !errors.As(err, &configFileNotFoundError) {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	if err := vp.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// validateConfig validates the configuration values
func validateConfig(cfg *Config) error {
	// Validate AMQP broker URL if provided
	if cfg.AMQP.BrokerURL != "" {
		if _, err := url.Parse(cfg.AMQP.BrokerURL); err != nil {
			return fmt.Errorf("invalid AMQP broker URL: %w", err)
		}
	} else {
		return fmt.Errorf("AMQP broker URL is required")
	}

	// Validate port numbers
	if cfg.Server.APIPort <= 0 || cfg.Server.APIPort > 65535 {
		return fmt.Errorf("invalid API port: %d", cfg.Server.APIPort)
	}

	// Validate batch settings
	if cfg.Db.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive: %d", cfg.Db.BatchSize)
	}
	if cfg.Db.FlushInterval <= 0 {
		return fmt.Errorf("flush interval must be positive: %v", cfg.Db.FlushInterval)
	}

	// Validate connection pool settings
	if cfg.Db.MaxConnections <= 0 {
		return fmt.Errorf("max connections must be positive: %d", cfg.Db.MaxConnections)
	}
	if cfg.Db.MaxIdleConns < 0 {
		return fmt.Errorf("max idle connections must be non-negative: %d", cfg.Db.MaxIdleConns)
	}
	if cfg.Db.MaxIdleConns > cfg.Db.MaxConnections {
		return fmt.Errorf("max idle connections (%d) cannot exceed max connections (%d)",
			cfg.Db.MaxIdleConns, cfg.Db.MaxConnections)
	}

	// Validate prefetch count
	if cfg.AMQP.PrefetchCount <= 0 {
		return fmt.Errorf("prefetch count must be positive: %d", cfg.AMQP.PrefetchCount)
	}

	return nil
}
