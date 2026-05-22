package config

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

// Config represents the service configuration
type Config struct {
	Server        Server        `mapstructure:"server"`
	AMQP          AMQP          `mapstructure:"amqp"`
	OrgEvents     OrgEvents     `mapstructure:"org_events"`
	Db            Db            `mapstructure:"db"`
	Notifications Notifications `mapstructure:"notifications"`
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
	LogLevel            string `mapstructure:"log_level"`
	APIPort             int    `mapstructure:"api_port"`
	AlertsProcessorsCfg string `mapstructure:"alerts_processors_path"`
	EventRulesDir       string `mapstructure:"event_rules_dir"`
}

// Notifications contains web push notification configuration
type Notifications struct {
	VAPIDPublicKey  string `mapstructure:"vapid_public_key"`
	VAPIDPrivateKey string `mapstructure:"vapid_private_key"`
	VAPIDSubject    string `mapstructure:"vapid_subject"`
	TTLSeconds      int    `mapstructure:"ttl_seconds"`
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
	Name            string        `mapstructure:"name"`
	Username        string        `mapstructure:"username"`
	Password        string        `mapstructure:"password"`
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	BatchSize       int           `mapstructure:"batch_size"`
	FlushInterval   time.Duration `mapstructure:"flush_interval"`
	MaxConnections  int           `mapstructure:"max_connections"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

// DefaultDb returns default database configuration
func DefaultDb() Db {
	return Db{
		BatchSize:       1000,
		FlushInterval:   time.Second,
		MaxConnections:  25,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	}
}

// LoadConfig loads configuration from file and environment variables
func LoadConfig() (*Config, error) {
	var cfg Config
	vp := viper.New()

	vp.SetConfigFile("configs/config.yaml")
	if err := vp.ReadInConfig(); err != nil {
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if !errors.As(err, &configFileNotFoundError) {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}
	_ = godotenv.Load(".env")

	vp.AutomaticEnv()
	vp.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	bindEnv := func(key string, envs ...string) error {
		args := append([]string{key}, envs...)
		if err := vp.BindEnv(args...); err != nil {
			return fmt.Errorf("bind env %s: %w", key, err)
		}
		return nil
	}
	for _, binding := range []struct {
		key  string
		envs []string
	}{
		{key: "amqp.broker_url", envs: []string{"AMQP_BROKER_URL"}},
		{key: "db.name", envs: []string{"DB_NAME"}},
		{key: "db.username", envs: []string{"DB_USERNAME", "DB_USER"}},
		{key: "db.password", envs: []string{"DB_PASSWORD"}},
		{key: "db.host", envs: []string{"DB_HOST"}},
		{key: "db.port", envs: []string{"DB_PORT"}},
		{key: "notifications.vapid_public_key", envs: []string{"NOTIFICATIONS_VAPID_PUBLIC_KEY"}},
		{key: "notifications.vapid_private_key", envs: []string{"NOTIFICATIONS_VAPID_PRIVATE_KEY"}},
		{key: "notifications.vapid_subject", envs: []string{"NOTIFICATIONS_VAPID_SUBJECT"}},
		{key: "notifications.ttl_seconds", envs: []string{"NOTIFICATIONS_TTL_SECONDS"}},
	} {
		if err := bindEnv(binding.key, binding.envs...); err != nil {
			return nil, err
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

	// Validate conn max lifetime (must be positive or zero for no limit)
	if cfg.Db.ConnMaxLifetime < 0 {
		return fmt.Errorf("conn max lifetime must be non-negative: %v", cfg.Db.ConnMaxLifetime)
	}

	// Validate shutdown timeout (default to 30 seconds if not set)
	if cfg.Db.ShutdownTimeout == 0 {
		cfg.Db.ShutdownTimeout = 30 * time.Second
	}
	if cfg.Db.ShutdownTimeout < 0 {
		return fmt.Errorf("shutdown timeout must be non-negative: %v", cfg.Db.ShutdownTimeout)
	}

	if cfg.Notifications.TTLSeconds <= 0 {
		cfg.Notifications.TTLSeconds = 60
	}

	hasNotificationConfig := cfg.Notifications.VAPIDPublicKey != "" ||
		cfg.Notifications.VAPIDPrivateKey != "" ||
		cfg.Notifications.VAPIDSubject != ""
	if hasNotificationConfig &&
		(cfg.Notifications.VAPIDPublicKey == "" ||
			cfg.Notifications.VAPIDPrivateKey == "" ||
			cfg.Notifications.VAPIDSubject == "") {
		// Treat partial notification config as disabled so the service can still boot
		// when web push is not in use.
		log.Printf("warning: incomplete notification VAPID configuration detected; disabling push notifications until vapid_public_key, vapid_private_key, and vapid_subject are all set")
		cfg.Notifications.VAPIDPublicKey = ""
		cfg.Notifications.VAPIDPrivateKey = ""
		cfg.Notifications.VAPIDSubject = ""
	}

	// Validate prefetch count
	if cfg.AMQP.PrefetchCount <= 0 {
		return fmt.Errorf("prefetch count must be positive: %d", cfg.AMQP.PrefetchCount)
	}

	return nil
}
