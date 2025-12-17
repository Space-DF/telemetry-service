package core

import (
	"fmt"
	"sync"
	"time"

	"github.com/stephenafamo/bob"
	"go.uber.org/zap"
)

// Base wraps the shared DB handle, logger, and connection metadata.
type Base struct {
	db            bob.DB
	logger        *zap.Logger
	batchSize     int
	flushInterval time.Duration
	connStr       string

	wg sync.WaitGroup
}

// NewBase creates a new TimescaleDB base client and verifies connectivity.
func NewBase(connStr string, batchSize int, flushInterval time.Duration, logger *zap.Logger) (*Base, error) {
	db, err := bob.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Base{
		db:            db,
		logger:        logger,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		connStr:       connStr,
	}, nil
}

func (b *Base) DB() bob.DB {
	return b.db
}

func (b *Base) Logger() *zap.Logger {
	return b.logger
}

func (b *Base) ConnStr() string {
	return b.connStr
}

// HealthCheck checks if TimescaleDB is reachable.
func (b *Base) HealthCheck() error {
	return b.db.Ping()
}

// Wait blocks until all background workers have finished.
func (b *Base) Wait() {
	b.wg.Wait()
}

// Close closes the database connection.
func (b *Base) Close() error {
	return b.db.Close()
}
