package main

import (
	"fmt"
	"net/url"

	"github.com/Space-DF/telemetry-service/pkgs/db"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

func cmdMigrate(ctx *cli.Context, logger *zap.Logger) error {
	logger.Info("Running database migrations...")

	dbURL, err := url.Parse(appConfig.Psql.Dsn)
	if err != nil {
		return fmt.Errorf("failed to parse database DSN: %w", err)
	}

	migrationPath := "pkgs/db/migrations"
	if err := db.Migrate(dbURL, migrationPath); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	logger.Info("Database migrations completed successfully")
	return nil
}
