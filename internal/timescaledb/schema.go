package timescaledb

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	dbpkg "github.com/Space-DF/telemetry-service/pkgs/db"
	"go.uber.org/zap"
)

// CreateSchema creates a PostgreSQL schema for the given organization if it doesn't exist.
func (c *Client) CreateSchema(ctx context.Context, orgSlug string) error {
	if orgSlug == "" {
		return fmt.Errorf("empty organization slug")
	}

	escaped := strings.ReplaceAll(orgSlug, `"`, `""`)
	query := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, escaped)

	if _, err := c.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create schema '%s': %w", orgSlug, err)
	}

	c.Logger.Info("Ensured database schema for organization", zap.String("org", orgSlug))
	return nil
}

// CreateSchemaAndTables ensures the schema exists and creates required tables
// for telemetry within that schema: device_locations and schema_migrations.
func (c *Client) CreateSchemaAndTables(ctx context.Context, orgSlug string) error {
	if err := c.CreateSchema(ctx, orgSlug); err != nil {
		return err
	}

	if c.connStr == "" {
		return fmt.Errorf("no connection string available to run migrations")
	}

	parsed, err := url.Parse(c.connStr)
	if err != nil {
		return fmt.Errorf("failed to parse connection string for migrations: %w", err)
	}

	q := parsed.Query()
	q.Set("options", fmt.Sprintf("-c search_path=%s,public", orgSlug))
	parsed.RawQuery = q.Encode()

	migrationPath := "pkgs/db/migrations"

	if err := dbpkg.Migrate(parsed, migrationPath); err != nil {
		return fmt.Errorf("failed to run migrations for schema '%s': %w", orgSlug, err)
	}

	c.Logger.Info("Ran migrations for organization schema", zap.String("org", orgSlug))
	return nil
}

// DropSchema drops a PostgreSQL schema for the given organization.
func (c *Client) DropSchema(ctx context.Context, orgSlug string) error {
	if orgSlug == "" {
		return fmt.Errorf("empty organization slug")
	}

	escaped := strings.ReplaceAll(orgSlug, `"`, `""`)
	query := fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, escaped)

	if _, err := c.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to drop schema '%s': %w", orgSlug, err)
	}

	c.Logger.Info("Dropped database schema for organization", zap.String("org", orgSlug))
	return nil
}
