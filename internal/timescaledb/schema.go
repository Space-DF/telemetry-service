package timescaledb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	dbpkg "github.com/Space-DF/telemetry-service/pkgs/db"
	"go.uber.org/zap"
)

var (
	migrationHashOnce sync.Once
	migrationHash     string
)

// computeMigrationHash returns a SHA-256 hash of all migration SQL files.
// Computed once per process lifetime — cached for subsequent calls.
func computeMigrationHash(migrationPath string) string {
	migrationHashOnce.Do(func() {
		files, err := filepath.Glob(filepath.Join(migrationPath, "*.sql"))
		if err != nil || len(files) == 0 {
			migrationHash = "none"
			return
		}
		sort.Strings(files)

		h := sha256.New()
		for _, f := range files {
			data, err := os.ReadFile(f)
			if err != nil {
				continue
			}
			h.Write([]byte(filepath.Base(f)))
			h.Write(data)
		}
		migrationHash = hex.EncodeToString(h.Sum(nil))[:16]
	})
	return migrationHash
}

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

// CreateSchemaAndTables ensures the schema exists and runs migrations.
// Migrations are skipped when the migration files haven't changed since the last run,
// tracked per-schema via a lightweight fingerprint table in the public schema.
func (c *Client) CreateSchemaAndTables(ctx context.Context, orgSlug string) error {
	if err := c.CreateSchema(ctx, orgSlug); err != nil {
		return err
	}

	if c.connStr == "" {
		return fmt.Errorf("no connection string available to run migrations")
	}

	migrationPath := "pkgs/db/migrations"
	currentHash := computeMigrationHash(migrationPath)

	storedHash, err := c.getSchemaMigrationFingerprint(ctx, orgSlug)
	if err == nil && storedHash == currentHash {
		c.Logger.Debug("Migrations unchanged, skipping",
			zap.String("org", orgSlug),
			zap.String("hash", currentHash),
		)
		return nil
	}

	parsed, err := url.Parse(c.connStr)
	if err != nil {
		return fmt.Errorf("failed to parse connection string for migrations: %w", err)
	}

	escaped := strings.ReplaceAll(orgSlug, `"`, `""`)
	quotedSchema := fmt.Sprintf(`"%s"`, escaped)

	q := parsed.Query()
	q.Set("options", fmt.Sprintf("-c search_path=%s,public", quotedSchema))
	parsed.RawQuery = q.Encode()

	if err := dbpkg.Migrate(parsed, migrationPath); err != nil {
		return fmt.Errorf("failed to run migrations for schema '%s': %w", orgSlug, err)
	}

	if err := c.setSchemaMigrationFingerprint(ctx, orgSlug, currentHash); err != nil {
		c.Logger.Warn("Failed to store migration fingerprint",
			zap.String("org", orgSlug),
			zap.Error(err),
		)
	}

	c.Logger.Info("Ran migrations for organization schema",
		zap.String("org", orgSlug),
		zap.String("hash", currentHash),
	)
	return nil
}

func (c *Client) getSchemaMigrationFingerprint(ctx context.Context, orgSlug string) (string, error) {
	row := c.DB.QueryRowContext(ctx, `
		SELECT value FROM public._schema_migration_fingerprint WHERE schema_name = $1
	`, orgSlug)
	var value string
	if err := row.Scan(&value); err != nil {
		return "", err
	}
	return value, nil
}

func (c *Client) setSchemaMigrationFingerprint(ctx context.Context, orgSlug, hash string) error {
	if _, err := c.DB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS public._schema_migration_fingerprint (
			schema_name TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return err
	}

	_, err := c.DB.ExecContext(ctx, `
		INSERT INTO public._schema_migration_fingerprint (schema_name, value)
		VALUES ($1, $2)
		ON CONFLICT (schema_name) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
	`, orgSlug, hash)
	return err
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

	_, _ = c.DB.ExecContext(ctx,
		`DELETE FROM public._schema_migration_fingerprint WHERE schema_name = $1`,
		orgSlug,
	)

	c.Logger.Info("Dropped database schema for organization", zap.String("org", orgSlug))
	return nil
}
