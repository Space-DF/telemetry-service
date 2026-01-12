package timescaledb

import (
	"context"
	"fmt"
	"log"

	"github.com/stephenafamo/bob"
)

// WithOrgTx begins a transaction, sets the search_path for the provided org,
// runs the provided function passing the transaction as the db handle, and commits the transaction.
// Exported for use by subpackages.
func (c *Client) WithOrgTx(ctx context.Context, org string, fn func(ctx context.Context, tx bob.Tx) error) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	setSQL := fmt.Sprintf("SET LOCAL search_path = %s,public", pqQuoteIdentifier(org))
	log.Printf("Executing SQL for transaction: %s", setSQL)
	if _, err := tx.ExecContext(ctx, setSQL); err != nil {
		return fmt.Errorf("failed to set search_path for org '%s': %w", org, err)
	}

	if err := fn(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction for org '%s': %w", org, err)
	}

	return nil
}
