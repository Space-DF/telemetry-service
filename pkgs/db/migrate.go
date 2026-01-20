package db

import (
	"net/url"
	"time"

	"github.com/amacneil/dbmate/v2/pkg/dbmate"
	_ "github.com/amacneil/dbmate/v2/pkg/driver/postgres"
)

//go:generate bobgen-psql -c ../../configs/config.yaml

func Migrate(conn *url.URL, migrationPath string) error {
	dbMate := dbmate.New(conn)

	dbMate.Verbose = true
	dbMate.WaitBefore = true
	dbMate.AutoDumpSchema = false

	// so that checkWaitCalled returns quickly
	dbMate.WaitInterval = time.Millisecond
	dbMate.WaitTimeout = 5 * time.Millisecond

	// setting the path where the migration scripts are
	dbMate.MigrationsDir = []string{migrationPath}

	return dbMate.Migrate()
}
