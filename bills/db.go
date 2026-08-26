package bills

import "encore.dev/storage/sqldb"

var db = sqldb.NewDatabase("bills", sqldb.DatabaseConfig{
	Migrations: "./migrations",
})
