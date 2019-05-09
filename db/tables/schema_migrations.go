package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type SchemaMigrations struct {
	TableName string
}

func (table SchemaMigrations) GetTableName() string {
	return table.TableName
}

func (table SchemaMigrations) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT version FROM %v ORDER BY version ASC`, table.TableName)
}

func (table SchemaMigrations) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tVersionSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tVersionSource)

	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tVersionDest := data_conversion.StringOrNullToString(tVersionSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v VALUES(%v)`,
		table.TableName, tVersionDest)
}

func (table SchemaMigrations) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`select now() as %v;`, table.TableName)
}
