package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type GroupRoles struct {
	TableName string
}

func (table GroupRoles) GetTableName() string {
	return table.TableName
}

func (table GroupRoles) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT * FROM %v ORDER BY id ASC`, table.TableName)
}

func (table GroupRoles) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tGroupIDSource sql.NullInt64
	var tResourceIDSource sql.NullInt64
	var tRoleSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tGroupIDSource, &tResourceIDSource, &tRoleSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tGroupIDDest := data_conversion.IntegerOrNullToString(tGroupIDSource)
	tResourceIDDest := data_conversion.IntegerOrNullToString(tResourceIDSource)
	tRoleDest := data_conversion.StringOrNullToString(tRoleSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v VALUES(%v,%v,%v,%v)`,
		table.TableName,
		tIDSource,
		tGroupIDDest,
		tResourceIDDest,
		tRoleDest)
}

func (table GroupRoles) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
