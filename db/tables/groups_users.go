package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type GroupsUsers struct {
	TableName string
}

func (table GroupsUsers) GetTableName() string {
	return table.TableName
}

func (table GroupsUsers) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT * FROM %v ORDER BY user_id, group_id ASC`, table.TableName)
}

func (table GroupsUsers) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tUserIDSource sql.NullInt64
	var tGroupIDSource sql.NullInt64

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tUserIDSource, &tGroupIDSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tUserIDDest := data_conversion.IntegerOrNullToString(tUserIDSource)
	tGroupIDDest := data_conversion.IntegerOrNullToString(tGroupIDSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v VALUES(%v, %v)`,
		table.TableName, tUserIDDest, tGroupIDDest)
}

func (table GroupsUsers) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`desc %v;`,
		table.TableName)
}
