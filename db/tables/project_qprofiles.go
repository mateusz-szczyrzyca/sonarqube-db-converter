package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type ProjectQprofiles struct {
	TableName string
}

func (table ProjectQprofiles) GetTableName() string {
	return table.TableName
}

func (table ProjectQprofiles) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT * FROM %v
    ORDER BY id ASC`, table.TableName)
}

func (table ProjectQprofiles) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tProjectUUIDSource sql.NullString
	var tProfileKEYSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tProjectUUIDSource, &tProfileKEYSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tProjectUUIDDest := data_conversion.StringOrNullToString(tProjectUUIDSource)
	tProfileKEYDest := data_conversion.StringOrNullToString(tProfileKEYSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(%v, %v, %v)`, table.TableName, tIDSource, tProjectUUIDDest, tProfileKEYDest)
}

func (table ProjectQprofiles) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
