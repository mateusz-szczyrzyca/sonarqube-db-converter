package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type Notifications struct {
	TableName string
}

func (table Notifications) GetTableName() string {
	return table.TableName
}

func (table Notifications) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT id,encode(data, 'hex') FROM
     %v
    ORDER BY id ASC`, table.TableName)
}

func (table Notifications) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tDataSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tDataSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tDataDest := data_conversion.StringOrNullToString(tDataSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(%v, unhex(%v))`, table.TableName, tIDSource, tDataDest)
}

func (table Notifications) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
