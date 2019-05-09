package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type Authors struct {
	TableName string
}

func (table Authors) GetTableName() string {
	return table.TableName
}

func (table Authors) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
	 person_id,
	 login,
	 %v,
	 %v
	FROM
	 %v
	ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		data_conversion.ReflectTimeFormat("updated_at"),
		table.TableName)
}

func (table Authors) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tPersonID int64
	var tLoginSource sql.NullString
	var tCreatedATSource sql.NullString
	var tUpdatedATSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tPersonID, &tLoginSource, &tCreatedATSource,
		&tUpdatedATSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tLoginDest := data_conversion.StringOrNullToString(tLoginSource)
	tUpdatedATDest := data_conversion.StringOrNullToString(tUpdatedATSource)
	tCreatedATDest := data_conversion.StringOrNullToString(tCreatedATSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v 
		VALUES(%v,
		%v,
		%v,
		%v,
		%v)`,
		table.TableName,
		tIDSource,
		tPersonID,
		tLoginDest,
		tCreatedATDest,
		tUpdatedATDest)
}

func (table Authors) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
