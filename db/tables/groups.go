package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type Groups struct {
	TableName string
}

func (table Groups) GetTableName() string {
	return table.TableName
}

func (table Groups) GetDataFromDataSource() string {
	return fmt.Sprintf(`
	SELECT
	 id,
	 name,
	 description,
	 %v,
	 %v
	FROM
	 %v
	ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		data_conversion.ReflectTimeFormat("updated_at"),
		table.TableName)
}

func (table Groups) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tNameSource sql.NullString
	var tDescriptionSource sql.NullString
	var tCreatedATSource sql.NullString
	var tUpdatedATSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tNameSource, &tDescriptionSource,
		&tCreatedATSource, &tUpdatedATSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tNameDest := data_conversion.StringOrNullToString(tNameSource)
	tDescriptionDest := data_conversion.StringOrNullToString(tDescriptionSource)
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
		tNameDest,
		tDescriptionDest,
		tCreatedATDest,
		tUpdatedATDest)
}

func (table Groups) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
