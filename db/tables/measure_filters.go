package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type MeasureFilters struct {
	TableName string
}

func (table MeasureFilters) GetTableName() string {
	return table.TableName
}

func (table MeasureFilters) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
     name,
	 user_id,
	 shared,
	 description,
	 data,
	 %v,
	 %v
	FROM
     %v
    ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		data_conversion.ReflectTimeFormat("updated_at"),
		table.TableName)
}

func (table MeasureFilters) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tNameSource sql.NullString
	var tUserIDSource sql.NullInt64
	var tSharedSource bool
	var tDescriptionSource sql.NullString
	var tDataSource sql.NullString
	var tCreatedATSource sql.NullString
	var tUpdatedATSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tNameSource, &tUserIDSource, &tSharedSource,
		&tDescriptionSource, &tDataSource, &tCreatedATSource, &tUpdatedATSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tNameDest := data_conversion.StringOrNullToString(tNameSource)
	tUserIDDest := data_conversion.IntegerOrNullToString(tUserIDSource)
	tSharedDest := data_conversion.PgBoolToMySQLTinyint(tSharedSource)
	tDescriptionDest := data_conversion.StringOrNullToString(tDescriptionSource)
	tDataDest := data_conversion.StringOrNullToString(tDataSource)
	tCreatedATDest := data_conversion.StringOrNullToString(tCreatedATSource)
	tUpdatedATDest := data_conversion.StringOrNullToString(tUpdatedATSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(%v,
		%v,
		%v,
		%v,
		%v,
		%v,
		%v,
		%v)`,
		table.TableName,
		tIDSource,
		tNameDest,
		tUserIDDest,
		tSharedDest,
		tDescriptionDest,
		tDataDest,
		tCreatedATDest,
		tUpdatedATDest)
}

func (table MeasureFilters) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
