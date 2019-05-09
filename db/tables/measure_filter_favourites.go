package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type MeasureFilterFavourites struct {
	TableName string
}

func (table MeasureFilterFavourites) GetTableName() string {
	return table.TableName
}

func (table MeasureFilterFavourites) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
	 user_id,
	 measure_filter_id,
	 %v
	FROM
     %v
    ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		table.TableName)
}

func (table MeasureFilterFavourites) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tUserIDSource sql.NullInt64
	var tMeasureFilterIDSource sql.NullInt64
	var tCreatedATSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tUserIDSource, &tMeasureFilterIDSource, &tCreatedATSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tUserIDDest := data_conversion.IntegerOrNullToString(tUserIDSource)
	tMeasureFilterIDDest := data_conversion.IntegerOrNullToString(tMeasureFilterIDSource)
	tCreatedATDest := data_conversion.StringOrNullToString(tCreatedATSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(%v,
		%v,
		%v,
		%v)`,
		table.TableName,
		tIDSource,
		tUserIDDest,
		tMeasureFilterIDDest,
		tCreatedATDest)
}

func (table MeasureFilterFavourites) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
