package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type Events struct {
	TableName string
}

func (table Events) GetTableName() string {
	return table.TableName
}

func (table Events) GetDataFromDataSource() string {
	return fmt.Sprintf(`
	SELECT
	 id,
	 name,
	 snapshot_id,
	 category,
	 description,
	 event_data,
	 event_date,
	 created_at,
	 component_uuid
	FROM
	 %v
	ORDER BY id ASC`, table.TableName)
}

func (table Events) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tNameSource sql.NullString
	var tSnapshotIDSource sql.NullInt64
	var tCategorySource sql.NullString
	var tDescriptionSource sql.NullString
	var tEventDataSource sql.NullString
	var tEventDate int64
	var tCreatedAT int64
	var tComponentUUIDSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tNameSource, &tSnapshotIDSource, &tCategorySource,
		&tDescriptionSource, &tEventDataSource, &tEventDate, &tCreatedAT, &tComponentUUIDSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tNameDest := data_conversion.StringOrNullToString(tNameSource)
	tSnapshotIDDest := data_conversion.IntegerOrNullToString(tSnapshotIDSource)
	tCategoryDest := data_conversion.StringOrNullToString(tCategorySource)
	tDescriptionDest := data_conversion.StringOrNullToString(tDescriptionSource)
	tEventDataDest := data_conversion.StringOrNullToString(tEventDataSource)
	tComponentUUIDDest := data_conversion.StringOrNullToString(tComponentUUIDSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(%v,
		%v,
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
		tSnapshotIDDest,
		tCategoryDest,
		tDescriptionDest,
		tEventDataDest,
		tEventDate,
		tCreatedAT,
		tComponentUUIDDest)
}

func (table Events) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
