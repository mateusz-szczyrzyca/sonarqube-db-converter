package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type FileSources struct {
	TableName string
}

func (table FileSources) GetTableName() string {
	return table.TableName
}

func (table FileSources) GetDataFromDataSource() string {
	return fmt.Sprintf(`
	SELECT
	 id,
	 project_uuid,
	 file_uuid,
	 line_hashes,
	 data_hash,
	 created_at,
	 updated_at,
	 src_hash,
	 encode(binary_data, 'hex'),
	 data_type,
	 revision
	FROM
     %v
	ORDER BY id ASC`, table.TableName)
}

func (table FileSources) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tProjectUUIDSource sql.NullString
	var tFileUUIDSource sql.NullString
	var tLineHashesSource sql.NullString
	var tDataHashSource sql.NullString
	var tCreatedATSource sql.NullInt64
	var tUpdatedATSource sql.NullInt64
	var tSrcHashSource sql.NullString
	var tBinaryDataSource sql.NullString
	var tDataTypeSource sql.NullString
	var tRevisionSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tProjectUUIDSource, &tFileUUIDSource,
		&tLineHashesSource, &tDataHashSource, &tCreatedATSource, &tUpdatedATSource, &tSrcHashSource,
		&tBinaryDataSource, &tDataTypeSource, &tRevisionSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tProjectUUIDDest := data_conversion.StringOrNullToString(tProjectUUIDSource)
	tFileUUIDDest := data_conversion.StringOrNullToString(tFileUUIDSource)
	tLineHashesDest := data_conversion.StringOrNullToString(tLineHashesSource)
	tDataHashDest := data_conversion.StringOrNullToString(tDataHashSource)
	tCreatedATDest := data_conversion.IntegerOrNullToString(tCreatedATSource)
	tUpdatedATDest := data_conversion.IntegerOrNullToString(tUpdatedATSource)
	tSrcHashDest := data_conversion.StringOrNullToString(tSrcHashSource)
	tBinaryDataDest := data_conversion.StringOrNullToString(tBinaryDataSource)
	tDataTypeDest := data_conversion.StringOrNullToString(tDataTypeSource)
	tRevisionDest := data_conversion.StringOrNullToString(tRevisionSource)

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
		unhex(%v),
		%v,
		%v)`,
		table.TableName,
		tIDSource,
		tProjectUUIDDest,
		tFileUUIDDest,
		tLineHashesDest,
		tDataHashDest,
		tCreatedATDest,
		tUpdatedATDest,
		tSrcHashDest,
		tBinaryDataDest,
		tDataTypeDest,
		tRevisionDest)
}

func (table FileSources) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id bigint(20) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
