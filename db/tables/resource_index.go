package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type ResourceIndex struct {
	TableName string
}

func (table ResourceIndex) GetTableName() string {
	return table.TableName
}

func (table ResourceIndex) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
	 kee,
	 position,
	 name_size,
	 resource_id,
	 root_project_id,
	 qualifier
    FROM
     %v
    ORDER BY id ASC`, table.TableName)
}

func (table ResourceIndex) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tKeeSource sql.NullString
	var tPositionSource sql.NullInt64
	var tNameSizeSource sql.NullInt64
	var tResourceIDSource sql.NullInt64
	var tRootProjectIDSource sql.NullInt64
	var tQualifierSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tKeeSource, &tPositionSource,
		&tNameSizeSource, &tResourceIDSource, &tRootProjectIDSource, &tQualifierSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tKeeDest := data_conversion.StringOrNullToString(tKeeSource)
	tPositionDest := data_conversion.IntegerOrNullToString(tPositionSource)
	tNameSizeDest := data_conversion.IntegerOrNullToString(tNameSizeSource)
	tResourceIDDest := data_conversion.IntegerOrNullToString(tResourceIDSource)
	tRootProjectIDDest := data_conversion.IntegerOrNullToString(tRootProjectIDSource)
	tQualifierDest := data_conversion.StringOrNullToString(tQualifierSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(%v,
		 %v,
		 %v,
		 %v,
		 %v,
		 %v,
		 %v)`,
		table.TableName,
		tIDSource,
		tKeeDest,
		tPositionDest,
		tNameSizeDest,
		tResourceIDDest,
		tRootProjectIDDest,
		tQualifierDest)
}

func (table ResourceIndex) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
