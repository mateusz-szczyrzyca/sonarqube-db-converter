package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type PermissionTemplates struct {
	TableName string
}

func (table PermissionTemplates) GetTableName() string {
	return table.TableName
}

func (table PermissionTemplates) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
     name,
	 kee,
	 description,
	 %v,
	 %v,
	 key_pattern
	FROM
     %v
    ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		data_conversion.ReflectTimeFormat("updated_at"),
		table.TableName)
}

func (table PermissionTemplates) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tNameSource sql.NullString
	var tKeeSource sql.NullString
	var tDescriptionSource sql.NullString
	var tCreatedATSource sql.NullString
	var tUpdatedATSource sql.NullString
	var tKeyPatternSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tNameSource, &tKeeSource,
		&tDescriptionSource, &tCreatedATSource, &tUpdatedATSource, &tKeyPatternSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tNameDest := data_conversion.StringOrNullToString(tNameSource)
	tKeeDest := data_conversion.StringOrNullToString(tKeeSource)
	tDescriptionDest := data_conversion.StringOrNullToString(tDescriptionSource)
	tCreatedATDest := data_conversion.StringOrNullToString(tCreatedATSource)
	tUpdatedATDest := data_conversion.StringOrNullToString(tUpdatedATSource)
	tKeyPatternDest := data_conversion.StringOrNullToString(tKeyPatternSource)

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
		tNameDest,
		tKeeDest,
		tDescriptionDest,
		tCreatedATDest,
		tUpdatedATDest,
		tKeyPatternDest)
}

func (table PermissionTemplates) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
