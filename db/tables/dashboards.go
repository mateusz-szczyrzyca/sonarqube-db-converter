package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type Dashboards struct {
	TableName string
}

func (table Dashboards) GetTableName() string {
	return table.TableName
}

func (table Dashboards) GetDataFromDataSource() string {
	return fmt.Sprintf(`
	SELECT
	 id,
	 user_id,
	 name,
	 description,
	 column_layout,
	 shared,
	 %v,
	 %v,
	 is_global
	FROM
	 %v
	ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		data_conversion.ReflectTimeFormat("updated_at"),
		table.TableName)
}

func (table Dashboards) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tUserIDSource sql.NullInt64
	var tNameSource sql.NullString
	var tDescriptionSource sql.NullString
	var tColumnLayoutSource sql.NullString
	var tSharedSource bool
	var tCreatedATSource sql.NullString
	var tUpdatedATSource sql.NullString
	var tIsGlobalSource bool

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tUserIDSource, &tNameSource, &tDescriptionSource,
		&tColumnLayoutSource, &tSharedSource, &tCreatedATSource, &tUpdatedATSource, &tIsGlobalSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tUserIDDest := data_conversion.IntegerOrNullToString(tUserIDSource)
	tNameDest := data_conversion.StringOrNullToString(tNameSource)
	tDescriptionDest := data_conversion.StringOrNullToString(tDescriptionSource)
	tColumnLayoutDest := data_conversion.StringOrNullToString(tColumnLayoutSource)
	tSharedDest := data_conversion.PgBoolToMySQLTinyint(tSharedSource)
	tUpdatedATDest := data_conversion.StringOrNullToString(tUpdatedATSource)
	tCreatedATDest := data_conversion.StringOrNullToString(tCreatedATSource)
	tIsGlobalDest := data_conversion.PgBoolToMySQLTinyint(tIsGlobalSource)

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
		tUserIDDest,
		tNameDest,
		tDescriptionDest,
		tColumnLayoutDest,
		tSharedDest,
		tCreatedATDest,
		tUpdatedATDest,
		tIsGlobalDest)
}

func (table Dashboards) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
