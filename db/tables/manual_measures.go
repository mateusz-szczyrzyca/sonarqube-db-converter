package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type ManualMeasures struct {
	TableName string
}

func (table ManualMeasures) GetTableName() string {
	return table.TableName
}

func (table ManualMeasures) GetDataFromDataSource() string {
	return fmt.Sprintf(`
	SELECT
	 id,
	 metric_id,
	 value,
	 text_value,
	 user_login,
	 description,
	 created_at,
	 updated_at,
	 component_uuid
	FROM
	 %v
	ORDER BY id ASC`,
		table.TableName)
}

func (table ManualMeasures) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tMetricIDSource sql.NullInt64
	var tValueSource sql.NullFloat64
	var tTextValueSource sql.NullInt64
	var tUserLoginSource sql.NullString
	var tDescriptionSource sql.NullString
	var tCreatedATSource sql.NullInt64
	var tUpdatedATSource sql.NullInt64
	var tComponentUUIDSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tMetricIDSource, &tValueSource, &tTextValueSource,
		&tUserLoginSource, &tDescriptionSource, &tCreatedATSource, &tUpdatedATSource, &tComponentUUIDSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tMetricIDDest := data_conversion.IntegerOrNullToString(tMetricIDSource)
	tValueDest := data_conversion.FloatOrNullToString(tValueSource)
	tTextValueDest := data_conversion.IntegerOrNullToString(tTextValueSource)
	tUserLoginDest := data_conversion.StringOrNullToString(tUserLoginSource)
	tDescriptionDest := data_conversion.StringOrNullToString(tDescriptionSource)
	tCreatedATDest := data_conversion.IntegerOrNullToString(tCreatedATSource)
	tUpdatedATDest := data_conversion.IntegerOrNullToString(tUpdatedATSource)
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
		tMetricIDDest,
		tValueDest,
		tTextValueDest,
		tUserLoginDest,
		tDescriptionDest,
		tCreatedATDest,
		tUpdatedATDest,
		tComponentUUIDDest)
}

func (table ManualMeasures) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id bigint(20) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
