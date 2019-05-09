package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type QualityGateConditions struct {
	TableName string
}

func (table QualityGateConditions) GetTableName() string {
	return table.TableName
}

func (table QualityGateConditions) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
	 qgate_id,
	 metric_id,
	 period,
	 operator,
	 value_error,
	 value_warning,
	 %v,
	 %v
    FROM
     %v
    ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		data_conversion.ReflectTimeFormat("updated_at"),
		table.TableName)
}

func (table QualityGateConditions) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tQGateIDSource sql.NullInt64
	var tMetricIDSource sql.NullInt64
	var tPeriodSource sql.NullInt64
	var tOperatorSource sql.NullString
	var tValueErrorSource sql.NullString
	var tValueWarningSource sql.NullString
	var tCreatedATSource sql.NullString
	var tUpdatedATSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tQGateIDSource, &tMetricIDSource,
		&tPeriodSource, &tOperatorSource, &tValueErrorSource, &tValueWarningSource, &tCreatedATSource,
		&tUpdatedATSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tQGateIDDest := data_conversion.IntegerOrNullToString(tQGateIDSource)
	tMetricIDDest := data_conversion.IntegerOrNullToString(tMetricIDSource)
	tPeriodDest := data_conversion.IntegerOrNullToString(tPeriodSource)
	tOperatorDest := data_conversion.StringOrNullToString(tOperatorSource)
	tValueErrorDest := data_conversion.StringOrNullToString(tValueErrorSource)
	tValueWarningDest := data_conversion.StringOrNullToString(tValueWarningSource)
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
		 %v,
		 %v)`,
		table.TableName,
		tIDSource,
		tQGateIDDest,
		tMetricIDDest,
		tPeriodDest,
		tOperatorDest,
		tValueErrorDest,
		tValueWarningDest,
		tCreatedATDest,
		tUpdatedATDest)
}

func (table QualityGateConditions) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
