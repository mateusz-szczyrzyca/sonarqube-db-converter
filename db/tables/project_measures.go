package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type ProjectMeasures struct {
	TableName string
}

func (table ProjectMeasures) GetTableName() string {
	return table.TableName
}

func (table ProjectMeasures) GetDataFromDataSource() string {
	return fmt.Sprintf(`
	SELECT
	 id,
	 value,
	 metric_id,
	 snapshot_id,
	 rule_id,
	 rules_category_id,
	 text_value,
	 tendency,
	 %v,
	 project_id,
	 alert_status,
	 alert_text,
	 url,
	 description,
	 rule_priority,
	 characteristic_id,
	 person_id,
	 variation_value_1,
	 variation_value_2,
	 variation_value_3,
	 variation_value_4,
	 variation_value_5,
	 encode(measure_data, 'hex')
	FROM
     %v
	ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("measure_date"),
		table.TableName)
}

func (table ProjectMeasures) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tValueSource sql.NullFloat64
	var tMetricIDSource sql.NullInt64
	var tSnapshotIDSource sql.NullInt64
	var tRuleIDSource sql.NullInt64
	var tRulesCategoryIDSource sql.NullInt64
	var tTextValueSource sql.NullString
	var tTendencySource sql.NullInt64
	var tMeasureDateSource sql.NullString
	var tProjectIDSource sql.NullInt64
	var tAlertStatusSource sql.NullString
	var tAlertTextSource sql.NullString
	var tURLSource sql.NullString
	var tDescriptionSource sql.NullString
	var tRulePrioritySource sql.NullInt64
	var tCharacteristicIDSource sql.NullInt64
	var tPersonIDSource sql.NullInt64
	var tVariationValue1Source sql.NullFloat64
	var tVariationValue2Source sql.NullFloat64
	var tVariationValue3Source sql.NullFloat64
	var tVariationValue4Source sql.NullFloat64
	var tVariationValue5Source sql.NullFloat64
	var tMeasureDataSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tValueSource, &tMetricIDSource, &tSnapshotIDSource,
		&tRuleIDSource, &tRulesCategoryIDSource, &tTextValueSource, &tTendencySource, &tMeasureDateSource,
		&tProjectIDSource, &tAlertStatusSource, &tAlertTextSource, &tURLSource, &tDescriptionSource,
		&tRulePrioritySource, &tCharacteristicIDSource, &tPersonIDSource, &tVariationValue1Source,
		&tVariationValue2Source, &tVariationValue3Source, &tVariationValue4Source, &tVariationValue5Source,
		&tMeasureDataSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tValueDest := data_conversion.FloatOrNullToString(tValueSource)
	tMetricIDDest := data_conversion.IntegerOrNullToString(tMetricIDSource)
	tSnapshotIDDest := data_conversion.IntegerOrNullToString(tSnapshotIDSource)
	tRuleIDDest := data_conversion.IntegerOrNullToString(tRuleIDSource)
	tRulesCategoryIDDest := data_conversion.IntegerOrNullToString(tRulesCategoryIDSource)
	tTextValueDest := data_conversion.StringOrNullToString(tTextValueSource)
	tTendencyDest := data_conversion.IntegerOrNullToString(tTendencySource)
	tMeasureDateDest := data_conversion.StringOrNullToString(tMeasureDateSource)
	tProjectIDDest := data_conversion.IntegerOrNullToString(tProjectIDSource)
	tAlertStatusDest := data_conversion.StringOrNullToString(tAlertStatusSource)
	tAlertTextDest := data_conversion.StringOrNullToString(tAlertTextSource)
	tURLDest := data_conversion.StringOrNullToString(tURLSource)
	tDescriptionDest := data_conversion.StringOrNullToString(tDescriptionSource)
	tRulePriorityDest := data_conversion.IntegerOrNullToString(tRulePrioritySource)
	tCharacteristicIDDest := data_conversion.IntegerOrNullToString(tCharacteristicIDSource)
	tPersonIDDest := data_conversion.IntegerOrNullToString(tPersonIDSource)
	tVariationValue1Dest := data_conversion.FloatOrNullToString(tVariationValue1Source)
	tVariationValue2Dest := data_conversion.FloatOrNullToString(tVariationValue2Source)
	tVariationValue3Dest := data_conversion.FloatOrNullToString(tVariationValue3Source)
	tVariationValue4Dest := data_conversion.FloatOrNullToString(tVariationValue4Source)
	tVariationValue5Dest := data_conversion.FloatOrNullToString(tVariationValue5Source)
	tMeasureDataEncodedDest := data_conversion.StringOrNullToString(tMeasureDataSource)

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
		%v,
		%v,
		%v,
		%v,
		%v,
		%v,
		%v,
		%v,
		%v,
		%v,
		%v,
		%v,
		%v,
		%v,
		unhex(%v))`,
		table.TableName,
		tIDSource,
		tValueDest,
		tMetricIDDest,
		tSnapshotIDDest,
		tRuleIDDest,
		tRulesCategoryIDDest,
		tTextValueDest,
		tTendencyDest,
		tMeasureDateDest,
		tProjectIDDest,
		tAlertStatusDest,
		tAlertTextDest,
		tURLDest,
		tDescriptionDest,
		tRulePriorityDest,
		tCharacteristicIDDest,
		tPersonIDDest,
		tVariationValue1Dest,
		tVariationValue2Dest,
		tVariationValue3Dest,
		tVariationValue4Dest,
		tVariationValue5Dest,
		tMeasureDataEncodedDest)
}

func (table ProjectMeasures) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id bigint(20) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
