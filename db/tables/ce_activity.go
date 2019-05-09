package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type CeActivity struct {
	TableName string
}

func (table CeActivity) GetTableName() string {
	return table.TableName
}

func (table CeActivity) GetDataFromDataSource() string {
	return fmt.Sprintf(`
	SELECT
	 id,
	 uuid,
	 task_type,
	 component_uuid,
	 status,
	 is_last,
	 is_last_key,
	 submitter_login,
	 submitted_at,
	 started_at,
	 executed_at,
	 created_at,
	 updated_at,
	 execution_time_ms
	FROM
	 %v
	ORDER BY id ASC`, table.TableName)
}

func (table CeActivity) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tUUIDSource sql.NullString
	var tTaskTypeSource sql.NullString
	var tComponentUUIDSource sql.NullString
	var tStatusSource sql.NullString
	var tIsLastSource bool
	var tIsLastKeySource sql.NullString
	var tSubmitterLoginSource sql.NullString
	var tSubmittedAT int64
	var tStartedATSource sql.NullInt64
	var tExecutedATSource sql.NullInt64
	var tCreatedAT int64
	var tUpdatedAT int64
	var tExecutionTimeMSSource sql.NullInt64

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tUUIDSource, &tTaskTypeSource, &tComponentUUIDSource,
		&tStatusSource, &tIsLastSource, &tIsLastKeySource, &tSubmitterLoginSource, &tSubmittedAT,
		&tStartedATSource, &tExecutedATSource, &tCreatedAT, &tUpdatedAT, &tExecutionTimeMSSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tUUIDDest := data_conversion.StringOrNullToString(tUUIDSource)
	tTaskTypeDest := data_conversion.StringOrNullToString(tTaskTypeSource)
	tComponentUUIDDest := data_conversion.StringOrNullToString(tComponentUUIDSource)
	tStatusDest := data_conversion.StringOrNullToString(tStatusSource)
	tIsLastDest := data_conversion.PgBoolToMySQLTinyint(tIsLastSource)
	tIsLastKeyDest := data_conversion.StringOrNullToString(tIsLastKeySource)
	tSubmitterLoginDest := data_conversion.StringOrNullToString(tSubmitterLoginSource)
	tStartedATDest := data_conversion.IntegerOrNullToString(tStartedATSource)
	tExecutedATDest := data_conversion.IntegerOrNullToString(tExecutedATSource)
	tExecutionTimeMSDest := data_conversion.IntegerOrNullToString(tExecutionTimeMSSource)

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
		 %v)`,
		table.TableName,
		tIDSource,
		tUUIDDest,
		tTaskTypeDest,
		tComponentUUIDDest,
		tStatusDest,
		tIsLastDest,
		tIsLastKeyDest,
		tSubmitterLoginDest,
		tSubmittedAT,
		tStartedATDest,
		tExecutedATDest,
		tCreatedAT,
		tUpdatedAT,
		tExecutionTimeMSDest)
}

func (table CeActivity) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
