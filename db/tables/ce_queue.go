package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type CeQueue struct {
	TableName string
}

func (table CeQueue) GetTableName() string {
	return table.TableName
}

func (table CeQueue) GetDataFromDataSource() string {
	return fmt.Sprintf(`
	SELECT
	 id,
	 uuid,
	 task_type,
	 component_uuid,
	 status,
	 submitter_login,
	 started_at,
	 created_at,
	 updated_at
	FROM
	 %v
	ORDER BY id ASC`, table.TableName)
}

func (table CeQueue) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tUUIDSource sql.NullString
	var tTaskTypeSource sql.NullString
	var tComponentUUIDSource sql.NullString
	var tStatusSource sql.NullString
	var tSubmitterLoginSource sql.NullString
	var tStartedATSource sql.NullInt64
	var tCreatedAT int64
	var tUpdatedAT int64

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tUUIDSource, &tTaskTypeSource,
		&tComponentUUIDSource, &tStatusSource, &tSubmitterLoginSource, &tStartedATSource,
		&tCreatedAT, &tUpdatedAT)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tUUIDDest := data_conversion.StringOrNullToString(tUUIDSource)
	tTaskTypeDest := data_conversion.StringOrNullToString(tTaskTypeSource)
	tComponentUUIDDest := data_conversion.StringOrNullToString(tComponentUUIDSource)
	tStatusDest := data_conversion.StringOrNullToString(tStatusSource)
	tSubmitterLoginDest := data_conversion.StringOrNullToString(tSubmitterLoginSource)
	tStartedATDest := data_conversion.IntegerOrNullToString(tStartedATSource)

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
		tUUIDDest,
		tTaskTypeDest,
		tComponentUUIDDest,
		tStatusDest,
		tSubmitterLoginDest,
		tStartedATDest,
		tCreatedAT,
		tUpdatedAT)
}

func (table CeQueue) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
