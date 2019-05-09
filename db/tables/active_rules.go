package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type ActiveRules struct {
	TableName string
}

func (table ActiveRules) GetTableName() string {
	return table.TableName
}

func (table ActiveRules) GetDataFromDataSource() string {
	return fmt.Sprintf(`
	SELECT
	 id,
	 profile_id,
	 rule_id,
	 failure_level,
	 inheritance,
	 %v,
	 %v
	FROM
	 %v
	ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		data_conversion.ReflectTimeFormat("updated_at"),
		table.TableName)
}

func (table ActiveRules) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tProfileID int64
	var tRuleID int64
	var tFailureLevel int64
	var tInheritanceSource sql.NullString
	var tCreatedATSource sql.NullString
	var tUpdatedATSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tProfileID, &tRuleID, &tFailureLevel,
		&tInheritanceSource, &tCreatedATSource, &tUpdatedATSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tInheritanceDest := data_conversion.StringOrNullToString(tInheritanceSource)
	tUpdatedATDest := data_conversion.StringOrNullToString(tUpdatedATSource)
	tCreatedATDest := data_conversion.StringOrNullToString(tCreatedATSource)

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
		tProfileID,
		tRuleID,
		tFailureLevel,
		tInheritanceDest,
		tCreatedATDest,
		tUpdatedATDest)
}

func (table ActiveRules) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
