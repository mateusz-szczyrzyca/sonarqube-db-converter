package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type ActiveRuleParameters struct {
	TableName string
}

func (table ActiveRuleParameters) GetTableName() string {
	return table.TableName
}

func (table ActiveRuleParameters) GetDataFromDataSource() string {
	return fmt.Sprintf("SELECT * FROM %v ORDER BY id ASC", table.TableName)
}

func (table ActiveRuleParameters) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tActiveRuleID int64
	var tRulesParameterID int64
	var tValueSource sql.NullString
	var tRulesParameterKeySource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tActiveRuleID, &tRulesParameterID,
		&tValueSource, &tRulesParameterKeySource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tValueDest := data_conversion.StringOrNullToString(tValueSource)
	tRulesParameterKeyDest := data_conversion.StringOrNullToString(tRulesParameterKeySource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v 
		VALUES(%v,
		%v,
		%v,
		%v,
		%v)`,
		table.TableName,
		tIDSource,
		tActiveRuleID,
		tRulesParameterID,
		tValueDest,
		tRulesParameterKeyDest)
}

func (table ActiveRuleParameters) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
