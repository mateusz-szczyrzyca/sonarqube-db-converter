package tables

import (
	"database/sql"
	"fmt"
	"time"
)

// CopyTableActiveRuleParameters source table: active_rule_parameters
func CopyTableActiveRuleParameters(pgsql, mysql *sql.DB) error {
	const pgsqlSourceTableName = "active_rule_parameters"

	// source table: project_links
	pgsqlQueryString := fmt.Sprintf("SELECT * FROM %v", pgsqlSourceTableName)
	sourceRows, errorSourceRetrievalQuery := pgsql.Query(pgsqlQueryString)

	if errorSourceRetrievalQuery != nil {
		return errorSourceRetrievalQuery
	}

	timeStart := time.Now()
	totalNumOfSourceRows := 0
	for sourceRows.Next() {
		var tID int64
		var tActiveRuleID int64
		var tRulesParameterID int64
		var tValue string
		var tRulesParameterKey string

		errorSourceRowsScan := sourceRows.Scan(&tID, &tActiveRuleID, &tRulesParameterID, &tValue, &tRulesParameterKey)
		if errorSourceRowsScan != nil {
			return errorSourceRowsScan
		}

		// We have to update main key sequence to reflect it's counterpart in PostgreSQL for data consistency from SQ point of view
		mysqlQueryUpdateSeqString := fmt.Sprintf("ALTER TABLE %v auto_increment = %v", pgsqlSourceTableName, tID)
		_, errorCurrentSequenceSetQuery := mysql.Exec(mysqlQueryUpdateSeqString)
		if errorCurrentSequenceSetQuery != nil {
			return errorCurrentSequenceSetQuery
		}

		mysqlQueryString := fmt.Sprintf(`INSERT INTO %v 
		VALUES(null,
		'%v',
		'%v',
		'%v',
		'%v')`, pgsqlSourceTableName, tActiveRuleID, tRulesParameterID, tValue, tRulesParameterKey)

		_, errorInsertDataToDestination := mysql.Exec(mysqlQueryString)
		if errorInsertDataToDestination != nil {
			return errorInsertDataToDestination
		}
		totalNumOfSourceRows++
	}
	timeStop := time.Since(timeStart)
	fmt.Printf("%v: successfully converted %v rows (%v)\n", pgsqlSourceTableName, totalNumOfSourceRows, timeStop)

	return nil
}
