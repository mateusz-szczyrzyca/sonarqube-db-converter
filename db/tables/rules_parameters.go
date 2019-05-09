package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type RulesParameters struct {
	TableName string
}

func (table RulesParameters) GetTableName() string {
	return table.TableName
}

func (table RulesParameters) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
	 rule_id,
	 name,
	 description,
     param_type,
	 default_value
	FROM
     %v
    ORDER BY id ASC`, table.TableName)
}

func (table RulesParameters) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tRuleIDSource sql.NullInt64
	var tNameSource sql.NullString
	var tDescriptionSource sql.NullString
	var tParamTypeSource sql.NullString
	var tDefaultValueSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tRuleIDSource, &tNameSource,
		&tDescriptionSource, &tParamTypeSource, &tDefaultValueSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tRuleIDDest := data_conversion.IntegerOrNullToString(tRuleIDSource)
	tNameDest := data_conversion.StringOrNullToString(tNameSource)
	tDescriptionDest := data_conversion.StringOrNullToString(tDescriptionSource)
	tParamTypeDest := data_conversion.StringOrNullToString(tParamTypeSource)
	tDefaultValueDest := data_conversion.StringOrNullToString(tDefaultValueSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(%v,
		%v,
		%v,
		%v,
		%v,
		%v)`,
		table.TableName,
		tIDSource,
		tRuleIDDest,
		tNameDest,
		tDescriptionDest,
		tParamTypeDest,
		tDefaultValueDest)
}

func (table RulesParameters) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
