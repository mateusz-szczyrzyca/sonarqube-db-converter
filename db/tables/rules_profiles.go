package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type RulesProfiles struct {
	TableName string
}

func (table RulesProfiles) GetTableName() string {
	return table.TableName
}

func (table RulesProfiles) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
	 name,
	 language,
	 kee,
	 parent_kee,
	 rules_updated_at,
	 %v,
	 %v,
	 is_default
	FROM
     %v
    ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		data_conversion.ReflectTimeFormat("updated_at"),
		table.TableName)
}

func (table RulesProfiles) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tNameSource sql.NullString
	var tLanguageSource sql.NullString
	var tKeeSource sql.NullString
	var tParentKeeSource sql.NullString
	var tRulesUpdatedATSource sql.NullString
	var tCreatedATSource sql.NullString
	var tUpdatedATSource sql.NullString
	var tISDefaultSource bool

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tNameSource, &tLanguageSource, &tKeeSource,
		&tParentKeeSource, &tRulesUpdatedATSource, &tCreatedATSource, &tUpdatedATSource,
		&tISDefaultSource)

	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tNameDest := data_conversion.StringOrNullToString(tNameSource)
	tLanguageDest := data_conversion.StringOrNullToString(tLanguageSource)
	tKeeDest := data_conversion.StringOrNullToString(tKeeSource)
	tParentKeeDest := data_conversion.StringOrNullToString(tParentKeeSource)
	tRulesUpdatedATDest := data_conversion.StringOrNullToString(tRulesUpdatedATSource)
	tCreatedATDest := data_conversion.StringOrNullToString(tCreatedATSource)
	tUpdatedATDest := data_conversion.StringOrNullToString(tUpdatedATSource)
	tISDefaultDest := data_conversion.PgBoolToMySQLTinyint(tISDefaultSource)

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
		tNameDest,
		tLanguageDest,
		tKeeDest,
		tParentKeeDest,
		tRulesUpdatedATDest,
		tCreatedATDest,
		tUpdatedATDest,
		tISDefaultDest)
}

func (table RulesProfiles) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
