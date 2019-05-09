package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type Activities struct {
	TableName string
}

func (table Activities) GetTableName() string {
	return table.TableName
}

func (table Activities) GetDataFromDataSource() string {
	return fmt.Sprintf(`
	SELECT
	 id,
	 %v,
	 user_login,
	 data_field,
	 log_type,
	 log_action,
	 log_message,
	 log_key
	FROM
	 %v
	ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		table.TableName)
}

func (table Activities) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tCreatedATSource sql.NullString
	var tUserLoginSource sql.NullString
	var tDataFieldSource sql.NullString
	var tLogTypeSource sql.NullString
	var tLogActionSource sql.NullString
	var tLogMessageSource sql.NullString
	var tLogKeySource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tCreatedATSource, &tUserLoginSource, &tDataFieldSource,
		&tLogTypeSource, &tLogActionSource, &tLogMessageSource, &tLogKeySource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tCreatedATDest := data_conversion.StringOrNullToString(tCreatedATSource)
	tUserLoginDest := data_conversion.StringOrNullToString(tUserLoginSource)
	tDataFieldDest := data_conversion.StringOrNullToString(tDataFieldSource)
	tLogTypeDest := data_conversion.StringOrNullToString(tLogTypeSource)
	tLogActionDest := data_conversion.StringOrNullToString(tLogActionSource)
	tLogMessageDest := data_conversion.StringOrNullToString(tLogMessageSource)
	tLogKeyDest := data_conversion.StringOrNullToString(tLogKeySource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(%v,
		 %v,
		 %v,
		 %v,
		 %v,
		 %v,
		 %v,
		 %v)`,
		table.TableName,
		tIDSource,
		tCreatedATDest,
		tUserLoginDest,
		tDataFieldDest,
		tLogTypeDest,
		tLogActionDest,
		tLogMessageDest,
		tLogKeyDest)
}

func (table Activities) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
