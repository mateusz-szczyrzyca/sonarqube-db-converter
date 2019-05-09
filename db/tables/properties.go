package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type Properties struct {
	TableName string
}

func (table Properties) GetTableName() string {
	return table.TableName
}

func (table Properties) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
	 prop_key,
	 resource_id,
	 text_value,
	 user_id
    FROM
     %v
    ORDER BY id ASC`, table.TableName)
}

func (table Properties) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tPropKeySource sql.NullString
	var tResourceIDSource sql.NullInt64
	var tTextValueSource sql.NullString
	var tUserIDSource sql.NullInt64

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tPropKeySource, &tResourceIDSource,
		&tTextValueSource, &tUserIDSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tPropKeyDest := data_conversion.StringOrNullToString(tPropKeySource)
	tResourceIDDest := data_conversion.IntegerOrNullToString(tResourceIDSource)
	tTextValueDest := data_conversion.StringOrNullToString(tTextValueSource)
	tUserIDDest := data_conversion.IntegerOrNullToString(tUserIDSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(%v,
		 %v,
		 %v,
		 %v,
		 %v)`,
		table.TableName,
		tIDSource,
		tPropKeyDest,
		tResourceIDDest,
		tTextValueDest,
		tUserIDDest)
}

func (table Properties) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
