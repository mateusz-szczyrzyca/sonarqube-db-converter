package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type WidgetProperties struct {
	TableName string
}

func (table WidgetProperties) GetTableName() string {
	return table.TableName
}

func (table WidgetProperties) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
	 widget_id,
	 kee,
	 text_value
    FROM
     %v
    ORDER BY id ASC`, table.TableName)
}

func (table WidgetProperties) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tWidgetIDSource sql.NullInt64
	var tKeeSource sql.NullString
	var tTextValueSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tWidgetIDSource, &tKeeSource,
		&tTextValueSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tWidgetIDDest := data_conversion.IntegerOrNullToString(tWidgetIDSource)
	tKeeDest := data_conversion.StringOrNullToString(tKeeSource)
	tTextValueDest := data_conversion.StringOrNullToString(tTextValueSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(%v,
		 %v,
		 %v,
		 %v)`,
		table.TableName,
		tIDSource,
		tWidgetIDDest,
		tKeeDest,
		tTextValueDest)
}

func (table WidgetProperties) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
