package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type Widgets struct {
	TableName string
}

func (table Widgets) GetTableName() string {
	return table.TableName
}

func (table Widgets) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
	 dashboard_id,
	 widget_key,
	 name,
	 description,
	 column_index,
	 row_index,
	 configured,
	 %v,
	 %v,
	 resource_id
    FROM
     %v
    ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		data_conversion.ReflectTimeFormat("updated_at"),
		table.TableName)
}

func (table Widgets) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tDashboardIDSource sql.NullInt64
	var tWidgetKEYSource sql.NullString
	var tNameSource sql.NullString
	var tDescriptionSource sql.NullString
	var tColumnIndexSource sql.NullInt64
	var tRowIndexSource sql.NullInt64
	var tConfiguredSource bool
	var tCreatedATSource sql.NullString
	var tUpdatedATSource sql.NullString
	var tResourceIDSource sql.NullInt64

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tDashboardIDSource, &tWidgetKEYSource,
		&tNameSource, &tDescriptionSource, &tColumnIndexSource, &tRowIndexSource,
		&tConfiguredSource, &tCreatedATSource, &tUpdatedATSource, &tResourceIDSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tDashboardIDDest := data_conversion.IntegerOrNullToString(tDashboardIDSource)
	tWidgetKEYDest := data_conversion.StringOrNullToString(tWidgetKEYSource)
	tNameDest := data_conversion.StringOrNullToString(tNameSource)
	tDescriptionDest := data_conversion.StringOrNullToString(tDescriptionSource)
	tColumnIndexDest := data_conversion.IntegerOrNullToString(tColumnIndexSource)
	tRowIndexDest := data_conversion.IntegerOrNullToString(tRowIndexSource)
	tConfiguredDest := data_conversion.PgBoolToMySQLTinyint(tConfiguredSource)
	tCreatedATDest := data_conversion.StringOrNullToString(tCreatedATSource)
	tUpdatedATDest := data_conversion.StringOrNullToString(tUpdatedATSource)
	tResourceIDDest := data_conversion.IntegerOrNullToString(tResourceIDSource)

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
		 %v)`,
		table.TableName,
		tIDSource,
		tDashboardIDDest,
		tWidgetKEYDest,
		tNameDest,
		tDescriptionDest,
		tColumnIndexDest,
		tRowIndexDest,
		tConfiguredDest,
		tCreatedATDest,
		tUpdatedATDest,
		tResourceIDDest)
}

func (table Widgets) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
