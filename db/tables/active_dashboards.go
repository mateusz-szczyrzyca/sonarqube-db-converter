package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type ActiveDashboards struct {
	TableName string
}

func (table ActiveDashboards) GetDataFromDataSource() string {
	return fmt.Sprintf("SELECT * FROM %v ORDER BY id ASC", table.TableName)
}

func (table ActiveDashboards) GetTableName() string {
	return table.TableName
}

func (table ActiveDashboards) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tDashboardID int64
	var tUserIDSource sql.NullInt64
	var tOrderIndexSource sql.NullInt64

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tDashboardID, &tUserIDSource, &tOrderIndexSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tUserIDDest := data_conversion.IntegerOrNullToString(tUserIDSource)
	tOrderIndexDest := data_conversion.IntegerOrNullToString(tOrderIndexSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v 
		VALUES(%v,
		%v,
		%v,
		%v)`,
		table.TableName,
		tIDSource,
		tDashboardID,
		tUserIDDest,
		tOrderIndexDest)
}

func (table ActiveDashboards) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
