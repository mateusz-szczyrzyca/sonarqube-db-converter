package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type IssueFilterFavourites struct {
	TableName string
}

func (table IssueFilterFavourites) GetTableName() string {
	return table.TableName
}

func (table IssueFilterFavourites) GetDataFromDataSource() string {
	return fmt.Sprintf(`
	SELECT
	 id,
	 user_login,
	 issue_filter_id,
	 %v
	FROM
     %v
	ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		table.TableName)
}

func (table IssueFilterFavourites) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tUserLoginSource sql.NullString
	var tIssueFilterIDSource sql.NullInt64
	var tCreatedATSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tUserLoginSource, &tIssueFilterIDSource, &tCreatedATSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tUserLoginDest := data_conversion.StringOrNullToString(tUserLoginSource)
	tIssueFilterIDDest := data_conversion.IntegerOrNullToString(tIssueFilterIDSource)
	tCreatedATDest := data_conversion.StringOrNullToString(tCreatedATSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(%v,
		%v,
		%v,
		%v)`,
		table.TableName,
		tIDSource,
		tUserLoginDest,
		tIssueFilterIDDest,
		tCreatedATDest)
}

func (table IssueFilterFavourites) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
