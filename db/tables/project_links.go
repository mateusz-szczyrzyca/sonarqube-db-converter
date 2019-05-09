package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type ProjectLinks struct {
	TableName string
}

func (table ProjectLinks) GetTableName() string {
	return table.TableName
}

func (table ProjectLinks) GetDataFromDataSource() string {
	return fmt.Sprintf("SELECT * FROM %v ORDER BY id ASC;", table.TableName)
}

func (table ProjectLinks) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tLinkTypeSource sql.NullString
	var tNameSource sql.NullString
	var tHrefSource sql.NullString
	var tComponentUUIDSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tLinkTypeSource, &tNameSource, &tHrefSource,
		&tComponentUUIDSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tLinkTypeDest := data_conversion.StringOrNullToString(tLinkTypeSource)
	tNameDest := data_conversion.StringOrNullToString(tNameSource)
	tHrefDest := data_conversion.StringOrNullToString(tHrefSource)
	tComponentUUIDDest := data_conversion.StringOrNullToString(tComponentUUIDSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(%v,
		%v,
		%v,
		%v,
		%v)`,
		table.TableName,
		tIDSource,
		tLinkTypeDest,
		tNameDest,
		tHrefDest,
		tComponentUUIDDest)
}

func (table ProjectLinks) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
