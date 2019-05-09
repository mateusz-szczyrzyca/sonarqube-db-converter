package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type LoadedTemplates struct {
	TableName string
}

func (table LoadedTemplates) GetTableName() string {
	return table.TableName
}

func (table LoadedTemplates) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT * FROM %v ORDER BY id ASC`, table.TableName)
}

func (table LoadedTemplates) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tKeeSource sql.NullString
	var tTemplateTypeSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tKeeSource, &tTemplateTypeSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tKeeDest := data_conversion.StringOrNullToString(tKeeSource)
	tTemplateTypeDest := data_conversion.StringOrNullToString(tTemplateTypeSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v VALUES(%v,%v,%v)`,
		table.TableName, tIDSource, tKeeDest, tTemplateTypeDest)
}

func (table LoadedTemplates) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
