package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type PermTemplatesUsers struct {
	TableName string
}

func (table PermTemplatesUsers) GetTableName() string {
	return table.TableName
}

func (table PermTemplatesUsers) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
     user_id,
	 template_id,
	 permission_reference,
	 %v,
	 %v
	FROM
     %v
    ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		data_conversion.ReflectTimeFormat("updated_at"),
		table.TableName)
}

func (table PermTemplatesUsers) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tUserIDSource sql.NullInt64
	var tTemplateIDSource sql.NullInt64
	var tPermissionReferenceSource sql.NullString
	var tCreatedATSource sql.NullString
	var tUpdatedATSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tUserIDSource, &tTemplateIDSource,
		&tPermissionReferenceSource, &tCreatedATSource, &tUpdatedATSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tUserIDDest := data_conversion.IntegerOrNullToString(tUserIDSource)
	tTemplateIDDest := data_conversion.IntegerOrNullToString(tTemplateIDSource)
	tPermissionReferenceDest := data_conversion.StringOrNullToString(tPermissionReferenceSource)
	tCreatedATDest := data_conversion.StringOrNullToString(tCreatedATSource)
	tUpdatedATDest := data_conversion.StringOrNullToString(tUpdatedATSource)

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
		tUserIDDest,
		tTemplateIDDest,
		tPermissionReferenceDest,
		tCreatedATDest,
		tUpdatedATDest)
}

func (table PermTemplatesUsers) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
