package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type UserRoles struct {
	TableName string
}

func (table UserRoles) GetTableName() string {
	return table.TableName
}

func (table UserRoles) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
	 user_id,
	 resource_id,
	 role
    FROM
     %v
    ORDER BY id ASC`, table.TableName)
}

func (table UserRoles) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tUserIDSource sql.NullInt64
	var tResourceIDSource sql.NullInt64
	var tRoleSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tUserIDSource, &tResourceIDSource,
		&tRoleSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tUserIDDest := data_conversion.IntegerOrNullToString(tUserIDSource)
	tResourceIDDest := data_conversion.IntegerOrNullToString(tResourceIDSource)
	tRoleDest := data_conversion.StringOrNullToString(tRoleSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(%v,
		 %v,
		 %v,
		 %v)`,
		table.TableName,
		tIDSource,
		tUserIDDest,
		tResourceIDDest,
		tRoleDest)
}

func (table UserRoles) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
