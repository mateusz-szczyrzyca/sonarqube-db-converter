package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type Users struct {
	TableName string
}

func (table Users) GetTableName() string {
	return table.TableName
}

func (table Users) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
	 login,
	 name,
	 email,
	 crypted_password,
	 salt,
	 remember_token,
	 %v,
	 active,
	 created_at,
	 updated_at,
	 scm_accounts
    FROM
     %v
    ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("remember_token_expires_at"),
		table.TableName)
}

func (table Users) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tLoginSource sql.NullString
	var tNameSource sql.NullString
	var tEmailSource sql.NullString
	var tCryptedPasswordSource sql.NullString
	var tSaltSource sql.NullString
	var tRememberTokenSource sql.NullString
	var tRememberTokenExpiresATSource sql.NullString
	var tActiveSource bool
	var tCreatedATSource sql.NullInt64
	var tUpdatedATSource sql.NullInt64
	var tSCMAccountsSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tLoginSource, &tNameSource, &tEmailSource,
		&tCryptedPasswordSource, &tSaltSource, &tRememberTokenSource, &tRememberTokenExpiresATSource,
		&tActiveSource, &tCreatedATSource, &tUpdatedATSource, &tSCMAccountsSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tLoginDest := data_conversion.StringOrNullToString(tLoginSource)
	tNameDest := data_conversion.StringOrNullToString(tNameSource)
	tEmailDest := data_conversion.StringOrNullToString(tEmailSource)
	tCryptedPasswordDest := data_conversion.StringOrNullToString(tCryptedPasswordSource)
	tSaltDest := data_conversion.StringOrNullToString(tSaltSource)
	tRememberTokenDest := data_conversion.StringOrNullToString(tRememberTokenSource)
	tRememberTokenExpiresATDest := data_conversion.StringOrNullToString(tRememberTokenExpiresATSource)
	tActiveDest := data_conversion.PgBoolToMySQLTinyint(tActiveSource)
	tCreatedATDest := data_conversion.IntegerOrNullToString(tCreatedATSource)
	tUpdatedATDest := data_conversion.IntegerOrNullToString(tUpdatedATSource)
	tSCMAccountsDest := data_conversion.StringOrNullToString(tSCMAccountsSource)

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
		 %v,
		 %v)`,
		table.TableName,
		tIDSource,
		tLoginDest,
		tNameDest,
		tEmailDest,
		tCryptedPasswordDest,
		tSaltDest,
		tRememberTokenDest,
		tRememberTokenExpiresATDest,
		tActiveDest,
		tCreatedATDest,
		tUpdatedATDest,
		tSCMAccountsDest)
}

func (table Users) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
