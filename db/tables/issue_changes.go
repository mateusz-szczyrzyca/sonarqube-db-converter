package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type IssueChanges struct {
	TableName string
}

func (table IssueChanges) GetDataFromDataSource() string {
	return fmt.Sprintf(`
	SELECT
	 id,
	 kee,
	 issue_key,
	 user_login,
	 change_type,
	 change_data,
	 created_at,
	 updated_at,
	 issue_change_creation_date
	FROM
     %v
	ORDER BY id ASC`, table.TableName)
}

func (table IssueChanges) GetTableName() string {
	return table.TableName
}

func (table IssueChanges) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tKeeSource sql.NullString
	var tIssueKeySource sql.NullString
	var tUserLoginSource sql.NullString
	var tChangeTypeSource sql.NullString
	var tChangeDataSource sql.NullString
	var tCreatedATSource sql.NullInt64
	var tUpdatedATSource sql.NullInt64
	var tIssueChangeCreationDateSource sql.NullInt64

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tKeeSource, &tIssueKeySource, &tUserLoginSource,
		&tChangeTypeSource, &tChangeDataSource, &tCreatedATSource, &tUpdatedATSource,
		&tIssueChangeCreationDateSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tKeeDest := data_conversion.StringOrNullToString(tKeeSource)
	tIssueKeyDest := data_conversion.StringOrNullToString(tIssueKeySource)
	tUserLoginDest := data_conversion.StringOrNullToString(tUserLoginSource)
	tChangeTypeDest := data_conversion.StringOrNullToString(tChangeTypeSource)
	tChangeDataDest := data_conversion.StringOrNullToString(tChangeDataSource)
	tCreatedATDest := data_conversion.IntegerOrNullToString(tCreatedATSource)
	tUpdatedATDest := data_conversion.IntegerOrNullToString(tUpdatedATSource)
	tIssueChangeCreationDateDest := data_conversion.IntegerOrNullToString(tIssueChangeCreationDateSource)

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
		%v)`,
		table.TableName,
		tIDSource,
		tKeeDest,
		tIssueKeyDest,
		tUserLoginDest,
		tChangeTypeDest,
		tChangeDataDest,
		tCreatedATDest,
		tUpdatedATDest,
		tIssueChangeCreationDateDest)
}

func (table IssueChanges) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id bigint(20) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
