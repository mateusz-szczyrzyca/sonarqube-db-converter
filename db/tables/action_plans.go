package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type ActionPlans struct {
	TableName string
}

func (table ActionPlans) GetTableName() string {
	return table.TableName
}

func (table ActionPlans) GetDataFromDataSource() string {
	return fmt.Sprintf(`
	SELECT
	 id,
	 %v,
	 %v,
	 name,
	 description,
	 %v,
	 user_login,
	 project_id,
	 status,
	 kee
	FROM
	 %v
	ORDER BY id ASC;`,
		data_conversion.ReflectTimeFormat("created_at"),
		data_conversion.ReflectTimeFormat("updated_at"),
		data_conversion.ReflectTimeFormat("deadline"),
		table.TableName)
}

func (table ActionPlans) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tCreatedATSource sql.NullString
	var tUpdatedATSource sql.NullString
	var tNameSource sql.NullString
	var tDescriptionSource sql.NullString
	var tDeadlineSource sql.NullString
	var tUserLoginSource sql.NullString
	var tProjectIDSource sql.NullInt64
	var tStatusSource sql.NullString
	var tKeeSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tCreatedATSource, &tUpdatedATSource,
		&tNameSource, &tDescriptionSource, &tDeadlineSource, &tUserLoginSource, &tProjectIDSource,
		&tStatusSource, &tKeeSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tCreatedATDest := data_conversion.StringOrNullToString(tCreatedATSource)
	tUpdatedATDest := data_conversion.StringOrNullToString(tUpdatedATSource)
	tNameDest := data_conversion.StringOrNullToString(tNameSource)
	tDescriptionDest := data_conversion.StringOrNullToString(tDescriptionSource)
	tDeadlineDest := data_conversion.StringOrNullToString(tDeadlineSource)
	tUserLoginDest := data_conversion.StringOrNullToString(tUserLoginSource)
	tProjectIDDest := data_conversion.IntegerOrNullToString(tProjectIDSource)
	tStatusDest := data_conversion.StringOrNullToString(tStatusSource)
	tKeeDest := data_conversion.StringOrNullToString(tKeeSource)

	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(null,
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
		tCreatedATDest,
		tUpdatedATDest,
		tNameDest,
		tDescriptionDest,
		tDeadlineDest,
		tUserLoginDest,
		tProjectIDDest,
		tStatusDest,
		tKeeDest)
}

func (table ActionPlans) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id bigint(20) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
