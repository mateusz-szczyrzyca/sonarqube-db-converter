package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type Projects struct {
	TableName string
}

func (table Projects) GetTableName() string {
	return table.TableName
}

func (table Projects) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
     name,
	 description,
	 enabled,
	 scope,
	 qualifier,
	 kee,
     root_id,
	 language,
	 copy_resource_id,
	 long_name,
	 person_id,
	 %v,
	 path,
	 deprecated_kee,
	 uuid,
	 project_uuid,
	 module_uuid,
	 module_uuid_path,
	 authorization_updated_at
	FROM
     %v
    ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		table.TableName)
}

func (table Projects) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tNameSource sql.NullString
	var tDescriptionSource sql.NullString
	var tEnabledSource bool
	var tScopeSource sql.NullString
	var tQualifierSource sql.NullString
	var tKeeSource sql.NullString
	var tRootIDSource sql.NullInt64
	var tLanguageSource sql.NullString
	var tCopyResourceIDSource sql.NullInt64
	var tLongNameSource sql.NullString
	var tPersonIDSource sql.NullInt64
	var tCreatedATSource sql.NullString
	var tPathSource sql.NullString
	var tDeprecatedKeeSource sql.NullString
	var tUUIDSource sql.NullString
	var tProjectUUIDSource sql.NullString
	var tModuleUUIDSource sql.NullString
	var tModuleUUIDPathSource sql.NullString
	var tAuthorizationUpdatedATSource sql.NullInt64

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tNameSource, &tDescriptionSource, &tEnabledSource,
		&tScopeSource, &tQualifierSource, &tKeeSource, &tRootIDSource, &tLanguageSource, &tCopyResourceIDSource,
		&tLongNameSource, &tPersonIDSource, &tCreatedATSource, &tPathSource, &tDeprecatedKeeSource,
		&tUUIDSource, &tProjectUUIDSource, &tModuleUUIDSource, &tModuleUUIDPathSource,
		&tAuthorizationUpdatedATSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tNameDest := data_conversion.StringOrNullToString(tNameSource)
	tDescriptionDest := data_conversion.StringOrNullToString(tDescriptionSource)
	tEnabledDest := data_conversion.PgBoolToMySQLTinyint(tEnabledSource)
	tScopeDest := data_conversion.StringOrNullToString(tScopeSource)
	tQualifierDest := data_conversion.StringOrNullToString(tQualifierSource)
	tKeeDest := data_conversion.StringOrNullToString(tKeeSource)
	tRootIDDest := data_conversion.IntegerOrNullToString(tRootIDSource)
	tLanguageDest := data_conversion.StringOrNullToString(tLanguageSource)
	tCopyResourceIDDest := data_conversion.IntegerOrNullToString(tCopyResourceIDSource)
	tLongNameDest := data_conversion.StringOrNullToString(tLongNameSource)
	tPersonIDDest := data_conversion.IntegerOrNullToString(tPersonIDSource)
	tCreatedATDest := data_conversion.StringOrNullToString(tCreatedATSource)
	tPathDest := data_conversion.StringOrNullToString(tPathSource)
	tDeprecatedKeeDest := data_conversion.StringOrNullToString(tDeprecatedKeeSource)
	tUUIDDest := data_conversion.StringOrNullToString(tUUIDSource)
	tProjectUUIDDest := data_conversion.StringOrNullToString(tProjectUUIDSource)
	tModuleUUIDDest := data_conversion.StringOrNullToString(tModuleUUIDSource)
	tModuleUUIDPathDest := data_conversion.StringOrNullToString(tModuleUUIDPathSource)
	tAuthorizationUpdatedATDest := data_conversion.IntegerOrNullToString(tAuthorizationUpdatedATSource)

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
		tNameDest,
		tDescriptionDest,
		tEnabledDest,
		tScopeDest,
		tQualifierDest,
		tKeeDest,
		tRootIDDest,
		tLanguageDest,
		tCopyResourceIDDest,
		tLongNameDest,
		tPersonIDDest,
		tCreatedATDest,
		tPathDest,
		tDeprecatedKeeDest,
		tUUIDDest,
		tProjectUUIDDest,
		tModuleUUIDDest,
		tModuleUUIDPathDest,
		tAuthorizationUpdatedATDest)
}

func (table Projects) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
