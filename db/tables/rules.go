package tables

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type Rules struct {
	TableName string
}

func (table Rules) GetTableName() string {
	return table.TableName
}

func (table Rules) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
	 plugin_rule_key,
	 plugin_config_key,
	 plugin_name,
	 description,
     priority,
	 template_id,
	 name,
	 status,
	 language,
	 %v,
	 %v,
	 %v,
	 %v,
	 note_user_login,
	 note_data,
	 characteristic_id,
	 default_characteristic_id,
	 remediation_function,
	 default_remediation_function,
	 remediation_coeff,
	 default_remediation_coeff,
	 remediation_offset,
	 default_remediation_offset,
	 effort_to_fix_description,
	 tags,
	 system_tags,
	 is_template,
	 description_format
	FROM
     %v
    ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		data_conversion.ReflectTimeFormat("updated_at"),
		data_conversion.ReflectTimeFormat("note_created_at"),
		data_conversion.ReflectTimeFormat("note_updated_at"),
		table.TableName)
}

func (table Rules) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tNameSource sql.NullString
	var tPluginRuleKEYSource sql.NullString
	var tPluginConfigKEYSource sql.NullString
	var tPluginNameSource sql.NullString
	var tDescriptionSource sql.NullString
	var tPrioritySource sql.NullInt64
	var tTemplateIDSource sql.NullInt64
	var tStatusSource sql.NullString
	var tLanguageSource sql.NullString
	var tCreatedATSource sql.NullString
	var tUpdatedATSource sql.NullString
	var tNoteCreatedATSource sql.NullString
	var tNoteUpdatedATSource sql.NullString
	var tNoteUserLoginSource sql.NullString
	var tNoteDataSource sql.NullString
	var tCharacteristicIDSource sql.NullInt64
	var tDefaultCharacteristicIDSource sql.NullInt64
	var tRemediationFunctionSource sql.NullString
	var tDefaultRemediationFunctionSource sql.NullString
	var tRemediationCoeffSource sql.NullString
	var tDefaultRemediationCoeffSource sql.NullString
	var tRemediationOffsetSource sql.NullString
	var tDefaultRemediationOffsetSource sql.NullString
	var tEffortTOFixDescriptionSource sql.NullString
	var tTagsSource sql.NullString
	var tSystemTagsSource sql.NullString
	var tISTemplateSource bool
	var tDescriptionFormatSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tPluginRuleKEYSource,
		&tPluginConfigKEYSource, &tPluginNameSource, &tDescriptionSource, &tPrioritySource, &tTemplateIDSource,
		&tNameSource, &tStatusSource, &tLanguageSource, &tCreatedATSource, &tUpdatedATSource,
		&tNoteCreatedATSource, &tNoteUpdatedATSource, &tNoteUserLoginSource, &tNoteDataSource,
		&tCharacteristicIDSource, &tDefaultCharacteristicIDSource, &tRemediationFunctionSource,
		&tDefaultRemediationFunctionSource, &tRemediationCoeffSource, &tDefaultRemediationCoeffSource,
		&tRemediationOffsetSource, &tDefaultRemediationOffsetSource, &tEffortTOFixDescriptionSource,
		&tTagsSource, &tSystemTagsSource, &tISTemplateSource, &tDescriptionFormatSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tNameDest := data_conversion.StringOrNullToString(tNameSource)
	tPluginRuleKEYDest := data_conversion.StringOrNullToString(tPluginRuleKEYSource)
	tPluginConfigKEYDest := data_conversion.StringOrNullToString(tPluginConfigKEYSource)
	tPluginNameDest := data_conversion.StringOrNullToString(tPluginNameSource)
	tDescriptionDest := data_conversion.StringOrNullToString(tDescriptionSource)
	tPriorityDest := data_conversion.IntegerOrNullToString(tPrioritySource)
	tTemplateIDDest := data_conversion.IntegerOrNullToString(tTemplateIDSource)
	tStatusDest := data_conversion.StringOrNullToString(tStatusSource)
	tLanguageDest := data_conversion.StringOrNullToString(tLanguageSource)
	tCreatedATDest := data_conversion.StringOrNullToString(tCreatedATSource)
	tUpdatedATDest := data_conversion.StringOrNullToString(tUpdatedATSource)
	tNoteCreatedATDest := data_conversion.StringOrNullToString(tNoteDataSource)
	tNoteUpdatedATDest := data_conversion.StringOrNullToString(tNoteUpdatedATSource)
	tNoteUserLoginDest := data_conversion.StringOrNullToString(tNoteUserLoginSource)
	tNoteDataDest := data_conversion.StringOrNullToString(tNoteDataSource)
	tCharacteristicIDDest := data_conversion.IntegerOrNullToString(tCharacteristicIDSource)
	tDefaultCharacteristicIDDest := data_conversion.IntegerOrNullToString(tDefaultCharacteristicIDSource)
	tRemediationFunctionDest := data_conversion.StringOrNullToString(tRemediationFunctionSource)
	tDefaultRemediationFunctionDest := data_conversion.StringOrNullToString(tDefaultRemediationFunctionSource)
	tRemediationCoeffDest := data_conversion.StringOrNullToString(tRemediationCoeffSource)
	tDefaultRemediationCoeffDest := data_conversion.StringOrNullToString(tDefaultRemediationCoeffSource)
	tRemediationOffsetDest := data_conversion.StringOrNullToString(tRemediationOffsetSource)
	tDefaultRemediationOffsetDest := data_conversion.StringOrNullToString(tDefaultRemediationOffsetSource)
	tEffortTOFixDescriptionDest := data_conversion.StringOrNullToString(tEffortTOFixDescriptionSource)
	tTagsDest := data_conversion.StringOrNullToString(tTagsSource)
	tSystemTagsDest := data_conversion.StringOrNullToString(tSystemTagsSource)
	tISTemplateDest := data_conversion.PgBoolToMySQLTinyint(tISTemplateSource)
	tDescriptionFormatDest := data_conversion.StringOrNullToString(tDescriptionFormatSource)

	// Problematic text - PgSQL does not have hex encoding for text, hence
	// we have to use it directly from stdlib
	tDescriptionDest = hex.EncodeToString([]byte(tDescriptionDest))

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(%v,
		%v,
		%v,
		%v,
		%v,
		unhex('%v'),
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
		%v,
		%v,
		%v,
		%v,
		%v)`,
		table.TableName,
		tIDSource,
		tNameDest,
		tPluginRuleKEYDest,
		tPluginConfigKEYDest,
		tPluginNameDest,
		tDescriptionDest,
		tPriorityDest,
		tTemplateIDDest,
		tStatusDest,
		tLanguageDest,
		tCreatedATDest,
		tUpdatedATDest,
		tNoteCreatedATDest,
		tNoteUpdatedATDest,
		tNoteUserLoginDest,
		tNoteDataDest,
		tCharacteristicIDDest,
		tDefaultCharacteristicIDDest,
		tRemediationFunctionDest,
		tDefaultRemediationFunctionDest,
		tRemediationCoeffDest,
		tDefaultRemediationCoeffDest,
		tRemediationOffsetDest,
		tDefaultRemediationOffsetDest,
		tEffortTOFixDescriptionDest,
		tTagsDest,
		tSystemTagsDest,
		tISTemplateDest,
		tDescriptionFormatDest)
}

func (table Rules) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
