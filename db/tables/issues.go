package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type Issues struct {
	TableName string
}

func (table Issues) GetTableName() string {
	return table.TableName
}

func (table Issues) GetDataFromDataSource() string {
	return fmt.Sprintf(`
	SELECT
	 id,
	 kee,
	 rule_id,
	 severity,
	 manual_severity,
	 message,
	 line,
	 effort_to_fix,
	 status,
	 resolution,
	 checksum,
	 reporter,
	 assignee,
	 author_login,
	 action_plan_key,
	 issue_attributes,
	 technical_debt,
	 created_at,
	 updated_at,
	 issue_creation_date,
	 issue_update_date,
	 issue_close_date,
	 tags,
	 component_uuid,
	 project_uuid,
	 encode(locations, 'hex')
	FROM
     %v
	ORDER BY id ASC`, table.TableName)
}

func (table Issues) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tKeeSource sql.NullString
	var tRuleIDSource sql.NullInt64
	var tSeveritySource sql.NullString
	var tManualSeveritySource bool
	var tMessageSource sql.NullString
	var tLineSource sql.NullInt64
	var tEffortTOFixSource sql.NullFloat64
	var tStatusSource sql.NullString
	var tResolutionSource sql.NullString
	var tChecksumSource sql.NullString
	var tReporterSource sql.NullString
	var tAssigneeSource sql.NullString
	var tAuthorLoginSource sql.NullString
	var tActionPlanKeySource sql.NullString
	var tIssueAttributesSource sql.NullString
	var tTechnicalDebtSource sql.NullInt64
	var tCreatedATSource sql.NullInt64
	var tUpdatedATSource sql.NullInt64
	var tIssueCreationDateSource sql.NullInt64
	var tIssueUpdateDateSource sql.NullInt64
	var tIssueCloseDateSource sql.NullInt64
	var tTagsSource sql.NullString
	var tComponentUUIDSource sql.NullString
	var tProjectUUIDSource sql.NullString
	var tLocationsSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tKeeSource, &tRuleIDSource, &tSeveritySource,
		&tManualSeveritySource, &tMessageSource, &tLineSource, &tEffortTOFixSource, &tStatusSource,
		&tResolutionSource, &tChecksumSource, &tReporterSource, &tAssigneeSource, &tAuthorLoginSource,
		&tActionPlanKeySource, &tIssueAttributesSource, &tTechnicalDebtSource, &tCreatedATSource,
		&tUpdatedATSource, &tIssueCreationDateSource, &tIssueUpdateDateSource, &tIssueCloseDateSource,
		&tTagsSource, &tComponentUUIDSource, &tProjectUUIDSource, &tLocationsSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tKeeDest := data_conversion.StringOrNullToString(tKeeSource)
	tRuleIDDest := data_conversion.IntegerOrNullToString(tRuleIDSource)
	tSeverityDest := data_conversion.StringOrNullToString(tSeveritySource)
	tManualSeverityDest := data_conversion.PgBoolToMySQLTinyint(tManualSeveritySource)
	tMessageDest := data_conversion.StringOrNullToString(tMessageSource)
	tLineDest := data_conversion.IntegerOrNullToString(tLineSource)
	tEffortTOFixDest := data_conversion.FloatOrNullToString(tEffortTOFixSource)
	tStatusDest := data_conversion.StringOrNullToString(tStatusSource)
	tResolutionDest := data_conversion.StringOrNullToString(tResolutionSource)
	tChecksumDest := data_conversion.StringOrNullToString(tChecksumSource)
	tReporterDest := data_conversion.StringOrNullToString(tReporterSource)
	tAssigneeDest := data_conversion.StringOrNullToString(tAssigneeSource)
	tAuthorLoginDest := data_conversion.StringOrNullToString(tAuthorLoginSource)
	tActionPlanKeyDest := data_conversion.StringOrNullToString(tActionPlanKeySource)
	tIssueAttributesDest := data_conversion.StringOrNullToString(tIssueAttributesSource)
	tTechnicalDebtDest := data_conversion.IntegerOrNullToString(tTechnicalDebtSource)
	tCreatedATDest := data_conversion.IntegerOrNullToString(tCreatedATSource)
	tUpdatedATDest := data_conversion.IntegerOrNullToString(tUpdatedATSource)
	tIssueCreationDateDest := data_conversion.IntegerOrNullToString(tIssueCreationDateSource)
	tIssueUpdateDateDest := data_conversion.IntegerOrNullToString(tIssueUpdateDateSource)
	tIssueCloseDateDest := data_conversion.IntegerOrNullToString(tIssueCloseDateSource)
	tTagsDest := data_conversion.StringOrNullToString(tTagsSource)
	tComponentUUIDDest := data_conversion.StringOrNullToString(tComponentUUIDSource)
	tProjectUUIDDest := data_conversion.StringOrNullToString(tProjectUUIDSource)
	tLocationsDest := data_conversion.StringOrNullToString(tLocationsSource)

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
		%v,
		%v,
		%v,
		%v,
		%v,
		%v,
		unhex(%v))`,
		table.TableName,
		tIDSource,
		tKeeDest,
		tRuleIDDest,
		tSeverityDest,
		tManualSeverityDest,
		tMessageDest,
		tLineDest,
		tEffortTOFixDest,
		tStatusDest,
		tResolutionDest,
		tChecksumDest,
		tReporterDest,
		tAssigneeDest,
		tAuthorLoginDest,
		tActionPlanKeyDest,
		tIssueAttributesDest,
		tTechnicalDebtDest,
		tCreatedATDest,
		tUpdatedATDest,
		tIssueCreationDateDest,
		tIssueUpdateDateDest,
		tIssueCloseDateDest,
		tTagsDest,
		tComponentUUIDDest,
		tProjectUUIDDest,
		tLocationsDest)
}

func (table Issues) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id bigint(20) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
