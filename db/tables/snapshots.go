package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type Snapshots struct {
	TableName string
}

func (table Snapshots) GetTableName() string {
	return table.TableName
}

func (table Snapshots) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
	 project_id,
	 parent_snapshot_id,
	 status,
	 islast,
     scope,
	 qualifier,
	 root_snapshot_id,
	 version,
	 path,
	 depth,
	 root_project_id,
	 purge_status,
	 period1_mode,
	 period1_param,
	 period2_mode,
	 period2_param,
	 period3_mode,
	 period3_param,
	 period4_mode,
	 period4_param,
	 period5_mode,
	 period5_param,
	 created_at,
	 build_date,
	 period1_date,
	 period2_date,
	 period3_date,
	 period4_date,
	 period5_date
	FROM
     %v
    ORDER BY id ASC`, table.TableName)
}

func (table Snapshots) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tProjectIDSource sql.NullInt64
	var tParentSnapshotIDSource sql.NullInt64
	var tStatusSource sql.NullString
	var tISLastSource bool
	var tScopeSource sql.NullString
	var tQualifierSource sql.NullString
	var tRootSnapshotIDSource sql.NullInt64
	var tVersionSource sql.NullString
	var tPathSource sql.NullString
	var tDepthSource sql.NullInt64
	var tRootProjectIDSource sql.NullInt64
	var tPurgeStatusSource sql.NullInt64
	var tPeriod1ModeSource sql.NullString
	var tPeriod1ParamSource sql.NullString
	var tPeriod2ModeSource sql.NullString
	var tPeriod2ParamSource sql.NullString
	var tPeriod3ModeSource sql.NullString
	var tPeriod3ParamSource sql.NullString
	var tPeriod4ModeSource sql.NullString
	var tPeriod4ParamSource sql.NullString
	var tPeriod5ModeSource sql.NullString
	var tPeriod5ParamSource sql.NullString
	var tCreatedATSource sql.NullInt64
	var tBuildDateSource sql.NullInt64
	var tPeriod1DateSource sql.NullInt64
	var tPeriod2DateSource sql.NullInt64
	var tPeriod3DateSource sql.NullInt64
	var tPeriod4DateSource sql.NullInt64
	var tPeriod5DateSource sql.NullInt64

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tProjectIDSource, &tParentSnapshotIDSource,
		&tStatusSource, &tISLastSource, &tScopeSource, &tQualifierSource, &tRootSnapshotIDSource,
		&tVersionSource, &tPathSource, &tDepthSource, &tRootProjectIDSource,
		&tPurgeStatusSource, &tPeriod1ModeSource, &tPeriod1ParamSource, &tPeriod2ModeSource,
		&tPeriod2ParamSource, &tPeriod3ModeSource, &tPeriod3ParamSource,
		&tPeriod4ModeSource, &tPeriod4ParamSource, &tPeriod5ModeSource,
		&tPeriod5ParamSource, &tCreatedATSource, &tBuildDateSource, &tPeriod1DateSource,
		&tPeriod2DateSource, &tPeriod3DateSource, &tPeriod4DateSource, &tPeriod5DateSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tProjectIDDest := data_conversion.IntegerOrNullToString(tProjectIDSource)
	tParentSnapshotIDDest := data_conversion.IntegerOrNullToString(tParentSnapshotIDSource)
	tStatusDest := data_conversion.StringOrNullToString(tStatusSource)
	tISLastDest := data_conversion.PgBoolToMySQLTinyint(tISLastSource)
	tScopeDest := data_conversion.StringOrNullToString(tScopeSource)
	tQualifierDest := data_conversion.StringOrNullToString(tQualifierSource)
	tRootSnapshotIDDest := data_conversion.IntegerOrNullToString(tRootSnapshotIDSource)
	tVersionDest := data_conversion.StringOrNullToString(tVersionSource)
	tPathDest := data_conversion.StringOrNullToString(tPathSource)
	tDepthDest := data_conversion.IntegerOrNullToString(tDepthSource)
	tRootProjectIDDest := data_conversion.IntegerOrNullToString(tRootProjectIDSource)
	tPurgeStatusDest := data_conversion.IntegerOrNullToString(tPurgeStatusSource)
	tPeriod1ModeDest := data_conversion.StringOrNullToString(tPeriod1ModeSource)
	tPeriod1ParamDest := data_conversion.StringOrNullToString(tPeriod1ParamSource)
	tPeriod2ModeDest := data_conversion.StringOrNullToString(tPeriod2ModeSource)
	tPeriod2ParamDest := data_conversion.StringOrNullToString(tPeriod2ParamSource)
	tPeriod3ModeDest := data_conversion.StringOrNullToString(tPeriod3ModeSource)
	tPeriod3ParamDest := data_conversion.StringOrNullToString(tPeriod3ParamSource)
	tPeriod4ModeDest := data_conversion.StringOrNullToString(tPeriod4ModeSource)
	tPeriod4ParamDest := data_conversion.StringOrNullToString(tPeriod4ParamSource)
	tPeriod5ModeDest := data_conversion.StringOrNullToString(tPeriod5ModeSource)
	tPeriod5ParamDest := data_conversion.StringOrNullToString(tPeriod5ParamSource)
	tCreatedATDest := data_conversion.IntegerOrNullToString(tCreatedATSource)
	tBuildDateDest := data_conversion.IntegerOrNullToString(tBuildDateSource)
	tPeriod1DateDest := data_conversion.IntegerOrNullToString(tPeriod1DateSource)
	tPeriod2DateDest := data_conversion.IntegerOrNullToString(tPeriod2DateSource)
	tPeriod3DateDest := data_conversion.IntegerOrNullToString(tPeriod3DateSource)
	tPeriod4DateDest := data_conversion.IntegerOrNullToString(tPeriod4DateSource)
	tPeriod5DateDest := data_conversion.IntegerOrNullToString(tPeriod5DateSource)

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
		%v,
		%v,
		%v,
		%v,
		%v)`,
		table.TableName,
		tIDSource,
		tProjectIDDest,
		tParentSnapshotIDDest,
		tStatusDest,
		tISLastDest,
		tScopeDest,
		tQualifierDest,
		tRootSnapshotIDDest,
		tVersionDest,
		tPathDest,
		tDepthDest,
		tRootProjectIDDest,
		tPurgeStatusDest,
		tPeriod1ModeDest,
		tPeriod1ParamDest,
		tPeriod2ModeDest,
		tPeriod2ParamDest,
		tPeriod3ModeDest,
		tPeriod3ParamDest,
		tPeriod4ModeDest,
		tPeriod4ParamDest,
		tPeriod5ModeDest,
		tPeriod5ParamDest,
		tCreatedATDest,
		tBuildDateDest,
		tPeriod1DateDest,
		tPeriod2DateDest,
		tPeriod3DateDest,
		tPeriod4DateDest,
		tPeriod5DateDest)
}

func (table Snapshots) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
