package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type Metrics struct {
	TableName string
}

func (table Metrics) GetTableName() string {
	return table.TableName
}

func (table Metrics) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT
	 id,
     name,
	 description,
	 direction,
	 domain,
	 short_name,
	 qualitative,
	 val_type,
	 user_managed,
	 enabled,
	 worst_value,
	 best_value,
	 optimized_best_value,
	 hidden,
     delete_historical_data
	FROM
     %v
    ORDER BY id ASC`,
		table.TableName)
}

func (table Metrics) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tNameSource sql.NullString
	var tDescriptionSource sql.NullString
	var tDirectionSource sql.NullInt64
	var tDomainSource sql.NullString
	var tShortNameSource sql.NullString
	var tQualitativeSource bool
	var tValTypeSource sql.NullString
	var tUserManagedSource bool
	var tEnabledSource bool
	var tWorstValueSource sql.NullFloat64
	var tBestValueSource sql.NullFloat64
	var tOptimizedBestValueSource bool
	var tHiddenSource bool
	var tDeleteHistoricalDataSource bool

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tNameSource, &tDescriptionSource, &tDirectionSource,
		&tDomainSource, &tShortNameSource, &tQualitativeSource, &tValTypeSource, &tUserManagedSource,
		&tEnabledSource, &tWorstValueSource, &tBestValueSource, &tOptimizedBestValueSource,
		&tHiddenSource, &tDeleteHistoricalDataSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tNameDest := data_conversion.StringOrNullToString(tNameSource)
	tDescriptionDest := data_conversion.StringOrNullToString(tDescriptionSource)
	tDirectionDest := data_conversion.IntegerOrNullToString(tDirectionSource)
	tDomainDest := data_conversion.StringOrNullToString(tDomainSource)
	tShortNameDest := data_conversion.StringOrNullToString(tShortNameSource)
	tQualitativeDest := data_conversion.PgBoolToMySQLTinyint(tQualitativeSource)
	tValTypeDest := data_conversion.StringOrNullToString(tValTypeSource)
	tUserManagedDest := data_conversion.PgBoolToMySQLTinyint(tUserManagedSource)
	tEnabledDest := data_conversion.PgBoolToMySQLTinyint(tEnabledSource)
	tWorstValueDest := data_conversion.FloatOrNullToString(tWorstValueSource)
	tBestValueDest := data_conversion.FloatOrNullToString(tBestValueSource)
	tOptimizedBestValueDest := data_conversion.PgBoolToMySQLTinyint(tOptimizedBestValueSource)
	tHiddenDest := data_conversion.PgBoolToMySQLTinyint(tHiddenSource)
	tDeleteHistoricalDataDest := data_conversion.PgBoolToMySQLTinyint(tDeleteHistoricalDataSource)

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
		%v)`,
		table.TableName,
		tIDSource,
		tNameDest,
		tDescriptionDest,
		tDirectionDest,
		tDomainDest,
		tShortNameDest,
		tQualitativeDest,
		tValTypeDest,
		tUserManagedDest,
		tEnabledDest,
		tWorstValueDest,
		tBestValueDest,
		tOptimizedBestValueDest,
		tHiddenDest,
		tDeleteHistoricalDataDest)
}

func (table Metrics) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
