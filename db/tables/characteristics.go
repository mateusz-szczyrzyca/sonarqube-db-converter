package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type Characteristics struct {
	TableName string
}

func (table Characteristics) GetTableName() string {
	return table.TableName
}

func (table Characteristics) GetDataFromDataSource() string {
	return fmt.Sprintf(`
	SELECT
	 id,
	 kee,
	 name,
	 rule_id,
	 characteristic_order,
	 enabled,
	 parent_id,
	 root_id,
	 function_key,
	 factor_value,
	 factor_unit,
	 offset_value,
	 offset_unit,
	 %v,
	 %v
	FROM
	 %v
	ORDER BY id ASC`,
		data_conversion.ReflectTimeFormat("created_at"),
		data_conversion.ReflectTimeFormat("updated_at"),
		table.TableName)
}

func (table Characteristics) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tIDSource int64
	var tKeeSource sql.NullString
	var tNameSource sql.NullString
	var tRuleIDSource sql.NullInt64
	var tCharacteristicOrderSource sql.NullInt64
	var tEnabledSource bool
	var tParentIDSource sql.NullInt64
	var tRootIDSource sql.NullInt64
	var tFunctionKEYSource sql.NullString
	var tFactorValueSource sql.NullInt64
	var tFactorUnitSource sql.NullString
	var tOffsetValueSource sql.NullInt64
	var tOffsetUnitSource sql.NullString
	var tCreatedATSource sql.NullString
	var tUpdatedATSource sql.NullString

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tIDSource, &tKeeSource, &tNameSource, &tRuleIDSource,
		&tCharacteristicOrderSource, &tEnabledSource, &tParentIDSource, &tRootIDSource, &tFunctionKEYSource,
		&tFactorValueSource, &tFactorUnitSource, &tOffsetValueSource, &tOffsetUnitSource, &tCreatedATSource,
		&tUpdatedATSource)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	// Required data conversion
	tKeeDest := data_conversion.StringOrNullToString(tKeeSource)
	tNameDest := data_conversion.StringOrNullToString(tNameSource)
	tRuleIDDest := data_conversion.IntegerOrNullToString(tRootIDSource)
	tCharacteristicOrderDest := data_conversion.IntegerOrNullToString(tCharacteristicOrderSource)
	tEnabledDest := data_conversion.PgBoolToMySQLTinyint(tEnabledSource)
	tParentIDDest := data_conversion.IntegerOrNullToString(tParentIDSource)
	tRootIDDest := data_conversion.IntegerOrNullToString(tRootIDSource)
	tFunctionKEYDest := data_conversion.StringOrNullToString(tFunctionKEYSource)
	tFactorValueDest := data_conversion.IntegerOrNullToString(tFactorValueSource)
	tFactorUnitDest := data_conversion.StringOrNullToString(tFactorUnitSource)
	tOffsetValueDest := data_conversion.IntegerOrNullToString(tOffsetValueSource)
	tOffsetUnitDest := data_conversion.StringOrNullToString(tOffsetUnitSource)
	tCreatedATDest := data_conversion.StringOrNullToString(tCreatedATSource)
	tUpdatedATDest := data_conversion.StringOrNullToString(tUpdatedATSource)

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
		tKeeDest,
		tNameDest,
		tRuleIDDest,
		tCharacteristicOrderDest,
		tEnabledDest,
		tParentIDDest,
		tRootIDDest,
		tFunctionKEYDest,
		tFactorValueDest,
		tFactorUnitDest,
		tOffsetValueDest,
		tOffsetUnitDest,
		tCreatedATDest,
		tUpdatedATDest)
}

func (table Characteristics) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id int(11) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
