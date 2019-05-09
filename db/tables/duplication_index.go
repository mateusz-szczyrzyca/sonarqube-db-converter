package tables

import (
	"database/sql"
	"fmt"
	data_conversion "sonarqube-db-converter/db/types"
)

type DuplicationsIndex struct {
	TableName string
}

func (table DuplicationsIndex) GetTableName() string {
	return table.TableName
}

func (table DuplicationsIndex) GetDataFromDataSource() string {
	return fmt.Sprintf(`SELECT * FROM %v ORDER BY id ASC`, table.TableName)
}

func (table DuplicationsIndex) PrepareMySQLQuery(sourceRows sql.Rows, queryString chan string, errorChan chan error) {
	var tProjectSnapshotID int64
	var tSnapshotID int64
	var tHashSource sql.NullString
	var tIndexINFile int64
	var tStartLine int64
	var tEndLine int64
	var tID int64

	// Data retrieving from source DB
	errorSourceRowsScan := sourceRows.Scan(&tProjectSnapshotID, &tSnapshotID, &tHashSource, &tIndexINFile,
		&tStartLine, &tEndLine, &tID)
	if errorSourceRowsScan != nil {
		errorChan <- errorSourceRowsScan
	}

	tHashDest := data_conversion.StringOrNullToString(tHashSource)

	// Inserting data to destination DB
	queryString <- fmt.Sprintf(`INSERT INTO %v
		VALUES(%v,
		%v,
		%v,
		%v,
		%v,
		%v,
		%v)`,
		table.TableName,
		tProjectSnapshotID,
		tSnapshotID,
		tHashDest,
		tIndexINFile,
		tStartLine,
		tEndLine,
		tID)
}

func (table DuplicationsIndex) GetSetPrimaryKEYQuery() string {
	return fmt.Sprintf(`ALTER TABLE %v CHANGE id id bigint(20) AUTO_INCREMENT PRIMARY KEY;`,
		table.TableName)
}
