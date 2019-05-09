package tables

import (
	"database/sql"
	"fmt"
	"time"
	data_conversion "sonarqube-db-converter/db/types"
)

// CopyTableDuplicationsIndex source table: characteristics
func CopyTableDuplicationsIndex(pgsql, mysql *sql.DB) error {
	const pgsqlSourceTableName = "duplications_index"
	timeStart := time.Now()

	pgsqlQueryString := fmt.Sprintf(`SELECT * FROM %v ORDER BY id ASC`, pgsqlSourceTableName)
	sourceRows, errorSourceRetrievalQuery := pgsql.Query(pgsqlQueryString)

	if errorSourceRetrievalQuery != nil {
		return errorSourceRetrievalQuery
	}

	totalNumOfSourceRows := 0

	for sourceRows.Next() {
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
			return errorSourceRowsScan
		}

		tHashDest := data_conversion.StringOrNullToString(tHashSource)

		// Inserting data to destination DB
		mysqlQueryString := fmt.Sprintf(`INSERT INTO %v
		VALUES(%v,
		%v,
		%v,
		%v,
		%v,
		%v,
		%v)`,
			pgsqlSourceTableName,
			tProjectSnapshotID,
			tSnapshotID,
			tHashDest,
			tIndexINFile,
			tStartLine,
			tEndLine,
			tID)

		_, errorInsertDataToDestination := mysql.Exec(mysqlQueryString)
		if errorInsertDataToDestination != nil {
			return errorInsertDataToDestination
		}
		totalNumOfSourceRows++
	}
	timeStop := time.Since(timeStart)
	fmt.Printf("%v: successfully converted %v rows (%v)\n",
		pgsqlSourceTableName, totalNumOfSourceRows, timeStop)

	return nil
}

