package main

import (
	"database/sql"
	"fmt"
	"time"

	table "sonarqube-db-converter/db/tables"

	"github.com/BurntSushi/toml"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// Configuration ...
type Configuration struct {
	PgDbHost string
	PgDbName string
	PgDbUser string
	PgDbPass string
	MyDbHost string
	MyDbName string
	MyDbUser string
	MyDbPass string
}

// Databases ... struct for keeping DB handlers
type Databases struct {
	DBSource *sql.DB
	DBDest   *sql.DB
}

// Tables for tables operations
type Tables interface {
	GetTableName() string
	GetDataFromDataSource() string
	PrepareMySQLQuery(sql.Rows, chan string, chan error)
	GetSetPrimaryKEYQuery() string
}

func main() {
	// parse config
	var config Configuration
	if _, errorConfigFile := toml.DecodeFile("./config.toml", &config); errorConfigFile != nil {
		panic(errorConfigFile)
	}

	// establish pgsql connection
	pgsqlConnectionString := fmt.Sprintf("user=%v dbname=%v password=%v host=%v sslmode=disable",
		config.PgDbUser, config.PgDbName, config.PgDbPass, config.PgDbHost)
	pgsqlDB, errorPgSQLConnection := sql.Open("postgres", pgsqlConnectionString)
	if errorPgSQLConnection != nil {
		panic(errorPgSQLConnection)
	}

	// establish mysql connection
	mysqlConnectionString := fmt.Sprintf("%v:%v@tcp(%v)/%v?charset=utf8&maxAllowedPacket=133554432",
		config.MyDbUser, config.MyDbPass, config.MyDbHost, config.MyDbName)
	mysqlDB, errorMySQLConnection := sql.Open("mysql", mysqlConnectionString)
	if errorMySQLConnection != nil {
		panic(errorMySQLConnection)
	}

	defer pgsqlDB.Close()
	defer mysqlDB.Close()

	mysqlDB.SetMaxOpenConns(2000)
	mysqlDB.SetMaxIdleConns(0)
	mysqlDB.SetConnMaxLifetime(0)

	destinationQueryChan := make(chan string, 1000)
	errorChan := make(chan error)

	var totalNumOfInsertedRows int64
	var totalNumOfSourceRows int64
	var mysqlTransaction *sql.Tx
	var tableName string
	var numOfRecordsNeededTOCommitTransaction int64
	var timeStart time.Time
	var timeStop time.Duration
	var commitError error
	var transactionError error
	var errorSourceRetrievalQuery error
	var errorSetPrimaryKey error
	var sourceRows *sql.Rows

	///////////////////////////////////////////////////////////////////////////
	conversionPool := []Tables{
		table.ProjectMeasures{TableName: "project_measures"},
		table.FileSources{TableName: "file_sources"},
		table.Projects{TableName: "projects"},
		table.ResourceIndex{TableName: "resource_index"},
		table.ActionPlans{TableName: "action_plans"},
		table.ActiveDashboards{TableName: "active_dashboards"},
		table.ActiveRuleParameters{TableName: "active_rule_parameters"},
		table.ActiveRules{TableName: "active_rules"},
		table.Activities{TableName: "activities"},
		table.CeActivity{TableName: "ce_activity"},
		table.CeQueue{TableName: "ce_queue"},
		table.Characteristics{TableName: "characteristics"},
		table.Dashboards{TableName: "dashboards"},
		table.DuplicationsIndex{TableName: "duplications_index"},
		table.GroupRoles{TableName: "group_roles"},
		table.Groups{TableName: "groups"},
		table.GroupsUsers{TableName: "groups_users"},
		table.IssueFilterFavourites{TableName: "issue_filter_favourites"},
		table.IssueFilters{TableName: "issue_filters"},
		table.LoadedTemplates{TableName: "loaded_templates"},
		table.ManualMeasures{TableName: "manual_measures"},
		table.MeasureFilterFavourites{TableName: "measure_filter_favourites"},
		table.MeasureFilters{TableName: "measure_filters"},
		table.Metrics{TableName: "metrics"},
		table.Notifications{TableName: "notifications"},
		table.PermTemplatesGroups{TableName: "perm_templates_groups"},
		table.PermTemplatesUsers{TableName: "perm_templates_users"},
		table.PermissionTemplates{TableName: "permission_templates"},
		table.ProjectLinks{TableName: "project_links"},
		table.ProjectQprofiles{TableName: "project_qprofiles"},
		table.Properties{TableName: "properties"},
		table.QualityGateConditions{TableName: "quality_gate_conditions"},
		table.QualityGates{TableName: "quality_gates"},
		table.Rules{TableName: "rules"},
		table.RulesParameters{TableName: "rules_parameters"},
		table.RulesProfiles{TableName: "rules_profiles"},
		table.SchemaMigrations{TableName: "schema_migrations"},
		table.UserRoles{TableName: "user_roles"},
		table.Users{TableName: "users"},
		table.WidgetProperties{TableName: "widget_properties"},
		table.Widgets{TableName: "widgets"},
		table.Events{TableName: "events"},
		table.Snapshots{TableName: "snapshots"},
		table.Issues{TableName: "issues"},
		table.IssueChanges{TableName: "issue_changes"},
	}
	///////////////////////////////////////////////////////////////////////////

	for _, currentTable := range conversionPool {
		tableName = currentTable.GetTableName()
		numOfRecordsNeededTOCommitTransaction = 20000
		querySource := currentTable.GetDataFromDataSource()
		sourceRows, errorSourceRetrievalQuery = pgsqlDB.Query(querySource)
		if errorSourceRetrievalQuery != nil {
			fmt.Printf("*** Error (%v): %v\n", tableName, errorSourceRetrievalQuery.Error())
		}

		mysqlTransaction, transactionError = mysqlDB.Begin()
		if transactionError != nil {
			panic(transactionError)
		}

		fmt.Printf("%v: started conversion\n", tableName)
		timeStart = time.Now()
		for sourceRows.Next() {
			totalNumOfSourceRows++

			go func() {
				currentTable.PrepareMySQLQuery(*sourceRows, destinationQueryChan, errorChan)
			}()

			select {
			case chErr := <-errorChan:
				panic(chErr)

			case queryString := <-destinationQueryChan:
				mysqlTransaction.Prepare(queryString)
				_, executionError := mysqlTransaction.Exec(queryString)
				if executionError != nil {
					fmt.Printf("*** Error (%v): exec error after prepare trans: %v (query: {%v})\n",
						tableName, executionError.Error(), queryString)
				} else {
					totalNumOfInsertedRows++
				}
				if totalNumOfSourceRows%numOfRecordsNeededTOCommitTransaction == 0 {
					transError := mysqlTransaction.Commit()
					if transError != nil {
						fmt.Printf("*** Error (%v): transaction error during commit: %v\n",
							tableName, transError.Error())
					} else {
						mysqlTransaction, transactionError = mysqlDB.Begin()
						if transactionError != nil {
							panic(transactionError)
						}
					}
				}
			}
		}

		_, errorSetPrimaryKey = mysqlTransaction.Exec(currentTable.GetSetPrimaryKEYQuery())
		if errorSetPrimaryKey != nil {
			fmt.Printf("*** Error (%v): problem with exec query for setting primary key: %v\n",
				tableName, errorSetPrimaryKey.Error())
		}

		commitError = mysqlTransaction.Commit()
		if commitError != nil {
			fmt.Printf("*** Error (%v): final transaction cannot be committed: %v\n",
				tableName, commitError.Error())
		}

		if totalNumOfInsertedRows != totalNumOfSourceRows {
			fmt.Printf("*** Error (%v): mismatch rows count, source rows: %v, converted rows: %v\n",
				tableName, totalNumOfSourceRows, totalNumOfInsertedRows)
		}

		// Reset counters for a next table
		totalNumOfInsertedRows = 0
		totalNumOfSourceRows = 0
		timeStop = time.Since(timeStart)
		fmt.Printf("%v: finished conversion in %v\n", tableName, timeStop)
	}
}
