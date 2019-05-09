package types

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// ReflectTimeFormat parses data from PgSQL format to MySQL (during PgSQL querying)
func ReflectTimeFormat(tableName string) string {
	return fmt.Sprintf("to_char(%v, 'YYYY-MM-DD HH24:MM:SS')", tableName)
}

// IntegerOrNullToString for field with integer type that they
//                           can be null
func IntegerOrNullToString(value sql.NullInt64) string {
	if value.Valid {
		return strconv.FormatInt(value.Int64, 10)
	}
	return "NULL"
}

// FloatOrNullToString for field with integer type that they
//                           can be null
func FloatOrNullToString(value sql.NullFloat64) string {
	if value.Valid {
		return strconv.FormatFloat(value.Float64, 'f', -1, 64)
	}
	return "NULL"
}

// StringOrNullToString for field with integer type that they
//                           can be null
func StringOrNullToString(value sql.NullString) string {
	if value.Valid {
		// Do not pass without special characters to escape function
		if strings.ContainsAny(value.String, "'") {
			return fmt.Sprintf("'%v'", escapeSpecialSQLCharacters(value.String))
		}
		return fmt.Sprintf("'%v'", value.String)
	}
	return "NULL"
}

// PgBoolToMySQLTinyint for casting true/false variables from PgSQL to MySQL
//                   tiny int
func PgBoolToMySQLTinyint(value bool) int {
	if value {
		return 1
	}
	return 0
}

// escapeSpecialSQLCharacters for escaping ' and such characters
//
func escapeSpecialSQLCharacters(queryString string) string {
	var parsedString string

	escapeQuoteRegex := regexp.MustCompile(`'`)
	parsedString = escapeQuoteRegex.ReplaceAllString(queryString, `''`)

	return parsedString
}
