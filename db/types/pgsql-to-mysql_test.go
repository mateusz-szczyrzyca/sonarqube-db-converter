package types

import (
	"fmt"
	"testing"
	"math/rand"
	"database/sql"
	"strconv"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEF GHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func TestReflectTimeFormat(t *testing.T) {
	for num :=0; num <= 100; num++ {
		randomTableName := randStringBytes(num)
		PgSQLValidStringQuery := fmt.Sprintf("to_char(%v, 'YYYY-MM-DD HH24:MM:SS')", randomTableName)

		if PgSQLValidStringQuery != ReflectTimeFormat(randomTableName) {
			t.Errorf("Format missmatch: (%v) and (%v)", PgSQLValidStringQuery, ReflectTimeFormat(randomTableName))
		}
	}
}

func TestIntegerOrNullToString(t *testing.T) {
	var sqlNullInt sql.NullInt64

	if IntegerOrNullToString(sqlNullInt) != "NULL" {
		t.Errorf("Problem with NULL value: %v != 0\n", sqlNullInt)
	}

	if strconv.FormatInt(sqlNullInt.Int64, 10) != strconv.FormatInt(0, 10) {
		t.Errorf("Problem: %v != 0\n", sqlNullInt)
	}

	sqlNullInt.Valid = true
	sqlNullInt.Int64 = 5
	result := IntegerOrNullToString(sqlNullInt)
	if strconv.FormatInt(sqlNullInt.Int64, 10) != result {
		t.Errorf("Problem: %v is different than %v (should not be)\n", sqlNullInt.Int64, result)
	}
}

func TestFloatOrNullToString(t *testing.T) {
	var sqlNullFloat sql.NullFloat64

	if "NULL" != FloatOrNullToString(sqlNullFloat) {
		t.Errorf("Problem with NULL value: %v != 0\n", sqlNullFloat)
	}

	if strconv.FormatFloat(sqlNullFloat.Float64, 'f', -1, 64) != strconv.FormatFloat(0,'f',-1,64) {
		t.Errorf("Problem: %v != 0\n", sqlNullFloat)
	}

	sqlNullFloat.Valid = true
	sqlNullFloat.Float64 = 5.000
	result := FloatOrNullToString(sqlNullFloat)
	if strconv.FormatFloat(sqlNullFloat.Float64, 'f', -1, 64) != result {
		t.Errorf("Problem: %v is different than %v (should not be)\n", sqlNullFloat.Float64, result)
	}
}

func TestStringOrNullToString(t *testing.T) {
	var sqlNullString sql.NullString

	if "NULL" != StringOrNullToString(sqlNullString) {
		t.Errorf("Problem with zero value: %v != 0\n", sqlNullString)
	}

	sqlNullString.Valid = true
	sqlNullString.String = "test_string"
	if "'test_string'" != StringOrNullToString(sqlNullString) {
		t.Errorf("Problem with test_string: %v\n", sqlNullString)
	}
}

func TestPgBoolToMySQLTinyint(t *testing.T) {
	var bool_value bool

	if 0 != PgBoolToMySQLTinyint(bool_value) {
		t.Errorf("Bool false didn't convert to 0\n")
	}

	bool_value = true
	if 1 != PgBoolToMySQLTinyint(bool_value) {
		t.Errorf("Bool true didn't convert to 1\n")
	}
}
