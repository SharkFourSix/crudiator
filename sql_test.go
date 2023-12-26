package crudiator_test

import (
	"testing"

	"github.com/SharkFourSix/crudiator"
)

func TestPostgresPlaceholders(t *testing.T) {
	actual := crudiator.CreateParameterPlaceholders(5, crudiator.POSTGRESQL)
	expected := "$1,$2,$3,$4,$5"
	if actual != expected {
		t.Fatal("postgresql placeholder generation failed")
	}
}

func TestSqliteMysqlPlaceholders(t *testing.T) {
	mysqlActual := crudiator.CreateParameterPlaceholders(5, crudiator.MYSQL)
	sqliteActual := crudiator.CreateParameterPlaceholders(5, crudiator.SQLITE)
	expected := "?,?,?,?,?"
	if mysqlActual != expected {
		t.Fatal("mysql placeholder generation failed")
	}
	if sqliteActual != expected {
		t.Fatal("sqlite placeholder generation failed")
	}
}
