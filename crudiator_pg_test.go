package crudiator_test

import (
	"context"
	"database/sql"
	"io"
	"os"
	"testing"
	"time"

	"github.com/SharkFourSix/crudiator"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"

	_ "github.com/lib/pq"
)

func seedDb(filename string, db *sql.DB) error {
	fd, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fd.Close()
	bytes, err := io.ReadAll(fd)
	statements := string(bytes)
	if err != nil {
		return err
	}
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	_, err = tx.Exec(statements)
	if err != nil {
		return err
	}
	err = tx.Commit()
	return err
}

func checkError(err error, t *testing.T, msg ...string) {
	if err != nil {
		if len(msg) > 0 {
			t.Fatalf("%s: %s", err, msg[0])
		} else {
			t.Fatal(err)
		}
	}
}

var (
	studentCrudiator = crudiator.MustNewEditor(
		"students",
		crudiator.POSTGRESQL,
		crudiator.NewField("id", crudiator.IsPrimaryKey, crudiator.IncludeOnRead),
		crudiator.NewField("name", crudiator.IncludeAlways),
		crudiator.NewField("age", crudiator.IncludeAlways),
		crudiator.NewField("created_at", crudiator.IncludeOnCreate, crudiator.IncludeOnRead),
		crudiator.NewField("deleted_at", crudiator.IncludeOnRead, crudiator.IsSelectionFilter, crudiator.IsNullConstant),
		crudiator.NewField("updated_at", crudiator.IncludeOnUpdate, crudiator.IncludeOnRead),
		crudiator.NewField("school_id", crudiator.IncludeOnCreate, crudiator.IncludeOnRead, crudiator.IsSelectionFilter),
	).SoftDelete(true, "deleted_at").
		SetLogger(crudiator.NewStdOutLogger(crudiator.Debug)).
		MustPaginate(crudiator.KEYSET, "id").
		Build()

	schoolCrudiator = crudiator.MustNewEditor(
		"schools",
		crudiator.POSTGRESQL,
		crudiator.NewField("id", crudiator.IsPrimaryKey, crudiator.IncludeOnRead),
		crudiator.NewField("school_name", crudiator.IncludeAlways),
		crudiator.NewField("deleted_at", crudiator.IncludeAlways),
	).
		SetLogger(crudiator.NewStdOutLogger(crudiator.Debug)).
		MustPaginate(crudiator.KEYSET, "id").
		Build()
)

func getPgConnection() (*sql.DB, error) {
	dbInfo := struct {
		Dsn string `toml:"PG_DSN"`
	}{}
	_, err := toml.DecodeFile("testdata/db.toml", &dbInfo)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("postgres", dbInfo.Dsn)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func TestPostgresqlCreate(t *testing.T) {
	db, err := getPgConnection()
	checkError(err, t)
	defer db.Close()

	checkError(seedDb("testdata/pg_seed.sql", db), t)

	jsonData := struct {
		Name      string     `json:"name"`
		Age       int        `json:"age"`
		CreatedAt time.Time  `json:"created_at"`
		UpdatedAt *time.Time `json:"updated_at"`
		DeletedAt *time.Time `json:"deleted_at"`
		SchoolID  int        `json:"school_id"`
	}{
		Name:      "John Doe",
		Age:       25,
		CreatedAt: time.Now(),
		UpdatedAt: nil,
		DeletedAt: nil,
		SchoolID:  1,
	}

	form := crudiator.FromJsonStruct(&jsonData)

	row, err := studentCrudiator.Create(form, db)
	checkError(err, t)
	require.Equal(t, "John Doe", row["name"])
	require.Equal(t, int64(25), row["age"])
	require.Equal(t, int64(1001), row["id"])
	require.Equal(t, int64(1), row.Get("school_id"))
}

func TestPostgresqlRead(t *testing.T) {
	db, err := getPgConnection()
	checkError(err, t)
	defer db.Close()

	checkError(seedDb("testdata/pg_seed.sql", db), t)

	jsonData := struct {
		Name      string     `json:"name"`
		Age       int        `json:"age"`
		CreatedAt time.Time  `json:"created_at"`
		UpdatedAt *time.Time `json:"updated_at"`
		DeletedAt *time.Time `json:"deleted_at"`
		SchoolID  int        `json:"school_id"`
	}{
		Name:      "John Doe",
		Age:       25,
		CreatedAt: time.Now(),
		UpdatedAt: nil,
		DeletedAt: nil,
		SchoolID:  1,
	}

	form := crudiator.FromJsonStruct(&jsonData)

	key := crudiator.NewKeysetPaging(0, 10)
	rows, err := studentCrudiator.Read(form, db, key)
	checkError(err, t)
	require.Equal(t, 10, len(rows))
}

func TestPostgresqlUpdate(t *testing.T) {
	db, err := getPgConnection()
	checkError(err, t)
	defer db.Close()

	checkError(seedDb("testdata/pg_seed.sql", db), t)

	updatedAt := time.Now()

	jsonData := struct {
		ID        int        `json:"id"`
		Name      string     `json:"name"`
		Age       int        `json:"age"`
		CreatedAt time.Time  `json:"created_at"`
		UpdatedAt *time.Time `json:"updated_at"`
		DeletedAt *time.Time `json:"deleted_at"`
		SchoolID  int        `json:"school_id"`
	}{
		ID:        1,
		Name:      "John Doe",
		Age:       25,
		CreatedAt: time.Now(),
		UpdatedAt: &updatedAt,
		DeletedAt: nil,
		SchoolID:  1,
	}

	layout := "2001-02-03 04:05"

	form := crudiator.FromJsonStruct(&jsonData)
	row, err := studentCrudiator.Update(form, db)
	checkError(err, t)
	require.True(t, row.HasData())
	require.Equal(t, row["updated_at"].(time.Time).Format(layout), updatedAt.Format(layout))
}

func TestPostgresqlDelete(t *testing.T) {
	db, err := getPgConnection()
	checkError(err, t)
	defer db.Close()

	checkError(seedDb("testdata/pg_seed.sql", db), t)

	jsonData := struct {
		Id        int        `json:"id"`
		Name      string     `json:"school_name"`
		DeletedAt *time.Time `json:"deleted_at"`
	}{
		Name:      "UCLA",
		DeletedAt: nil,
	}

	form := crudiator.FromJsonStruct(&jsonData)

	row, err := schoolCrudiator.Create(form, db)
	checkError(err, t)
	require.Equal(t, "UCLA", row["school_name"])

	// soft delete
	form.Set("id", row["id"])
	deletedRow, err := schoolCrudiator.Delete(form, db)
	checkError(err, t)
	require.NotNil(t, deletedRow, "hard deletion")
}
