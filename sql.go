package crudiator

import (
	"database/sql"
	"strconv"
	"strings"
)

// Represents a database row (column set)
//
// Since this is an aliased map type, it can readily be serialized.
//
// Call DbRow.HasData() to check if any data was read into the row
type DbRow map[string]any

func (row DbRow) Get(col string) any {
	return row[col]
}

func (row DbRow) Remove(col string) {
	delete(row, col)
}

func (row DbRow) Has(col string) bool {
	_, ok := row[col]
	return ok
}

func (row DbRow) HasData() bool {
	return len(row) > 0
}

// Scan columns at the current cursor into this row. sql.Rows.Next() must have already
// been called before calling this function.
func (row DbRow) Scan(rows *sql.Rows) error {
	var (
		cols, _        = rows.Columns()
		pointers []any = make([]any, len(cols))
		tmp      []any = make([]any, len(cols))
	)

	for _, c := range cols {
		row[c] = nil
	}

	for index := range pointers {
		pointers[index] = &tmp[index]
	}

	err := rows.Scan(pointers...)
	if err != nil {
		return err
	}
	for index, col := range cols {
		row[col] = tmp[index]
	}
	return nil
}

func ParameterizeFields(fields []string, dialect SQLDialect, useAnd bool, startCountFrom ...int) string {
	var separator bool
	var builder strings.Builder
	var start = 1
	var sep string

	if useAnd {
		sep = " AND "
	} else {
		sep = ","
	}

	if len(startCountFrom) > 0 {
		start = startCountFrom[0]
	}

	for _, f := range fields {
		if separator {
			builder.WriteString(sep)
		}
		builder.WriteString(f)
		if !(strings.HasSuffix(f, "IS NULL") || strings.HasSuffix(f, "IS NOT NULL")) {
			builder.WriteRune('=')
			switch dialect {
			case POSTGRESQL:
				builder.WriteRune('$')
				builder.WriteString(strconv.Itoa(start))
			case MYSQL:
				fallthrough
			case SQLITE:
				builder.WriteRune('?')
			}
			start++
		}
		if !separator {
			separator = true
		}
		// start++
	}
	return builder.String()
}

func CreateParameterPlaceholders(count int, dialect SQLDialect) string {
	var builder strings.Builder
	var separator bool

	for i := 0; i < count; i++ {
		if separator {
			builder.WriteRune(',')
		}
		switch dialect {
		case POSTGRESQL:
			builder.WriteRune('$')
			builder.WriteString(strconv.Itoa(i + 1))
		case MYSQL:
			fallthrough
		case SQLITE:
			builder.WriteRune('?')
		}
		if !separator {
			separator = true
		}
	}
	return builder.String()
}
