package crudiator

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// The dialect to use when interacting with the underlying database
type SQLDialect int

const (
	MYSQL SQLDialect = iota + 1
	POSTGRESQL
	SQLITE
)

type Crudiator interface {
	Create(form DataForm, db *sql.DB) (DbRow, error)
	Read(form DataForm, db *sql.DB, pageable ...Pageable) ([]DbRow, error)

	// Reads a single database row. May return nil,nil if no row exists
	SingleRead(form DataForm, db *sql.DB) (DbRow, error)
	// Updates the specified record and returns the updated row.
	//
	// A single statement is executed for postgres (using the RETURNING keyword) and for
	// all others, two statements are executed; one to update and one for the query.
	Update(form DataForm, db *sql.DB) (DbRow, error)

	Delete(form DataForm, db *sql.DB) (DbRow, error)
}

type PreActionCallback func(editor Editor, form DataForm)

type PostActionCallback func(editor Editor, rows []DbRow)

// Editor is the object that interacts with the underlying object.
//
// One instance can be used multiple times concurrently as no state is stored
type Editor struct {
	softDelete               bool
	softDeleteColumns        []string
	fields                   []Field
	quoteRune                rune
	dialect                  SQLDialect
	preCreate                PreActionCallback
	postCreate               PostActionCallback
	preRead                  PreActionCallback
	postRead                 PostActionCallback
	preUpdate                PreActionCallback
	postUpdate               PostActionCallback
	preDelete                PreActionCallback
	postDelete               PostActionCallback
	fieldList                string
	tableName                string
	tableNameQuoted          string
	pagination               PaginationStrategy
	keysetPaginationField    string
	createStatement          string
	singleSelectionStatement string
	readStatement            string
	updateStatement          string
	deleteStatement          string
	createFields             []string
	readFields               []string
	updateFields             []string
	filterFields             []string
	primaryKeyField          string
	logger                   Logger
	dbg                      bool
}

// MustNewEditor Creates a table editor instance used for CRUD operations on that table.
//
// The function will panic if fields is empty or if a duplicate field is found
func MustNewEditor(table string, dialect SQLDialect, fields ...Field) *Editor {
	l := len(fields)
	if l == 0 {
		panic("fields cannot be empty")
	} else if l >= 2 {
		f := fields[0]
		for i := 1; i < l; i++ {
			// start by checking the next immediate field
			for x := i; x < l; x++ {
				if f.Name == fields[x].Name {
					panic(errors.Errorf("duplicate field '%s' at %d and %d", f.Name, i, x))
				}
			}
			f = fields[i]
		}
	}

	var separator bool = false
	var builder strings.Builder
	var quoteRune rune

	switch dialect {
	case MYSQL:
		fallthrough
	case SQLITE:
		quoteRune = '`'
	case POSTGRESQL:
		quoteRune = '"'
	}

	for _, f := range fields {
		if separator {
			builder.WriteRune(',')
		}
		builder.WriteRune(quoteRune)
		builder.WriteString(f.Name)
		builder.WriteRune(quoteRune)
		if !separator {
			separator = true
		}
	}
	return &Editor{
		tableName:       table,
		fields:          fields,
		dialect:         dialect,
		quoteRune:       quoteRune,
		fieldList:       builder.String(),
		tableNameQuoted: fmt.Sprintf("%c%s%c", quoteRune, table, quoteRune),
	}
}

func (e Editor) invokePreActionCallback(pac PreActionCallback, form DataForm) {
	if pac != nil {
		pac(e, form)
	}
}

func (e Editor) UsesKeysetPagination() bool {
	return e.pagination == KEYSET
}

func (e Editor) invokePostActionCallback(pac PostActionCallback, rows []DbRow) {
	if pac != nil {
		pac(e, rows)
	}
}

// Sets
func (e *Editor) SetLogger(l Logger) *Editor {
	e.logger = l
	return e
}

// Toggles debug mode, which is equivalent to setting a Logger with DEBUG level
func (e *Editor) Debug(b bool) *Editor {
	e.dbg = b
	return e
}

func (e *Editor) OnPreCreate(f PreActionCallback) *Editor {
	e.preCreate = f
	return e
}

func (e *Editor) OnPostCreate(f PostActionCallback) *Editor {
	e.postCreate = f
	return e
}

func (e *Editor) OnPreRead(f PreActionCallback) *Editor {
	e.preRead = f
	return e
}

func (e *Editor) OnPostRead(f PostActionCallback) *Editor {
	e.postRead = f
	return e
}

func (e *Editor) OnPreUpdate(f PreActionCallback) *Editor {
	e.preUpdate = f
	return e
}

func (e *Editor) OnPostUpdate(f PostActionCallback) *Editor {
	e.postUpdate = f
	return e
}

// SoftDelete Indicates whether records in this table should be soft deleted,
// and specifies the columns to be set when the Delete function is called.
//
// If true, a call to 'Delete' is converted to an 'Update', with only
// the given columns being updated.
func (e *Editor) SoftDelete(v bool, columns ...string) *Editor {
	e.softDelete = v
	e.softDeleteColumns = columns
	return e
}

// MustPaginate configures selection query pagination.
//
// The function will panic if strategy is not 'KEYSET' and no fields have been defined
func (e *Editor) MustPaginate(strategy PaginationStrategy, fields ...string) *Editor {
	if strategy == KEYSET {
		if len(fields) == 0 {
			panic(errors.Errorf("Keyset pagination requires a field to be specified"))
		}
		e.keysetPaginationField = fields[0]
	}
	e.pagination = strategy
	return e
}

func (e *Editor) buildFilterFields() *Editor {
	e.filterFields = make([]string, 0)
	for _, f := range e.fields {
		if f.SelectionFilter {
			e.filterFields = append(e.filterFields, fmt.Sprintf("%c%s%c", e.quoteRune, f.Name, e.quoteRune))
		}
	}
	return e
}

func (e *Editor) buildCreateFields() *Editor {
	e.createFields = make([]string, 0)
	for _, f := range e.fields {
		if f.Create {
			e.createFields = append(e.createFields, fmt.Sprintf("%c%s%c", e.quoteRune, f.Name, e.quoteRune))
		}
	}
	return e
}

func (e *Editor) buildReadFields() *Editor {
	e.readFields = make([]string, 0)
	for _, f := range e.fields {
		if f.Read {
			e.readFields = append(e.readFields, fmt.Sprintf("%c%s%c", e.quoteRune, f.Name, e.quoteRune))
		}
	}
	return e
}

func (e *Editor) buildUpdateFields() *Editor {
	e.updateFields = make([]string, 0)
	for _, f := range e.fields {
		if f.Update {
			e.updateFields = append(e.updateFields, fmt.Sprintf("%c%s%c", e.quoteRune, f.Name, e.quoteRune))
		}
	}
	return e
}

func (e *Editor) Build() Crudiator {
	var hasFilters bool
	var builder strings.Builder
	var parameterCount int

	if e.dbg {
		e.logger = NewStdOutLogger(Debug)
	} else {
		if e.logger == nil {
			e.logger = NewNopLogger()
		}
	}

	switch e.dialect {
	case MYSQL:
		fallthrough
	case SQLITE:
		e.quoteRune = '`'
	case POSTGRESQL:
		e.quoteRune = '"'
	}

	e.buildCreateFields().
		buildReadFields().
		buildUpdateFields().
		buildFilterFields()

	for _, f := range e.fields {
		if f.PrimaryKey {
			e.primaryKeyField = f.Name
			break
		}
	}

	// create
	builder.WriteString("INSERT INTO ")
	builder.WriteString(e.tableNameQuoted)
	builder.WriteRune('(')
	builder.WriteString(strings.Join(e.createFields, ","))
	builder.WriteRune(')')
	builder.WriteString(" VALUES (")
	builder.WriteString(CreateParameterPlaceholders(len(e.createFields), e.dialect))
	builder.WriteRune(')')

	if e.dialect == POSTGRESQL {
		builder.WriteString(" RETURNING ")
		builder.WriteString(strings.Join(e.readFields, ","))
	}

	e.createStatement = builder.String()

	// single selection statement
	builder.Reset()
	builder.WriteString("SELECT ")
	builder.WriteString(strings.Join(e.readFields, ","))
	builder.WriteString(" FROM ")
	builder.WriteString(e.tableNameQuoted)
	builder.WriteString(" WHERE (")
	builder.WriteRune(e.quoteRune)
	builder.WriteString(e.primaryKeyField)
	builder.WriteRune(e.quoteRune)

	parameterCount = 0
	if e.dialect == POSTGRESQL {
		builder.WriteString("=$1)")
		parameterCount++
	} else {
		builder.WriteString("=?)")
	}

	if len(e.filterFields) > 0 {
		builder.WriteString(" AND (")
		builder.WriteString(ParameterizeFields(e.filterFields, e.dialect, parameterCount+1))
		builder.WriteRune(')')
	}

	e.singleSelectionStatement = builder.String()

	// Bulk selection
	parameterCount = 0
	builder.Reset()
	builder.WriteString("SELECT ")
	builder.WriteString(strings.Join(e.readFields, ","))
	builder.WriteString(" FROM ")
	builder.WriteString(e.tableNameQuoted)

	if len(e.filterFields) > 0 {
		hasFilters = true
		builder.WriteString(" WHERE ")
		builder.WriteRune('(')
		builder.WriteString(ParameterizeFields(e.filterFields, e.dialect))
		builder.WriteRune(')')

		if e.dialect == POSTGRESQL {
			parameterCount = len(e.filterFields)
		}
	}

	// pagination
	if e.pagination == OFFSET {
		switch e.dialect {
		case SQLITE:
			fallthrough
		case MYSQL:
			builder.WriteString(" LIMIT ? OFFSET ?")
		case POSTGRESQL:
			builder.WriteString(fmt.Sprintf(" OFFSET $%d FETCH NEXT $%d ROWS ONLY", parameterCount+1, parameterCount+2))
			parameterCount += 2
		}
	} else if e.pagination == KEYSET {
		if !hasFilters {
			builder.WriteString(" WHERE (")
		} else {
			builder.WriteString(" AND (")
		}
		builder.WriteRune(e.quoteRune)
		builder.WriteString(e.keysetPaginationField)
		builder.WriteRune(e.quoteRune)
		builder.WriteRune('>')
		if e.dialect == POSTGRESQL {
			builder.WriteRune('$')
			builder.WriteString(strconv.Itoa(parameterCount + 1))

			builder.WriteString(") ORDER BY ")
			builder.WriteRune(e.quoteRune)
			builder.WriteString(e.keysetPaginationField)
			builder.WriteRune(e.quoteRune)

			builder.WriteString(" ASC LIMIT ")
			builder.WriteRune('$')
			builder.WriteString(strconv.Itoa(parameterCount + 2))
		} else {
			builder.WriteString("?) ORDER BY ")
			builder.WriteRune(e.quoteRune)
			builder.WriteString(e.keysetPaginationField)
			builder.WriteRune(e.quoteRune)
			builder.WriteString(" ASC LIMIT ?")
		}
	}

	e.readStatement = builder.String()
	builder.Reset()
	parameterCount = 0

	builder.WriteString("UPDATE ")
	builder.WriteString(e.tableNameQuoted)
	builder.WriteString(" SET ")
	builder.WriteString(ParameterizeFields(e.updateFields, e.dialect))
	builder.WriteString(" WHERE ")
	builder.WriteRune(e.quoteRune)
	builder.WriteString(e.primaryKeyField)
	builder.WriteRune(e.quoteRune)
	builder.WriteRune('=')

	if e.dialect == POSTGRESQL {
		parameterCount = 1 + len(e.updateFields)
		builder.WriteRune('$')
		builder.WriteString(strconv.Itoa(parameterCount))
		parameterCount++
	} else {
		builder.WriteRune('?')
	}

	if len(e.filterFields) > 0 {
		builder.WriteString(" AND ")
		builder.WriteRune('(')
		builder.WriteString(ParameterizeFields(e.filterFields, e.dialect, parameterCount))
		builder.WriteRune(')')
	}

	if e.dialect == POSTGRESQL {
		builder.WriteString(" RETURNING ")
		builder.WriteString(strings.Join(e.readFields, ","))
	}

	e.updateStatement = builder.String()

	builder.Reset()
	parameterCount = 0

	// delete statement
	if e.softDelete {
		builder.WriteString("UPDATE ")
		builder.WriteString(e.tableNameQuoted)
		builder.WriteString(" SET ")
		builder.WriteString(ParameterizeFields(e.softDeleteColumns, e.dialect))
	} else {
		builder.WriteString("DELETE FROM ")
		builder.WriteString(e.tableNameQuoted)
	}

	if e.dialect == POSTGRESQL {
		parameterCount = len(e.softDeleteColumns)
	}

	builder.WriteString(" WHERE ")
	builder.WriteRune(e.quoteRune)
	builder.WriteString(e.primaryKeyField)
	builder.WriteRune(e.quoteRune)
	builder.WriteRune('=')

	if e.dialect == POSTGRESQL {
		builder.WriteRune('$')
		builder.WriteString(strconv.Itoa(parameterCount + 1))
		parameterCount++
	} else {
		builder.WriteRune('?')
	}

	if len(e.filterFields) > 0 {
		builder.WriteString(" AND ")
		builder.WriteRune('(')
		builder.WriteString(ParameterizeFields(e.filterFields, e.dialect, parameterCount+1))
		builder.WriteRune(')')
	}

	e.deleteStatement = builder.String()

	e.logger.Debug("create statement => %s", e.createStatement)
	e.logger.Debug("read statement => %s", e.readStatement)
	e.logger.Debug("update statement => %s", e.updateStatement)
	e.logger.Debug("delete statement => %s", e.deleteStatement)
	e.logger.Debug("single selection statement => %s", e.singleSelectionStatement)

	return e
}

// Returns values in order of field occurrence
func (e Editor) getFieldValues(fields []string, form DataForm) []any {
	var data []any = make([]any, len(fields))
	for i, f := range fields {
		isquoted := f[0] == byte(e.quoteRune) && f[len(f)-1] == byte(e.quoteRune)
		if isquoted {
			f = strings.Trim(f, string(e.quoteRune))
		}
		data[i] = form.Get(f)
	}
	return data
}

func (e Editor) getFieldvalue(field string, form DataForm) any {
	return form.Get(e.unquote(field))
}

func (e Editor) scanRows(rows *sql.Rows) ([]DbRow, error) {
	var rowset []DbRow = make([]DbRow, 0)
	for rows.Next() {
		row := DbRow{}
		if err := row.Scan(rows); err != nil {
			return nil, err
		}
		rowset = append(rowset, row)
	}
	return rowset, nil
}

func (e Editor) scanRow(rows *sql.Rows) (DbRow, error) {
	scanned, err := e.scanRows(rows)
	if err != nil {
		return nil, err
	}
	return scanned[0], nil
}

func (e Editor) unquote(name string) string {
	if len(name) >= 3 {
		if name[0] == byte(e.quoteRune) && name[len(name)-1] == byte(e.quoteRune) {
			return strings.Trim(name, string(e.quoteRune))
		}
	}
	return name
}

func (e Editor) SingleRead(form DataForm, db *sql.DB) (DbRow, error) {
	var row DbRow
	args := e.getFieldValues(e.readFields, form)
	rows, err := db.Query(e.singleSelectionStatement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		err := row.Scan(rows)
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}

func (e Editor) Create(form DataForm, db *sql.DB) (DbRow, error) {
	var row DbRow
	e.invokePreActionCallback(e.preCreate, form)
	fieldValues := e.getFieldValues(e.createFields, form)

	//var query string
	switch e.dialect {
	case SQLITE:
		fallthrough
	case MYSQL:
		res, err := db.Exec(e.createStatement, fieldValues...)
		if err != nil {
			return nil, err
		}
		identifier, err := res.LastInsertId()
		if err != nil {
			return nil, err
		}
		form.Set(e.unquote(e.primaryKeyField), identifier)
		dbRow, err := e.SingleRead(form, db)
		if err != nil {
			return nil, err
		}
		row = dbRow
	case POSTGRESQL:
		rows, err := db.Query(e.createStatement, fieldValues...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		row, err = e.scanRow(rows)
		if err != nil {
			return nil, err
		}
	}
	e.invokePostActionCallback(e.postCreate, []DbRow{row})
	return row, nil
}

func (e Editor) Read(form DataForm, db *sql.DB, pageable ...Pageable) ([]DbRow, error) {
	var results []DbRow
	e.invokePreActionCallback(e.preRead, form)
	fieldValues := e.getFieldValues(e.filterFields, form)

	if len(pageable) != 0 {
		p := pageable[0]
		if e.UsesKeysetPagination() {
			fieldValues = append(fieldValues, p.KeysetValue(), p.Size())
		} else {
			fieldValues = append(fieldValues, p.Offset(), p.Size())
		}
	}

	switch e.dialect {
	case MYSQL:
		fallthrough
	case SQLITE:
		fallthrough
	case POSTGRESQL:
		rows, err := db.Query(e.readStatement, fieldValues...)
		if err != nil {
			return nil, err
		}
		results, err = e.scanRows(rows)
		if err != nil {
			return nil, err
		}
	}
	e.invokePostActionCallback(e.postRead, results)
	return results, nil

}

func (e Editor) Update(form DataForm, db *sql.DB) (DbRow, error) {
	var results DbRow

	e.invokePreActionCallback(e.preUpdate, form)
	fieldValues := e.getFieldValues(e.updateFields, form)

	pkv := e.getFieldvalue(e.primaryKeyField, form)
	fieldValues = append(fieldValues, pkv)

	if len(e.filterFields) > 0 {
		fieldValues = append(fieldValues, e.getFieldValues(e.filterFields, form)...)
	}

	switch e.dialect {
	case SQLITE:
		fallthrough
	case MYSQL:
		_, err := db.Exec(e.updateStatement, fieldValues...)
		if err != nil {
			return nil, err
		}
		result, err := e.SingleRead(form, db)
		if err != nil {
			return nil, err
		}
		results = result
	case POSTGRESQL:
		rows, err := db.Query(e.updateStatement, fieldValues...)
		if err != nil {
			return nil, err
		}
		results, err = e.scanRow(rows)
		if err != nil {
			return nil, err
		}
	}
	e.invokePostActionCallback(e.postUpdate, []DbRow{results})
	return results, nil
}

func (e Editor) Delete(form DataForm, db *sql.DB) (DbRow, error) {
	var results DbRow
	e.invokePreActionCallback(e.preDelete, form)
	switch e.dialect {
	case SQLITE:
	case MYSQL:
	case POSTGRESQL:
	}
	e.invokePostActionCallback(e.postDelete, []DbRow{results})
	return results, nil
}

type Field struct {
	PrimaryKey bool
	Name       string
	Alias      string
	Create     bool
	Read       bool
	Update     bool
	Unique     bool
	// Mark this field as a selection filter when 'Read()' is called
	//
	// Example
	//	studentEditor := MustNewEditor(
	//		"students",
	//		POSTGRESQL,
	//		NewField("name", IncludeAlways),
	//		NewField("school_id", IncludeAlways, IsSelectionFilter)
	//	)
	//	editor.Read() // SELECT "name", school_id FROM students WHERE school_id = $1
	SelectionFilter bool
}

type FieldOption func(f *Field)

var (
	IsPrimaryKey  FieldOption = func(f *Field) { f.PrimaryKey = true }
	IncludeAlways FieldOption = func(f *Field) {
		f.Read = true
		f.Create = true
		f.Update = true
	}
	IncludeOnCreate   FieldOption = func(f *Field) { f.Create = true }
	IncludeOnUpdate   FieldOption = func(f *Field) { f.Update = true }
	IncludeOnRead     FieldOption = func(f *Field) { f.Read = true }
	IsUnique          FieldOption = func(f *Field) { f.Unique = true }
	IsSelectionFilter FieldOption = func(f *Field) { f.SelectionFilter = true }
)

func (f *Field) SetValue() *Field {
	return f
}

func NewField(name string, options ...FieldOption) Field {
	f := Field{Name: name}
	for _, o := range options {
		o(&f)
	}
	return f
}
