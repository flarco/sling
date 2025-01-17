package database

import (
	"database/sql"
	"encoding/json"
	"runtime/debug"
	"strings"
	"unicode"

	"github.com/flarco/g"
	"github.com/gobwas/glob"
	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// Table represents a schemata table
type Table struct {
	Name     string      `json:"name"`
	Schema   string      `json:"schema"`
	Database string      `json:"database,omitempty"`
	IsView   bool        `json:"is_view,omitempty"` // whether is a view
	SQL      string      `json:"sql,omitempty"`
	DDL      string      `json:"ddl,omitempty"`
	Dialect  dbio.Type   `json:"dialect,omitempty"`
	Columns  iop.Columns `json:"columns,omitempty"`
	Keys     TableKeys   `json:"keys,omitempty"`

	Raw string `json:"raw"`

	limit, offset int
}

var ChunkByColumn = func(conn Connection, table Table, c string, p int) ([]Table, error) {
	return []Table{table}, nil
}

func (t *Table) IsQuery() bool {
	return t.SQL != ""
}

func (t *Table) IsProcedural() bool {
	// Convert to lowercase for case-insensitive matching
	sqlLower := strings.ToLower(t.Raw)

	// declare and cursor are before select
	declareIndex := strings.Index(sqlLower, "declare")
	cursorIndex := strings.Index(sqlLower, "cursor")
	selectIndex := strings.Index(sqlLower, "select")
	hintIndex := strings.Index(sqlLower, "sling-procedural-sql")

	// should be found in order DECLARE, CURSOR and SELECT
	if declareIndex > -1 && declareIndex < cursorIndex && cursorIndex < selectIndex {
		return true
	}
	if hintIndex > -1 {
		return true
	}

	return false
}

func (t *Table) MarshalJSON() ([]byte, error) {
	type Alias Table
	return json.Marshal(&struct {
		*Alias
		FullName string `json:"full_name"`
		FDQN     string `json:"fdqn"`
	}{
		Alias:    (*Alias)(t),
		FullName: t.FullName(),
		FDQN:     t.FDQN(),
	})
}

func (t *Table) SetKeys(sourcePKCols []string, updateCol string, tableKeys TableKeys) error {
	// set keys
	t.Keys = tableKeys

	eG := g.ErrorGroup{}

	if len(t.Columns) == 0 {
		return nil // columns are missing, cannot check
	}

	if len(sourcePKCols) > 0 {
		// set true PK only when StarRocks, we don't want to create PKs on target table implicitly
		if t.Dialect == dbio.TypeDbStarRocks {
			eG.Capture(t.Columns.SetKeys(iop.PrimaryKey, sourcePKCols...))
		}
		eG.Capture(t.Columns.SetMetadata(iop.PrimaryKey.MetadataKey(), "source", sourcePKCols...))
	}

	if updateCol != "" {
		eG.Capture(t.Columns.SetMetadata(iop.UpdateKey.MetadataKey(), "source", updateCol))
	}

	if tkMap := tableKeys; tkMap != nil {
		for tableKey, keys := range tkMap {
			eG.Capture(t.Columns.SetKeys(tableKey, keys...))
		}
	}

	return eG.Err()
}

func (t *Table) FullName() string {
	q := GetQualifierQuote(t.Dialect)

	fdqnArr := []string{}
	if t.Schema != "" {
		fdqnArr = append(fdqnArr, q+t.Schema+q)
	}
	if t.Name != "" {
		if t.Name == "*" {
			fdqnArr = append(fdqnArr, t.Name)
		} else {
			fdqnArr = append(fdqnArr, q+t.Name+q)
		}
	}
	return strings.Join(fdqnArr, ".")
}

func (t *Table) DatabaseQ() string {
	q := GetQualifierQuote(t.Dialect)
	return q + t.Database + q
}

func (t *Table) SchemaQ() string {
	q := GetQualifierQuote(t.Dialect)
	return q + t.Schema + q
}

func (t *Table) NameQ() string {
	q := GetQualifierQuote(t.Dialect)
	return q + t.Name + q
}

func (t *Table) FDQN() string {
	q := GetQualifierQuote(t.Dialect)

	fdqnArr := []string{}
	if t.Database != "" {
		fdqnArr = append(fdqnArr, q+t.Database+q)
	}
	if t.Schema != "" {
		fdqnArr = append(fdqnArr, q+t.Schema+q)
	}
	if t.Name != "" {
		if t.Name == "*" {
			fdqnArr = append(fdqnArr, t.Name)
		} else {
			fdqnArr = append(fdqnArr, q+t.Name+q)
		}
	}
	return strings.Join(fdqnArr, ".")
}

func (t *Table) Clone() Table {
	return Table{
		Name:     t.Name,
		Schema:   t.Schema,
		Database: t.Database,
		IsView:   t.IsView,
		SQL:      t.SQL,
		DDL:      t.DDL,
		Dialect:  t.Dialect,
		Columns:  t.Columns,
		Keys:     t.Keys,
		Raw:      t.Raw,
		limit:    t.limit,
		offset:   t.offset,
	}
}

func (t *Table) ColumnsMap() map[string]iop.Column {
	columns := map[string]iop.Column{}
	for _, column := range t.Columns {
		key := strings.ToLower(column.Name)
		columns[key] = column
	}
	return columns
}

type SelectOptions struct {
	Fields []string
	Offset int
	Limit  int
	Where  string
}

func (t *Table) Select(Opts ...SelectOptions) (sql string) {
	opts := SelectOptions{}
	if len(Opts) > 0 {
		opts = Opts[0]
	}
	// set to internal value if not specified
	limit := lo.Ternary(opts.Limit == 0, t.limit, opts.Limit)
	offset := lo.Ternary(opts.Offset == 0, t.offset, opts.Offset)
	fields := opts.Fields

	switch t.Dialect {
	case dbio.TypeDbPrometheus:
		return t.SQL
	case dbio.TypeDbMongoDB, dbio.TypeDbElasticsearch:
		m, _ := g.UnmarshalMap(t.SQL)
		if m == nil {
			m = g.M()
		}
		if opts.Where != "" {
			var where any
			g.Unmarshal(opts.Where, &where)
			m["filter"] = where // json array or object
		}

		if len(fields) > 0 && fields[0] != "*" {
			m["fields"] = lo.Map(fields, func(v string, i int) string {
				return strings.TrimSpace(v)
			})
		}

		if len(m) > 0 {
			return g.Marshal(m)
		}
		return t.SQL
	}

	isSQLServer := g.In(t.Dialect, dbio.TypeDbSQLServer, dbio.TypeDbAzure, dbio.TypeDbAzureDWH)
	startsWith := strings.HasPrefix(strings.TrimSpace(strings.ToLower(t.SQL)), "with")
	whereClause := lo.Ternary(opts.Where != "", g.F(" where (%s)", opts.Where), "")
	whereAnd := lo.Ternary(opts.Where != "", g.F(" and (%s)", opts.Where), "")

	fields = lo.Map(fields, func(f string, i int) string {
		q := GetQualifierQuote(t.Dialect)
		f = strings.TrimSpace(f)
		if f == "*" {
			return f
		}
		return q + strings.ReplaceAll(f, q, "") + q
	})

	template, err := t.Dialect.Template()
	if err != nil {
		return
	}

	fieldsStr := lo.Ternary(len(fields) > 0, strings.Join(fields, ", "), "*")

	// auto convert to json if needed
	{
		switch t.Dialect {
		case dbio.TypeDbBigQuery:
			var toJsonCols iop.Columns

			for _, col := range t.Columns {
				dtType := strings.ToLower(col.DbType)
				if strings.HasPrefix(dtType, "array") ||
					strings.HasPrefix(dtType, "record") ||
					strings.HasPrefix(dtType, "struct") {
					toJsonCols = append(toJsonCols, col)
				}
			}

			if len(fields) == 0 {
				replaceExprs := []string{}
				for _, col := range toJsonCols {
					colQ := t.Dialect.Quote(col.Name)
					expr := g.F("safe.parse_json(to_json_string(%s)) as %s", colQ, colQ)
					replaceExprs = append(replaceExprs, expr)
				}
				if len(replaceExprs) > 0 {
					// append replace
					fieldsStr = g.F("* replace(%s)", strings.Join(replaceExprs, ", "))
				}
			} else {
				fieldsExprs := []string{}
				for _, field := range opts.Fields {
					field = strings.TrimSpace(field)
					colQ := t.Dialect.Quote(field)
					if col := toJsonCols.GetColumn(field); col != nil {
						expr := g.F("safe.parse_json(to_json_string(%s)) as %s", colQ, colQ)
						fieldsExprs = append(fieldsExprs, expr)
					} else {
						fieldsExprs = append(fieldsExprs, colQ)
					}
				}
				fieldsStr = strings.Join(fieldsExprs, ", ")
			}
		}
	}

	if t.IsQuery() {
		if len(fields) > 0 && !(len(fields) == 1 && fields[0] == "*") && !(isSQLServer && startsWith) {
			sql = g.F("select %s from (\n%s\n) t", fieldsStr, t.SQL)
		} else {
			sql = t.SQL
		}
	} else {
		if t.Dialect == dbio.TypeDbProton {
			sql = g.F("select %s from table(%s)", fieldsStr, t.FDQN())
		} else {
			sql = g.F("select %s from %s", fieldsStr, t.FDQN())
		}
		if opts.Where != "" {
			sql = g.F("%s where %s", sql, opts.Where)
		}
	}

	if limit > 0 && !strings.Contains(sql, "{limit}") {
		if isSQLServer && startsWith {
			// leave it alone since it starts with WITH
		} else if t.IsQuery() {
			sql = g.R(
				template.Core["limit_sql"],
				"sql", sql,
				"where_cond", opts.Where,
				"where_clause", whereClause,
				"where_and", whereAnd,
				"limit", cast.ToString(limit),
				"offset", cast.ToString(offset),
			)
		} else {
			key := lo.Ternary(offset > 0, "limit_offset", "limit")
			sql = g.R(
				template.Core[key],
				"fields", fieldsStr,
				"table", t.FDQN(),
				"where_cond", opts.Where,
				"where_clause", whereClause,
				"where_and", whereAnd,
				"limit", cast.ToString(limit),
				"offset", cast.ToString(offset),
			)
		}
	}

	// replace any provided placeholders
	sql = g.R(sql,
		"limit", cast.ToString(limit),
		"offset", cast.ToString(offset),
		"where_cond", opts.Where,
		"where_clause", whereClause,
		"where_and", whereAnd,
	)

	return
}

type TableKeys map[iop.KeyType][]string

// Database represents a schemata database
type Database struct {
	Name    string            `json:"name"`
	Schemas map[string]Schema `json:"schemas"`
}

func (db *Database) Tables() map[string]Table {
	tables := map[string]Table{}
	for _, schema := range db.Schemas {
		for _, table := range schema.Tables {
			key := strings.ToLower(g.F("%s.%s", schema.Name, table.Name))
			tables[key] = table
		}
	}
	return tables
}

func (db *Database) Columns() map[string]iop.Column {
	columns := map[string]iop.Column{}
	for _, schema := range db.Schemas {
		for _, table := range schema.Tables {
			for _, column := range table.Columns {
				key := strings.ToLower(g.F("%s.%s.%s", schema.Name, table.Name, column.Name))
				columns[key] = column
			}
		}
	}
	return columns
}

// Schema represents a schemata schema
type Schema struct {
	Name     string           `json:"name"`
	Database string           `json:"database"`
	Tables   map[string]Table `json:"tables"`
}

func (schema *Schema) Columns() map[string]iop.Column {
	columns := map[string]iop.Column{}
	for _, table := range schema.Tables {
		for _, column := range table.Columns {
			key := strings.ToLower(g.F("%s.%s", table.Name, column.Name))
			columns[key] = column
		}
	}
	return columns
}

// ToData converts schema objects to tabular format
func (schema *Schema) ToData() (data iop.Dataset) {
	columns := []string{"schema_name", "table_name", "is_view", "column_id", "column_name", "column_type"}
	data = iop.NewDataset(iop.NewColumnsFromFields(columns...))

	for _, table := range schema.Tables {
		for _, col := range table.Columns {
			row := []interface{}{schema.Name, table.Name, table.IsView, col.Position, col.Name, col.DbType}
			data.Rows = append(data.Rows, row)
		}
	}
	return
}

type SchemataLevel string

const (
	SchemataLevelSchema SchemataLevel = "schema"
	SchemataLevelTable  SchemataLevel = "table"
	SchemataLevelColumn SchemataLevel = "column"
)

// Schemata contains the full schema for a connection
type Schemata struct {
	Databases map[string]Database `json:"databases"`
	conn      Connection          `json:"-"`
}

// LoadTablesJSON loads from a json string
func (s *Schemata) LoadTablesJSON(payload string) error {
	tables := map[string]Table{}
	err := g.Unmarshal(payload, &tables)
	if err != nil {
		return g.Error(err, "could not unmarshal TablesJSON")
	}

	// reconstruct data
	databases := map[string]Database{}
	for key, table := range tables {
		keyArr := strings.Split(key, ".")
		if len(keyArr) != 3 {
			return g.Error("table key must be formatted as `database.schema.table`, got `%s`", key)
		}

		databaseName := strings.ToLower(keyArr[0])
		schemaName := strings.ToLower(keyArr[1])
		tableName := strings.ToLower(keyArr[2])

		if _, ok := databases[databaseName]; !ok {
			databases[databaseName] = Database{
				Name:    table.Database,
				Schemas: map[string]Schema{},
			}
		}
		database := databases[databaseName]

		if _, ok := database.Schemas[schemaName]; !ok {
			database.Schemas[schemaName] = Schema{
				Name:   table.Schema,
				Tables: map[string]Table{},
			}
		}
		schema := database.Schemas[schemaName]

		// fill in positions
		for i := range table.Columns {
			table.Columns[i].Position = i + 1
		}

		// store data
		schema.Tables[tableName] = table
		database.Schemas[schemaName] = schema
		databases[databaseName] = database
	}

	s.Databases = databases

	return nil
}

// Database returns the first encountered database
func (s *Schemata) Database() Database {
	for _, db := range s.Databases {
		return db
	}
	return Database{}
}

func (s *Schemata) Tables(filters ...string) map[string]Table {
	tables := map[string]Table{}
	for _, db := range s.Databases {
		for _, schema := range db.Schemas {
			for _, table := range schema.Tables {
				if len(filters) == 0 || g.IsMatched(filters, table.Name) {
					key := strings.ToLower(g.F("%s.%s.%s", db.Name, schema.Name, table.Name))
					tables[key] = table
				}
			}
		}
	}
	return tables
}

func (s *Schemata) Columns(filters ...string) map[string]iop.Column {
	columns := map[string]iop.Column{}
	for _, db := range s.Databases {
		for _, schema := range db.Schemas {
			for _, table := range schema.Tables {
				for _, column := range table.Columns {
					if len(filters) == 0 || g.IsMatched(filters, column.Name) {
						if !column.Type.IsValid() {
							// get general type
							column.Type = NativeTypeToGeneral(column.Name, column.DbType, s.conn)
						}
						column.SetLengthPrecisionScale()
						key := strings.ToLower(g.F("%s.%s.%s.%s", db.Name, schema.Name, table.Name, column.Name))
						columns[key] = column
					}
				}
			}
		}
	}
	return columns
}

func (s *Schemata) Filtered(columnLevel bool, filters ...string) (ns Schemata) {
	if columnLevel {
		return s.filterColumns(filters...)
	}
	return s.filterTables(filters...)
}

func (s *Schemata) filterTables(filters ...string) (ns Schemata) {
	ns = Schemata{Databases: map[string]Database{}, conn: s.conn}

	var gc *glob.Glob
	if len(filters) == 0 {
		return *s
	} else if len(filters) == 1 && (strings.Contains(filters[0], "*") || strings.Contains(filters[0], "?")) {
		val, err := glob.Compile(strings.ToLower(filters[0]))
		if err == nil {
			gc = &val
		}
	}

	matchedTables := lo.Filter(lo.Values(s.Tables()), func(t Table, i int) bool {
		key := strings.ToLower(g.F("%s.%s", t.Schema, t.Name))
		if gc != nil {
			return (*gc).Match(key)
		}
		return g.IsMatched(filters, key)
	})

	if len(matchedTables) == 0 {
		return
	}

	for _, table := range matchedTables {
		db, ok := ns.Databases[strings.ToLower(table.Database)]
		if !ok {
			db = Database{
				Name:    table.Database,
				Schemas: map[string]Schema{},
			}
		}

		schema, ok := db.Schemas[strings.ToLower(table.Schema)]
		if !ok {
			schema = Schema{
				Name:   table.Schema,
				Tables: map[string]Table{},
			}
		}

		if _, ok := schema.Tables[strings.ToLower(table.Name)]; !ok {
			schema.Tables[strings.ToLower(table.Name)] = table
		}

		db.Schemas[strings.ToLower(table.Schema)] = schema
		ns.Databases[strings.ToLower(table.Database)] = db
	}

	return ns
}

func (s *Schemata) filterColumns(filters ...string) (ns Schemata) {
	ns = Schemata{Databases: map[string]Database{}, conn: s.conn}

	var gc *glob.Glob
	if len(filters) == 0 {
		return *s
	} else if len(filters) == 1 && (strings.Contains(filters[0], "*") || strings.Contains(filters[0], "?")) {
		val, err := glob.Compile(strings.ToLower(filters[0]))
		if err == nil {
			gc = &val
		}
	}

	matchedColumns := lo.Filter(lo.Values(s.Columns()), func(col iop.Column, i int) bool {
		keyTable := strings.ToLower(g.F("%s.%s", col.Schema, col.Table))
		keyCol := strings.ToLower(g.F("%s.%s.%s", col.Schema, col.Table, col.Name))
		if gc != nil {
			return (*gc).Match(keyCol)
		}
		return g.IsMatched(filters, keyCol) || g.IsMatched(filters, keyTable)
	})

	if len(matchedColumns) == 0 {
		return
	}

	for _, col := range matchedColumns {
		db, ok := ns.Databases[strings.ToLower(col.Database)]
		if !ok {
			db = Database{
				Name:    col.Database,
				Schemas: map[string]Schema{},
			}
		}

		schema, ok := db.Schemas[strings.ToLower(col.Schema)]
		if !ok {
			schema = Schema{
				Name:   col.Schema,
				Tables: map[string]Table{},
			}
		}

		table, ok := schema.Tables[strings.ToLower(col.Table)]
		if !ok {
			table = Table{
				Name: col.Table,
			}
		}

		table.Columns = append(table.Columns, col)

		schema.Tables[strings.ToLower(col.Table)] = table
		db.Schemas[strings.ToLower(col.Schema)] = schema
		ns.Databases[strings.ToLower(col.Database)] = db
	}

	return ns
}

type ColumnType struct {
	Name             string
	DatabaseTypeName string
	FetchedColumn    *iop.Column
	Length           int
	Precision        int
	Scale            int
	Nullable         bool
	CT               *sql.ColumnType
	Sourced          bool
}

func (ct *ColumnType) IsSourced() bool {
	if ct.Sourced {
		return true
	}

	if ct.CT != nil {
		_, ok1 := ct.CT.Length()
		_, _, ok2 := ct.CT.DecimalSize()
		if g.In(strings.ToLower(ct.DatabaseTypeName), "uuid", "uniqueidentifier") {
			return true
		}
		return lo.Ternary(ok1, ok1, ok2)
	}
	return ct.Sourced
}

func ParseTableName(text string, dialect dbio.Type) (table Table, err error) {
	table.Dialect = dialect
	table.Raw = text

	quote := GetQualifierQuote(dialect)

	textLower := strings.ToLower(text)
	if strings.Contains(textLower, "select") && strings.Contains(textLower, "from") && (strings.Contains(text, " ") || strings.Contains(text, "\n")) && !strings.Contains(text, quote) {
		table.SQL = strings.TrimSpace(text)
		return
	}

	inQuote := false
	words := []string{}
	word := ""

	addWord := func(caseAsIs bool) {
		if word == "" {
			return
		}

		var hasUpper, hasSpecial bool

		for _, r := range word {
			if unicode.IsUpper(r) {
				hasUpper = true
			}
		}

		defCaseAsIs := hasUpper || hasSpecial
		if caseAsIs || defCaseAsIs {
		} else {
			word = lo.Ternary(dialect.DBNameUpperCase(), strings.ToUpper(word), strings.ToLower(word))
		}
		words = append(words, word)
		word = ""
	}

	for _, r := range text {
		c := string(r)

		switch c {
		case quote:
			if inQuote {
				addWord(true)
			}
			inQuote = !inQuote
			continue
		case ".":
			if !inQuote {
				addWord(false)
				continue
			}
		case " ", "\n", "\t", "\r", "(", ")", "'":
			if !inQuote {
				table.SQL = strings.TrimSpace(text)
				return
			}
		}

		word = word + c
	}

	if inQuote {
		return table, g.Error("unterminated qualifier quote")
	} else if word != "" {
		addWord(false)
	}

	if len(words) == 0 {
		err = g.Error("invalid table name: %s", text)
		return
	} else if len(words) == 1 {
		table.Name = words[0]
	} else if len(words) == 2 {
		table.Schema = words[0]
		table.Name = words[1]
	} else if len(words) == 3 {
		table.Database = words[0]
		table.Schema = words[1]
		table.Name = words[2]
	} else {
		table.SQL = strings.TrimSpace(text)
	}

	return
}

func ParseColumnName(text string, dialect dbio.Type) (colName string, err error) {

	quote := GetQualifierQuote(dialect)

	inQuote := false
	words := []string{}
	word := ""

	addWord := func(caseAsIs bool) {
		if word == "" {
			return
		}

		var hasLower, hasUpper, hasSpecial bool

		for _, r := range word {
			if unicode.IsUpper(r) {
				hasUpper = true
			} else if unicode.IsLower(r) {
				hasLower = true
			}
		}

		defCaseAsIs := (hasLower && hasUpper) || hasSpecial
		if caseAsIs || defCaseAsIs {
		} else {
			word = lo.Ternary(dialect.DBNameUpperCase(), strings.ToUpper(word), strings.ToLower(word))
		}
		words = append(words, word)
		word = ""
	}

	for _, r := range text {
		c := string(r)

		switch c {
		case quote:
			if inQuote {
				addWord(true)
			}
			inQuote = !inQuote
			continue
		case ".":
			if !inQuote {
				addWord(false)
				continue
			}
		case " ", "\n", "\t", "\r", "(", ")", "'":
			if !inQuote {
				err = g.Error("invalid character: %#v", c)
				return
			}
		}

		word = word + c
	}

	if inQuote {
		return colName, g.Error("unterminated qualifier quote")
	} else if word != "" {
		addWord(false)
	}

	if len(words) == 0 {
		err = g.Error("invalid column name")
	} else {
		colName = words[len(words)-1]
	}

	return
}

// getColumnsProp returns the coercedCols from the columns property
func getColumnsProp(conn Connection) (coerceCols iop.Columns, ok bool) {
	if coerceColsV := conn.GetProp("columns"); coerceColsV != "" {
		if err := g.Unmarshal(coerceColsV, &coerceCols); err == nil {
			return coerceCols, true
		}
	}
	return coerceCols, false
}

func GetQualifierQuote(dialect dbio.Type) string {
	quote := `"`
	switch dialect {
	case dbio.TypeDbMySQL, dbio.TypeDbMariaDB, dbio.TypeDbStarRocks, dbio.TypeDbBigQuery, dbio.TypeDbClickhouse, dbio.TypeDbProton:
		quote = "`"
	case dbio.TypeDbBigTable, dbio.TypeDbMongoDB, dbio.TypeDbPrometheus:
		quote = ""
	}
	return quote
}

// GetTablesSchemata obtains the schemata for specified tables
func GetTablesSchemata(conn Connection, tableNames ...string) (schemata Schemata, err error) {
	schemata = Schemata{Databases: map[string]Database{}}

	schemaTableGroups := map[string][]Table{}
	for _, tableName := range tableNames {
		table, err := ParseTableName(tableName, conn.GetType())
		if err != nil {
			return schemata, g.Error(err, "could not parse table: %s", tableName)
		}
		if arr, ok := schemaTableGroups[table.Schema]; ok {
			schemaTableGroups[table.Schema] = append(arr, table)
		} else {
			schemaTableGroups[table.Schema] = []Table{table}
		}
	}

	getSchemata := func(schema string, tables []Table) {
		defer conn.Context().Wg.Read.Done()

		// pull down schemata
		names := lo.Map(tables, func(t Table, i int) string { return t.Name })
		newSchemata, err := conn.GetSchemata(SchemataLevelColumn, schema, names...)
		if err != nil {
			g.Warn("could not obtain schemata for schema: %s. %s", schema, err)
			return
		}

		// merge all schematas
		database := schemata.Database()
		schemas := database.Schemas
		if len(schemas) == 0 {
			schemas = map[string]Schema{}
		}

		for name, schema := range newSchemata.Database().Schemas {
			g.Debug(
				"   collected %d columns, in %d tables/views from schema %s",
				len(schema.Columns()),
				len(schema.Tables),
				schema.Name,
			)
			schemas[name] = schema
		}
		database.Schemas = schemas
		schemata.Databases[strings.ToLower(schemata.Database().Name)] = database
	}

	// loop an connect to each
	for schema, tables := range schemaTableGroups {
		conn.Context().Wg.Read.Add()
		go getSchemata(schema, tables)
	}

	conn.Context().Wg.Read.Wait()

	return schemata, nil
}

// GetSchemataAll obtains the schemata for all databases detected
func GetSchemataAll(conn Connection) (schemata Schemata, err error) {
	schemata = Schemata{Databases: map[string]Database{}}

	connInfo := conn.Info()

	// get all databases
	data, err := conn.GetDatabases()
	if err != nil {
		err = g.Error(err, "could not obtain list of databases")
		return
	}
	dbNames := data.ColValuesStr(0)

	getSchemata := func(dbName string) {
		defer conn.Context().Wg.Read.Done()

		// create new connection for database
		g.Debug("getting schemata for database: %s", dbName)

		// remove schema if specified
		connInfo.URL.PopParam("schema")

		// replace database with new one
		connURL := strings.ReplaceAll(
			connInfo.URL.String(),
			"/"+connInfo.Database,
			"/"+dbName,
		)

		newConn, err := NewConn(connURL)
		if err != nil {
			g.Warn("could not connect using database %s. %s", dbName, err)
			return
		}

		// pull down schemata
		newSchemata, err := newConn.GetSchemata("", "")
		if err != nil {
			g.Warn("could not obtain schemata for database: %s. %s", dbName, err)
			return
		}

		// merge all schematas
		for name, database := range newSchemata.Databases {
			g.Debug(
				"   collected %d columns, in %d tables/views from database %s",
				len(database.Columns()),
				len(database.Tables()),
				database.Name,
			)
			schemata.Databases[name] = database
		}
	}

	// loop an connect to each
	for _, dbName := range dbNames {
		conn.Context().Wg.Read.Add()
		go getSchemata(dbName)
	}

	conn.Context().Wg.Read.Wait()

	return schemata, nil
}

// AddPrimaryKeyToDDL adds a primary key to the table
func (t *Table) AddPrimaryKeyToDDL(ddl string, columns iop.Columns) (string, error) {

	if pkCols := columns.GetKeys(iop.PrimaryKey); len(pkCols) > 0 {
		ddl = strings.TrimSpace(ddl)

		// add pk right before the last parenthesis
		lastParen := strings.LastIndex(ddl, ")")
		if lastParen == -1 {
			return ddl, g.Error("could not find last parenthesis")
		}

		prefix := "primary key"
		switch t.Dialect {
		case dbio.TypeDbOracle:
			prefix = g.F("constraint %s_pkey primary key", strings.ToLower(t.Name))
		}

		quotedNames := t.Dialect.QuoteNames(pkCols.Names()...)
		ddl = ddl[:lastParen] + g.F(", %s (%s)", prefix, strings.Join(quotedNames, ", ")) + ddl[lastParen:]
	}

	return ddl, nil
}

type TableIndex struct {
	Name    string
	Columns iop.Columns
	Unique  bool
	Table   *Table
}

func (ti *TableIndex) CreateDDL() string {
	dialect := ti.Table.Dialect
	quotedNames := dialect.QuoteNames(ti.Columns.Names()...)

	if ti.Unique {
		return g.R(
			dialect.GetTemplateValue("core.create_unique_index"),
			"index", dialect.Quote(ti.Name),
			"table", ti.Table.FDQN(),
			"cols", strings.Join(quotedNames, ", "),
		)
	}

	return g.R(
		dialect.GetTemplateValue("core.create_index"),
		"index", dialect.Quote(ti.Name),
		"table", ti.Table.FDQN(),
		"cols", strings.Join(quotedNames, ", "),
	)
}

func (ti *TableIndex) DropDDL() string {
	dialect := ti.Table.Dialect

	return g.R(
		dialect.GetTemplateValue("core.drop_index"),
		"index", dialect.Quote(ti.Name),
		"name", ti.Name,
		"table", ti.Table.FDQN(),
		"schema", ti.Table.SchemaQ(),
	)
}

func (t *Table) Indexes(columns iop.Columns) (indexes []TableIndex) {

	// TODO: composite column indexes not yet supported
	// if indexSet := columns.GetKeys(iop.IndexKey); len(indexSet) > 0 {

	// 	// create index name from the first 6 chars of each column name
	// 	indexNameParts := []string{strings.ToLower(t.Name)}
	// 	for _, col := range indexSet.Names() {
	// 		if len(col) < 6 {
	// 			indexNameParts = append(indexNameParts, col)
	// 		} else {
	// 			indexNameParts = append(indexNameParts, col[:6])
	// 		}
	// 	}

	// 	index := TableIndex{
	// 		Name:    strings.Join(indexNameParts, "_"),
	// 		Columns: indexSet,
	// 		Unique:  false,
	// 		Table:   t,
	// 	}

	// 	indexes = append(indexes, index)
	// }

	// normal index
	for _, col := range columns.GetKeys(iop.IndexKey) {

		indexNameParts := []string{strings.ToLower(t.Name), strings.ToLower(col.Name)}
		index := TableIndex{
			Name:    strings.Join(indexNameParts, "_"),
			Columns: iop.Columns{col},
			Unique:  false,
			Table:   t,
		}

		indexes = append(indexes, index)
	}

	// unique index
	for _, col := range columns.GetKeys(iop.UniqueKey) {

		indexNameParts := []string{strings.ToLower(t.Name), strings.ToLower(col.Name)}
		index := TableIndex{
			Name:    strings.Join(indexNameParts, "_"),
			Columns: iop.Columns{col},
			Unique:  true,
			Table:   t,
		}

		indexes = append(indexes, index)
	}

	return
}

// getColumnTypes recovers from ColumnTypes panics
// this can happen in the Microsoft go-mssqldb driver
// See https://github.com/microsoft/go-mssqldb/issues/79
func getColumnTypes(result *sqlx.Rows) (dbColTypes []*sql.ColumnType, err error) {

	// recover from panic
	defer func() {
		if r := recover(); r != nil {
			err = g.Error(g.F("panic occurred! %#v\n%s", r, string(debug.Stack())))
		}
	}()

	return result.ColumnTypes()
}
