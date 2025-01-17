package database

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/labstack/echo/v4"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// DuckDbConn is a Duck DB connection
type DuckDbConn struct {
	BaseConn
	URL string

	duck *iop.DuckDb

	isInteractive bool
}

var DuckDbUseTempFile = false
var DuckDbMux = sync.Mutex{}
var DuckDbFileContext = map[string]*g.Context{} // so that collision doesn't happen
var DuckDbFileCmd = map[string]*exec.Cmd{}
var duckDbReadOnlyHint = "/* -readonly */"

// Init initiates the object
func (conn *DuckDbConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbDuckDb
	if strings.HasPrefix(conn.URL, "motherduck") || strings.HasPrefix(conn.URL, "duckdb://md:") {
		conn.BaseConn.Type = dbio.TypeDbMotherDuck
	}

	// init duckdb instance
	conn.duck = iop.NewDuckDb(conn.Context().Ctx, g.MapToKVArr(conn.properties)...)

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// GetURL returns the processed URL
func (conn *DuckDbConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}
	URL := strings.ReplaceAll(
		connURL,
		"duckdb://",
		"",
	)
	URL = strings.ReplaceAll(
		URL,
		"motherduck://",
		"md:",
	)
	return URL
}

// DuckDb returns the DuckDb instance
func (conn *DuckDbConn) DuckDb() *iop.DuckDb {
	return conn.duck
}

func (conn *DuckDbConn) dbPath() (string, error) {
	dbPathU, err := net.NewURL(conn.GetURL())
	if err != nil {
		err = g.Error(err, "could not get duckdb file path")
		return "", err
	}
	dbPath := strings.ReplaceAll(conn.GetURL(), "?"+dbPathU.U.RawQuery, "")
	return dbPath, nil
}

func (conn *DuckDbConn) Connect(timeOut ...int) (err error) {
	connURL := conn.GetURL()

	dbPath, err := conn.dbPath()
	if err != nil {
		return g.Error(err, "could not get db path")
	} else if conn.GetType() != dbio.TypeDbMotherDuck && !g.PathExists(dbPath) {
		g.Trace("The file %s does not exist, however it will be created if needed.", dbPath)
	}

	connPool.Mux.Lock()
	dbConn, poolOk := connPool.DuckDbs[connURL]
	connPool.Mux.Unlock()

	if poolOk {
		conn.duck = dbConn.duck
	}

	usePool = os.Getenv("USE_POOL") == "TRUE"
	if usePool && !poolOk {
		connPool.Mux.Lock()
		connPool.DuckDbs[connURL] = conn
		connPool.Mux.Unlock()
	}

	if cast.ToBool(conn.duck.GetProp("connected")) {
		return nil
	} else if err = conn.duck.Open(); err != nil {
		return err
	}

	g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))

	conn.SetProp("connected", "true")

	if conn.GetType() == dbio.TypeDbMotherDuck {
		_, err = conn.Exec("SET autoinstall_known_extensions=1; SET autoload_known_extensions=1;" + noDebugKey)
		if err != nil {
			return g.Error(err, "could not init extensions")
		}
	}

	return nil
}

// ExecContext runs a sql query with context, returns `error`
func (conn *DuckDbConn) ExecMultiContext(ctx context.Context, sqls ...string) (result sql.Result, err error) {
	return conn.duck.ExecMultiContext(ctx, sqls...)
}

func (conn *DuckDbConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	return conn.duck.ExecContext(ctx, sql, args...)
}

func (conn *DuckDbConn) Close() (err error) {
	if conn.duck != nil {
		err = conn.duck.Close()
		if err == nil && !cast.ToBool(conn.GetProp("silent")) {
			g.Debug(`closed "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
		}
	}
	conn.SetProp("connected", "false")
	return err
}

func (conn *DuckDbConn) StreamRowsContext(ctx context.Context, sql string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {
	return conn.duck.StreamContext(ctx, sql, options...)
}

// InsertBatchStream inserts a stream into a table in batch
func (conn *DuckDbConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	df, err := iop.MakeDataFlow(ds)
	if err != nil {
		return
	}
	return conn.BulkImportFlow(tableFName, df)
}

// InsertStream demonstrates loading data into a BigQuery table using a file on the local filesystem.
func (conn *DuckDbConn) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	df, err := iop.MakeDataFlow(ds)
	if err != nil {
		return
	}
	return conn.BulkImportFlow(tableFName, df)
}

func (conn *DuckDbConn) importViaTempCSVs(tableFName string, df *iop.Dataflow) (count uint64, err error) {

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not get table name for import")
		return
	}

	folderPath := path.Join(env.GetTempFolder(), "duckdb", "import", env.CleanTableName(tableFName), g.NowFileStr())
	fileReadyChn := make(chan filesys.FileReady, 3)

	go func() {
		fs, err := filesys.NewFileSysClient(dbio.TypeFileLocal, conn.PropArrExclude("url")...)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Could not get fs client for Local"))
			return
		}

		_, err = fs.WriteDataflowReady(df, folderPath, fileReadyChn, conn.defaultCsvConfig())
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error writing dataflow to disk: "+folderPath))
			return
		}
	}()

	doInsert := func(file filesys.FileReady) (err error) {
		columnNames := lo.Map(file.Columns.Names(), func(col string, i int) string {
			return `"` + col + `"`
		})

		sqlLines := []string{
			g.F(`insert into %s (%s) select * from read_csv('%s', delim=',', header=True, columns=%s, max_line_size=134217728, parallel=false, quote='"', escape='"', nullstr='\N');`, table.FDQN(), strings.Join(columnNames, ", "), file.Node.Path(), conn.generateCsvColumns(file.Columns)),
		}

		sql := strings.Join(sqlLines, ";\n")

		result, err := conn.duck.ExecContext(conn.Context().Ctx, sql)
		if err != nil {
			return g.Error(err, "could not insert into %s", tableFName)
		}

		if result != nil {
			inserted, _ := result.RowsAffected()
			g.Debug("inserted %d rows", inserted)
		}

		return nil
	}

	for file := range fileReadyChn {
		// sequential inserting, no wait group
		err = doInsert(file)

		// delete file
		env.RemoveLocalTempFile(file.Node.Path())

		if err != nil {
			return 0, err
		}

	}

	return df.Count(), nil
}

func (conn *DuckDbConn) importViaHTTP(tableFName string, df *iop.Dataflow) (count uint64, err error) {

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not get table name for import")
		return
	}

	// start local http server
	port, err := g.GetPort("localhost:0")
	if err != nil {
		err = g.Error(err, "could not acquire local port for duckdb http import")
		return 0, err
	}
	// create reader channel to pass between handler and main flow
	readerCh := make(chan io.Reader, 1)

	// create http server to serve data
	importContext := g.NewContext(conn.context.Ctx)
	httpURL := g.F("http://localhost:%d/data", port)
	server := echo.New()
	{
		server.HidePort = true
		server.HideBanner = true
		// server.Use(middleware.Logger())
		server.Add(http.MethodGet, "/data", func(c echo.Context) (err error) {
			select {
			case reader := <-readerCh:
				if reader != nil {
					return c.Stream(200, "text/csv", reader)
				}
			default:
			}
			return c.NoContent(http.StatusOK)
		})

		server.Add(http.MethodHead, "/data", func(c echo.Context) error {
			c.Response().Header().Set("Content-Type", "text/csv")
			return c.NoContent(http.StatusOK)
		})

		// start server in background, wait for it to start
		importContext.Wg.Read.Add()
		go func() {
			g.Debug("started %s for duckdb direct import", httpURL)
			importContext.Wg.Read.Done()
			if err := server.Start(g.F("localhost:%d", port)); err != http.ErrServerClosed {
				g.Error(err, "duckdb import http server error")
			}
		}()
	}

	// wait for local server startup
	importContext.Wg.Read.Wait()
	time.Sleep(100 * time.Millisecond)
	defer server.Shutdown(conn.Context().Ctx)

	sc := conn.defaultCsvConfig()
	df.SetBatchLimit(sc.BatchLimit)
	ds := iop.MergeDataflow(df)

	for batchR := range ds.NewCsvReaderChnl(sc) {
		g.Trace("processing duckdb batch %s", batchR.Batch.ID())
		readerCh <- batchR.Reader

		columnNames := lo.Map(batchR.Columns.Names(), func(col string, i int) string {
			return `"` + col + `"`
		})

		sqlLines := []string{
			g.F(`insert into %s (%s) select * from read_csv('%s', delim=',', header=True, columns=%s, max_line_size=134217728, parallel=false, quote='"', escape='"', nullstr='\N');`, table.FDQN(), strings.Join(columnNames, ", "), httpURL, conn.generateCsvColumns(batchR.Columns)),
		}

		sql := strings.Join(sqlLines, ";\n")

		result, err := conn.duck.ExecContext(conn.Context().Ctx, sql)
		if err != nil {
			return df.Count(), g.Error(err, "could not insert into %s", tableFName)
		}

		if err = importContext.Err(); err != nil {
			return df.Count(), g.Error(err, "error importing insert into %s", tableFName)
		}

		if result != nil {
			inserted, _ := result.RowsAffected()
			g.Trace("inserted %d rows into temp duckdb", inserted)
		}
	}

	return df.Count(), nil
}

func (conn *DuckDbConn) defaultCsvConfig() (config iop.StreamConfig) {
	return conn.duck.DefaultCsvConfig()
}

func (conn *DuckDbConn) generateCsvColumns(columns iop.Columns) (colStr string) {
	return conn.duck.GenerateCsvColumns(columns)
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *DuckDbConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	// _, indexTable := SplitTableFullName(tgtTable)

	// indexSQL := g.R(
	// 	conn.GetTemplateValue("core.create_unique_index"),
	// 	"index", strings.Join(pkFields, "_")+"_idx",
	// 	"table", indexTable,
	// 	"cols", strings.Join(pkFields, ", "),
	// )

	// _, err = conn.Exec(indexSQL)
	// if err != nil {
	// 	err = g.Error(err, "could not create unique index")
	// 	return
	// }

	// V0.7
	// sqlTempl := `
	// insert into {tgt_table} as tgt
	// 	({insert_fields})
	// select {src_fields}
	// from {src_table} as src
	// where true
	// ON CONFLICT ({pk_fields})
	// DO UPDATE
	// SET {set_fields}
	// `

	sqlTempl := `
	delete from {tgt_table} tgt
	using {src_table} src
	where {src_tgt_pk_equal}
	;

	insert into {tgt_table}
		({insert_fields})
	select {src_fields}
	from {src_table} src
	`

	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"src_upd_pk_equal", strings.ReplaceAll(upsertMap["src_tgt_pk_equal"], "tgt.", "upd."),
		"src_fields", upsertMap["src_fields"],
		"pk_fields", upsertMap["pk_fields"],
		"set_fields", strings.ReplaceAll(upsertMap["set_fields"], "src.", "excluded."),
		"insert_fields", upsertMap["insert_fields"],
	)

	return
}

// CastColumnForSelect casts to the correct target column type
func (conn *DuckDbConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) (selectStr string) {
	qName := conn.Self().Quote(srcCol.Name)

	switch {
	case srcCol.Type != iop.TimestampzType && tgtCol.Type == iop.TimestampzType:
		selectStr = g.F("%s::%s", qName, tgtCol.DbType)
	case srcCol.Type == iop.TimestampzType && tgtCol.Type != iop.TimestampzType:
		selectStr = g.F("%s::%s", qName, tgtCol.DbType)
	default:
		selectStr = qName
	}

	return selectStr
}
