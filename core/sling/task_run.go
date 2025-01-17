package sling

import (
	"context"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	_ "net/http/pprof"

	"github.com/nqd/flat"
	"github.com/slingdata-io/sling-cli/core"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// connPool a way to cache connections to that they don't have to reconnect
// for each replication steps
var connPool = map[string]database.Connection{}

var (
	start                time.Time
	slingLoadedAtColumn  = "_sling_loaded_at"
	slingDeletedAtColumn = "_sling_deleted_at"
	slingStreamURLColumn = "_sling_stream_url"
	slingRowNumColumn    = "_sling_row_num"
	slingRowIDColumn     = "_sling_row_id"
	slingExecIDColumn    = "_sling_exec_id"
)

var deleteMissing func(*TaskExecution, database.Connection, database.Connection) error = func(_ *TaskExecution, _, _ database.Connection) error {
	g.Warn("use the official release of sling-cli to use delete_missing")
	return nil
}

func init() {
	// we need a webserver to get the pprof webserver
	if cast.ToBool(os.Getenv("SLING_PPROF")) {
		go func() {
			g.Debug("Starting pprof webserver @ localhost:6060")
			g.LogError(http.ListenAndServe("localhost:6060", nil))
		}()
	}
}

// Execute runs a Sling task.
// This may be a file/db to file/db transfer
func (t *TaskExecution) Execute() error {

	done := make(chan struct{})
	now := time.Now()
	t.StartTime = &now
	t.lastIncrement = now

	if t.Context == nil {
		t.Context = g.NewContext(context.Background())
	}

	// get stats of process at beginning
	t.ProcStatsStart = g.GetProcStats(os.Getpid())

	// set defaults
	t.Config.SetDefault()

	// print for debugging
	g.Trace("using Config:\n%s", g.Pretty(t.Config))
	env.SetTelVal("stage", "2 - task-execution")

	if StoreSet != nil {
		ticker5s := time.NewTicker(5 * time.Second)
		go func() {
			defer ticker5s.Stop()
			for range ticker5s.C {
				if t.Status != ExecStatusRunning {
					return // is done
				}
				select {
				case <-t.Context.Ctx.Done():
					return
				case <-ticker5s.C:
					StateSet(t)
				}
			}
		}()
	}

	go func() {
		defer close(done)
		defer t.PBar.Finish()

		// recover from panic
		defer func() {
			if r := recover(); r != nil {
				t.Err = g.Error("panic occurred! %#v\n%s", r, string(debug.Stack()))
			}
		}()

		t.Status = ExecStatusRunning

		if t.Err != nil {
			return
		}

		// update into store
		StateSet(t)

		g.DebugLow("Sling version: %s (%s %s)", core.Version, runtime.GOOS, runtime.GOARCH)
		g.DebugLow("type is %s", t.Type)
		g.Debug("using: %s", g.Marshal(g.M("mode", t.Config.Mode, "columns", t.Config.Target.Columns, "transforms", t.Config.Transforms, "select", t.Config.Source.Select)))
		g.Debug("using source options: %s", g.Marshal(t.Config.Source.Options))
		g.Debug("using target options: %s", g.Marshal(t.Config.Target.Options))

		// pre-hooks
		if t.Err = t.ExecuteHooks(HookStagePre); t.Err != nil {
			return
		} else if t.skipStream {
			t.SetProgress("skipping stream")
			t.Status = ExecStatusSkipped
			return
		}

		switch t.Type {
		case DbSQL:
			t.Err = t.runDbSQL()
		case FileToDB:
			t.Err = t.runFileToDB()
		case DbToDb:
			t.Err = t.runDbToDb()
		case DbToFile:
			t.Err = t.runDbToFile()
		case FileToFile:
			t.Err = t.runFileToFile()
		default:
			t.SetProgress("task execution configuration is invalid")
			t.Err = g.Error("Cannot Execute. Task Type is not specified")
		}

		// warn constrains
		if df := t.Df(); df != nil {
			for _, col := range df.Columns {
				if c := col.Constraint; c != nil && c.FailCnt > 0 {
					g.Warn("column '%s' had %d constraint failures (%s) ", col.Name, c.FailCnt, c.Expression)
					t.Status = ExecStatusWarning // set as warning status
				}
			}
		}
	}()

	select {
	case <-done:
		t.Cleanup()
	case <-t.Context.Ctx.Done():
		go t.Cleanup()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
		if t.Err == nil {
			t.Err = g.Error("Execution interrupted")
		}
	}

	if t.Err == nil {
		if t.Status == ExecStatusWarning {
			t.SetProgress("execution succeeded (with warnings)")
		} else {
			t.SetProgress("execution succeeded")
			t.Status = ExecStatusSuccess
		}
	} else {
		t.SetProgress("execution failed")
		t.Status = ExecStatusError
		if err := t.df.Context.Err(); err != nil && err.Error() != t.Err.Error() {
			eG := g.ErrorGroup{}
			eG.Add(err)
			eG.Add(t.Err)
			t.Err = g.Error(eG.Err())
		} else {
			t.Err = g.Error(t.Err)
		}
	}

	now2 := time.Now()
	t.EndTime = &now2

	// update into store
	StateSet(t)

	// post-hooks
	if hookErr := t.ExecuteHooks(HookStagePost); hookErr != nil {
		if t.Err == nil {
			t.Err = hookErr
		}
	}

	return t.Err
}

func (t *TaskExecution) ExecuteHooks(stage HookStage) (err error) {
	if t.Config == nil || t.Config.ReplicationStream == nil {
		return nil
	}

	hooks, err := t.Replication.ParseStreamHook(stage, t.Config.ReplicationStream)
	if err != nil {
		return g.Error(err, "could not parse hooks")
	} else if len(hooks) > 0 {
		if err = hooks.Execute(); err != nil {
			return g.Error(err, "error executing %s hooks", stage)
		}
	}

	return nil
}

func (t *TaskExecution) GetStateMap() map[string]any {
	// format map
	fMap, _ := t.Config.GetFormatMap()

	// merge with context map
	cMap := t.Context.Map.Items()
	for k, v := range fMap {
		cMap[k] = v
	}

	// flatten with dot separator
	sMap, err := flat.Flatten(cMap, &flat.Options{Delimiter: ".", Safe: true})
	if err != nil {
		g.LogError(err, "error flattening state map")
	}

	return sMap
}

func (t *TaskExecution) getSrcDBConn(ctx context.Context) (conn database.Connection, err error) {

	// sets metadata
	metadata := t.setGetMetadata()

	options := t.getOptionsMap()
	options["METADATA"] = g.Marshal(metadata)

	// merge options
	for k, v := range options {
		t.Config.SrcConn.Data[k] = v
	}

	conn, err = t.Config.SrcConn.AsDatabaseContext(ctx, t.isUsingPool())
	if err != nil {
		err = g.Error(err, "Could not initialize source connection")
		return
	}

	err = conn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to source connection")
		return
	}

	// set read_only if sqlite / duckdb since it's a source
	if g.In(conn.GetType(), dbio.TypeDbSQLite, dbio.TypeDbD1, dbio.TypeDbDuckDb, dbio.TypeDbMotherDuck) {
		conn.SetProp("read_only", "true")
	}

	return
}

func (t *TaskExecution) getTgtDBConn(ctx context.Context) (conn database.Connection, err error) {

	options := g.M()
	g.Unmarshal(g.Marshal(t.Config.Target.Options), &options)

	// merge options
	for k, v := range options {
		t.Config.TgtConn.Data[k] = v
	}

	conn, err = t.Config.TgtConn.AsDatabaseContext(ctx, t.isUsingPool())
	if err != nil {
		err = g.Error(err, "Could not initialize target connection")
		return
	}

	err = conn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to target connection")
		return
	}

	// set bulk
	if val := t.Config.Target.Options.UseBulk; val != nil && !*val {
		conn.SetProp("use_bulk", "false")
		conn.SetProp("allow_bulk_import", "false")
	}
	return
}

func (t *TaskExecution) runDbSQL() (err error) {

	start = time.Now()

	t.SetProgress("connecting to target database (%s)", t.Config.TgtConn.Type)
	tgtConn, err := t.getTgtDBConn(t.Context.Ctx)
	if err != nil {
		err = g.Error(err, "Could not initialize target connection")
		return
	}

	if !t.isUsingPool() {
		defer tgtConn.Close()
	}

	t.SetProgress("executing sql on target database")
	result, err := tgtConn.Exec(t.Config.Target.Object)
	if err != nil {
		err = g.Error(err, "Could not complete sql execution on %s (%s)", t.Config.TgtConn.Info().Name, tgtConn.GetType())
		return
	}

	rowAffCnt, err := result.RowsAffected()
	if err == nil {
		t.SetProgress("%d rows affected", rowAffCnt)
	}

	return
}

func (t *TaskExecution) runDbToFile() (err error) {

	start = time.Now()

	t.SetProgress("connecting to source database (%s)", t.Config.SrcConn.Type)
	srcConn, err := t.getSrcDBConn(t.Context.Ctx)
	if err != nil {
		err = g.Error(err, "Could not initialize source connection")
		return
	}

	if t.isIncrementalStateWithUpdateKey() {
		if err = getIncrementalValueViaState(t); err != nil {
			err = g.Error(err, "Could not get incremental value")
			return err
		}
		t.Context.Map.Set("incremental_value", t.Config.IncrementalVal)
	} else if t.isIncrementalWithUpdateKey() {
		return g.Error("Please use the SLING_STATE environment variable for writing to files incrementally")
	}

	if !t.isUsingPool() {
		defer srcConn.Close()
	}

	t.SetProgress("reading from source database")
	defer t.Cleanup()
	t.df, err = t.ReadFromDB(t.Config, srcConn)
	if err != nil {
		err = g.Error(err, "Could not ReadFromDB")
		return
	}
	defer t.df.Close()

	if t.Config.Options.StdOut {
		t.SetProgress("writing to target stream (stdout)")
	} else {
		t.SetProgress("writing to target file system (%s)", t.Config.TgtConn.Type)
	}
	cnt, err := t.WriteToFile(t.Config, t.df)
	if err != nil {
		err = g.Error(err, "Could not WriteToFile")
		return
	}

	t.SetProgress("wrote %d rows [%s r/s] to %s", cnt, getRate(cnt), t.getTargetObjectValue())

	if err = t.df.Err(); err != nil {
		err = g.Error(err, "Error running runDbToFile")
	}

	if cnt > 0 && t.hasStateWithUpdateKey() {
		if err = setIncrementalValueViaState(t); err != nil {
			err = g.Error(err, "Could not set incremental value")
			return err
		}
	}

	return

}

func (t *TaskExecution) runFileToDB() (err error) {

	start = time.Now()

	t.SetProgress("connecting to target database (%s)", t.Config.TgtConn.Type)
	tgtConn, err := t.getTgtDBConn(t.Context.Ctx)
	if err != nil {
		err = g.Error(err, "Could not initialize target connection")
		return
	}

	if !t.isUsingPool() {
		t.AddCleanupTaskLast(func() { tgtConn.Close() })
	}

	// check if table exists by getting target columns
	// only pull if ignore_existing is specified (don't need columns yet otherwise)
	if t.Config.IgnoreExisting() {
		if cols, _ := pullTargetTableColumns(t.Config, tgtConn, false); len(cols) > 0 {
			g.Debug("not writing since table exists at %s (ignore_existing=true)", t.Config.Target.Object)
			return nil
		}
	}

	if t.isIncrementalWithUpdateKey() {
		t.SetProgress("getting checkpoint value")
		if t.Config.Source.UpdateKey == "." {
			t.Config.Source.UpdateKey = slingLoadedAtColumn
		}

		if err = getIncrementalValueViaDB(t.Config, tgtConn, dbio.TypeDbDuckDb); err != nil {
			err = g.Error(err, "Could not get incremental value")
			return err
		}
		t.Context.Map.Set("incremental_value", t.Config.IncrementalVal)
	}

	if t.Config.Options.StdIn && t.Config.SrcConn.Type.IsUnknown() {
		t.SetProgress("reading from stream (stdin)")
	} else {
		t.SetProgress("reading from source file system (%s)", t.Config.SrcConn.Type)
	}
	t.df, err = t.ReadFromFile(t.Config)
	if err != nil {
		if strings.Contains(err.Error(), "Provided 0 files") {
			if t.isIncrementalWithUpdateKey() && t.Config.HasIncrementalVal() {
				t.SetProgress("no new files found since latest timestamp (%s)", time.Unix(cast.ToInt64(t.Config.IncrementalVal), 0))
			} else {
				t.SetProgress("no files found")
			}
			return nil
		} else if len(t.df.Streams) == 1 && t.df.Streams[0].IsClosed() {
			return nil
		}
		err = g.Error(err, "could not read from file")
		return
	}
	defer t.df.Close()

	t.SetProgress("writing to target database [mode: %s]", t.Config.Mode)
	defer t.Cleanup()
	cnt, err := t.WriteToDb(t.Config, t.df, tgtConn)
	if err != nil {
		err = g.Error(err, "could not write to database")
		return
	}

	elapsed := int(time.Since(start).Seconds())
	t.SetProgress("inserted %d rows into %s in %d secs [%s r/s]", cnt, t.getTargetObjectValue(), elapsed, getRate(cnt))

	if err != nil {
		err = g.Error(t.df.Err(), "error in transfer")
	}
	return
}

func (t *TaskExecution) runFileToFile() (err error) {

	start = time.Now()

	if t.Config.Options.StdIn && t.Config.SrcConn.Type.IsUnknown() {
		t.SetProgress("reading from stream (stdin)")
	} else {
		t.SetProgress("reading from source file system (%s)", t.Config.SrcConn.Type)
	}
	t.df, err = t.ReadFromFile(t.Config)
	if err != nil {
		if strings.Contains(err.Error(), "Provided 0 files") {
			if t.isIncrementalWithUpdateKey() && t.Config.HasIncrementalVal() {
				t.SetProgress("no new files found since latest timestamp (%s)", time.Unix(cast.ToInt64(t.Config.IncrementalVal), 0))
			} else {
				t.SetProgress("no files found")
			}
			return nil
		} else if len(t.df.Streams) == 1 && t.df.Streams[0].IsClosed() {
			return nil
		}
		err = g.Error(err, "Could not ReadFromFile")
		return
	}
	defer t.df.Close()

	if t.Config.Options.StdOut {
		t.SetProgress("writing to target stream (stdout)")
	} else {
		t.SetProgress("writing to target file system (%s)", t.Config.TgtConn.Type)
	}
	defer t.Cleanup()
	cnt, err := t.WriteToFile(t.Config, t.df)
	if err != nil {
		err = g.Error(err, "Could not WriteToFile")
		return
	}

	elapsed := int(time.Since(start).Seconds())
	t.SetProgress("wrote %d rows to %s in %d secs [%s r/s]", cnt, t.getTargetObjectValue(), elapsed, getRate(cnt))

	if t.df.Err() != nil {
		err = g.Error(t.df.Err(), "Error in runFileToFile")
	}
	return
}

func (t *TaskExecution) runDbToDb() (err error) {
	start = time.Now()
	if t.Config.Mode == Mode("") {
		t.Config.Mode = FullRefreshMode
	}

	// Initiate connections
	t.SetProgress("connecting to source database (%s)", t.Config.SrcConn.Type)
	srcConn, err := t.getSrcDBConn(t.Context.Ctx)
	if err != nil {
		err = g.Error(err, "Could not initialize source connection")
		return
	}

	t.SetProgress("connecting to target database (%s)", t.Config.TgtConn.Type)
	tgtConn, err := t.getTgtDBConn(t.Context.Ctx)
	if err != nil {
		err = g.Error(err, "Could not initialize target connection")
		return
	}

	if !t.isUsingPool() {
		t.AddCleanupTaskLast(func() { srcConn.Close() })
		t.AddCleanupTaskLast(func() { tgtConn.Close() })
	}

	// check if table exists by getting target columns
	if cols, _ := pullTargetTableColumns(t.Config, tgtConn, false); len(cols) > 0 {
		if t.Config.IgnoreExisting() {
			g.Debug("not writing since table exists at %s (ignore_existing=true)", t.Config.Target.Object)
			return nil
		}
	}

	// get watermark
	if t.isIncrementalStateWithUpdateKey() {
		if err = getIncrementalValueViaState(t); err != nil {
			err = g.Error(err, "Could not get incremental value")
			return err
		}
		t.Context.Map.Set("incremental_value", t.Config.IncrementalVal)
	} else if t.isIncrementalWithUpdateKey() {
		t.SetProgress("getting checkpoint value")
		if err = getIncrementalValueViaDB(t.Config, tgtConn, srcConn.GetType()); err != nil {
			err = g.Error(err, "Could not get incremental value")
			return err
		}
		t.Context.Map.Set("incremental_value", t.Config.IncrementalVal)
	}

	t.SetProgress("reading from source database")
	t.df, err = t.ReadFromDB(t.Config, srcConn)
	if err != nil {
		err = g.Error(err, "Could not ReadFromDB")
		return
	}
	defer t.df.Close()

	// to DirectLoad if possible
	if t.df.FsURL != "" {
		data := g.M("url", t.df.FsURL)
		for k, v := range srcConn.Props() {
			data[k] = v
		}
		t.Config.Source.Data["SOURCE_FILE"] = g.M("data", data)
	}

	t.SetProgress("writing to target database [mode: %s]", t.Config.Mode)
	defer t.Cleanup()
	cnt, err := t.WriteToDb(t.Config, t.df, tgtConn)
	if err != nil {
		err = g.Error(err, "Could not WriteToDb")
		return
	}

	bytesStr := ""
	if val := t.GetBytesString(); val != "" {
		bytesStr = "[" + val + "]"
	}
	elapsed := int(time.Since(start).Seconds())
	t.SetProgress("inserted %d rows into %s in %d secs [%s r/s] %s", cnt, t.getTargetObjectValue(), elapsed, getRate(cnt), bytesStr)

	if t.df.Err() != nil {
		err = g.Error(t.df.Err(), "Error running runDbToDb")
	}

	if cnt > 0 && t.hasStateWithUpdateKey() {
		if err = setIncrementalValueViaState(t); err != nil {
			err = g.Error(err, "Could not set incremental value")
			return err
		}
	}

	// if delete missing is specified with incremental mode
	if t.Config.Target.Options.DeleteMissing != nil {
		if g.In(t.Config.Mode, IncrementalMode) && len(t.Config.Source.PrimaryKey()) > 0 {
			if err = deleteMissing(t, srcConn, tgtConn); err != nil {
				err = g.Error(err, "could not delete missing records")
			}
		} else {
			g.Warn("must set mode to incremental with a primary-key to use `delete_missing`")
		}
	}

	return
}
