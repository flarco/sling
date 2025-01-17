package sling

import (
	"math"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/segmentio/ksuid"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// Set in the store/store.go file for history keeping
var StoreSet = func(t *TaskExecution) error { return nil }

// TaskExecution is a sling ELT task run, synonymous to an execution
type TaskExecution struct {
	ExecID    string     `json:"exec_id"`
	Config    *Config    `json:"config"`
	Type      JobType    `json:"type"`
	Status    ExecStatus `json:"status"`
	Err       error      `json:"error"`
	StartTime *time.Time `json:"start_time"`
	EndTime   *time.Time `json:"end_time"`
	Bytes     uint64     `json:"bytes"`
	Context   *g.Context `json:"-"`
	Progress  string     `json:"progress"`

	df            *iop.Dataflow `json:"-"`
	data          *iop.Dataset  `json:"-"`
	prevRowCount  uint64
	prevByteCount uint64
	skipStream    bool            `json:"skip_stream"`
	lastIncrement time.Time       // the time of last row increment (to determine stalling)
	Output        strings.Builder `json:"-"`
	OutputLines   chan *g.LogLine

	Replication    *ReplicationConfig `json:"replication"`
	ProgressHist   []string           `json:"progress_hist"`
	PBar           *ProgressBar       `json:"-"`
	ProcStatsStart g.ProcStats        `json:"-"` // process stats at beginning
	cleanupFuncs   []func()
}

// ExecutionStatus is an execution status object
type ExecutionStatus struct {
	JobID       int        `json:"job_id,omitempty"`
	ExecID      int64      `json:"exec_id,omitempty"`
	Status      ExecStatus `json:"status,omitempty"`
	Text        string     `json:"text,omitempty"`
	Rows        uint64     `json:"rows,omitempty"`
	Bytes       uint64     `json:"bytes,omitempty"`
	Percent     int        `json:"percent,omitempty"`
	Stalled     bool       `json:"stalled,omitempty"`
	Duration    *int       `json:"duration,omitempty"`
	AvgDuration int        `json:"avg_duration,omitempty"`
}

func NewExecID() string {
	uid, err := ksuid.NewRandom()
	execID := g.NewTsID("exec")
	if err == nil {
		execID = uid.String()
	}

	return execID
}

// NewTask creates a Sling task with given configuration
func NewTask(execID string, cfg *Config) (t *TaskExecution) {
	if execID == "" {
		execID = NewExecID()
	}

	t = &TaskExecution{
		ExecID:       execID,
		Config:       cfg,
		Status:       ExecStatusCreated,
		df:           iop.NewDataflow(),
		PBar:         NewPBar(time.Second),
		ProgressHist: []string{},
		cleanupFuncs: []func(){},
		OutputLines:  make(chan *g.LogLine, 5000),
	}

	if args := os.Getenv("SLING_CLI_ARGS"); args != "" {
		t.AppendOutput(&g.LogLine{Level: 9, Text: " -- args: " + args})
	}

	err := cfg.Prepare()
	if err != nil {
		t.Err = g.Error(err, "could not prepare task")
		return
	}

	t.Type, err = cfg.DetermineType()
	if err != nil {
		t.Err = g.Error(err, "could not determine type")
		return
	}

	if ShowProgress {
		// progress bar ticker
		t.PBar = NewPBar(time.Second)
		ticker1s := time.NewTicker(1 * time.Second)
		go func() {
			defer ticker1s.Stop()

			for {
				select {
				case <-ticker1s.C:
					cnt := t.df.Count()
					if cnt > 1000 {
						t.PBar.Start()
						t.PBar.bar.SetCurrent(cast.ToInt64(cnt))
						t.PBar.bar.Set("bytes", t.GetBytesString())
						rowRate, byteRate := t.GetRate(1)
						t.PBar.bar.Set("rowRate", g.F("%s r/s", humanize.Comma(rowRate)))
						t.PBar.bar.Set("byteRate", g.F("%s/s", humanize.Bytes(cast.ToUint64(byteRate))))
					}

				default:
					time.Sleep(100 * time.Millisecond)
					if t.PBar.finished || t.df.Err() != nil {
						t.PBar.bar.SetCurrent(cast.ToInt64(t.df.Count()))
						t.PBar.Finish()
						return
					}
				}
			}
		}()
	}

	return
}

// SetProgress sets the progress
func (t *TaskExecution) SetProgress(progressText string, args ...interface{}) {
	progressText = g.F(progressText, args...)
	t.ProgressHist = append(t.ProgressHist, progressText)
	t.Progress = progressText
	if !t.PBar.started || t.PBar.finished {
		if strings.HasSuffix(progressText, "failed") {
			progressText = env.RedString(progressText)
		}
		g.Info(progressText)
	} else {
		t.PBar.SetStatus(progressText)
	}
}

func (t *TaskExecution) GetSourceTable() (sTable database.Table, err error) {
	sTable, err = database.ParseTableName(t.Config.Source.Stream, t.Config.SrcConn.Type)
	if err != nil {
		err = g.Error(err, "Could not parse source stream text")
	} else if !sTable.IsQuery() && sTable.Schema == "" {
		sTable.Schema = cast.ToString(t.Config.Source.Data["schema"])
	}
	return
}

func (t *TaskExecution) GetTargetTable(tempTableSuffix ...string) (tTable database.Table, err error) {
	tTable, err = database.ParseTableName(t.Config.Target.Object, t.Config.TgtConn.Type)
	if err != nil {
		err = g.Error(err, "Could not parse target object")
	} else if tTable.Schema == "" {
		tTable.Schema = cast.ToString(t.Config.Target.Data["schema"])
	}

	// add suffix to table name (for temp table)
	if len(tempTableSuffix) > 0 {
		tempTableSuffix[0] = strings.ToLower(tempTableSuffix[0])
		if t.Config.TgtConn.Type.DBNameUpperCase() {
			tempTableSuffix[0] = strings.ToUpper(tempTableSuffix[0])
		}
		tTable.Name = tTable.Name + tempTableSuffix[0]
	}

	return
}

// GetTotalBytes gets the inbound/oubound bytes of the task
func (t *TaskExecution) GetTotalBytes() (rcBytes, txBytes uint64) {
	procStatsEnd := g.GetProcStats(os.Getpid())

	switch {
	case g.In(t.Config.SrcConn.Type, dbio.TypeDbPostgres, dbio.TypeDbOracle, dbio.TypeDbMySQL, dbio.TypeDbStarRocks, dbio.TypeDbMariaDB):
		rcBytes = procStatsEnd.RcBytes - t.ProcStatsStart.RcBytes
	case g.In(t.Config.SrcConn.Type, dbio.TypeDbSnowflake, dbio.TypeDbBigQuery, dbio.TypeDbRedshift):
		rcBytes = procStatsEnd.RcBytes - t.ProcStatsStart.RcBytes
	case g.In(t.Config.SrcConn.Type, dbio.TypeFileLocal):
		rcBytes = procStatsEnd.ReadBytes - t.ProcStatsStart.ReadBytes
	default:
	}

	switch {
	case g.In(t.Config.TgtConn.Type, dbio.TypeDbPostgres, dbio.TypeDbOracle, dbio.TypeDbMySQL):
		txBytes = procStatsEnd.TxBytes - t.ProcStatsStart.TxBytes
	case g.In(t.Config.TgtConn.Type, dbio.TypeDbSnowflake, dbio.TypeDbBigQuery, dbio.TypeDbRedshift):
		txBytes = procStatsEnd.TxBytes - t.ProcStatsStart.TxBytes
	case g.In(t.Config.TgtConn.Type, dbio.TypeFileLocal):
		txBytes = procStatsEnd.WriteBytes - t.ProcStatsStart.WriteBytes
	default:
	}

	switch {
	case t.Type == DbToDb:
	case t.Type == DbToFile:
	case t.Type == FileToDB:
	case t.Type == FileToFile:
	}
	return
}

// IsStalled determines if the task has stalled (no row increment)
func (t *TaskExecution) IsStalled(window float64) bool {
	if strings.Contains(t.Progress, "pre-sql") || strings.Contains(t.Progress, "post-sql") {
		return false
	}
	return time.Since(t.lastIncrement).Seconds() > window
}

// GetBytes return the current total of bytes processed
func (t *TaskExecution) GetBytes() (inBytes, outBytes uint64) {
	if t.df == nil {
		return
	}

	inBytes, outBytes = t.df.Bytes()
	return
}

func (t *TaskExecution) AppendOutput(ll *g.LogLine) {
	t.Output.WriteString(ll.Line() + "\n") // add new-line char

	// push line if not full
	select {
	case t.OutputLines <- ll:
	default:
	}
}

func (t *TaskExecution) GetBytesString() (s string) {
	inBytes, _ := t.GetBytes()
	if inBytes == 0 {
		return ""
	}
	return g.F("%s", humanize.Bytes(inBytes))
	// if inBytes > 0 && inBytes == outBytes {
	// 	return g.F("%s", humanize.Bytes(inBytes))
	// }
	// return g.F("%s -> %s", humanize.Bytes(inBytes), humanize.Bytes(outBytes))
}

// GetCount return the current count of rows processed
func (t *TaskExecution) GetCount() (count uint64) {
	if t.StartTime == nil {
		return
	}

	return t.df.Count()
}

// Df return the dataflow object
func (t *TaskExecution) Df() *iop.Dataflow {
	return t.df
}

// Data return the dataset object
func (t *TaskExecution) Data() *iop.Dataset {
	return t.data
}

// GetRate return the speed of flow (rows / sec and bytes / sec)
// secWindow is how many seconds back to measure (0 is since beginning)
func (t *TaskExecution) GetRate(secWindow int) (rowRate, byteRate int64) {
	var secElapsed float64
	count := t.GetCount()
	bytes, _ := t.GetBytes()
	if t.StartTime == nil || t.StartTime.IsZero() {
		return
	} else if t.EndTime == nil || t.EndTime.IsZero() {
		st := *t.StartTime
		if secWindow <= 0 {
			secElapsed = time.Since(st).Seconds()
			rowRate = cast.ToInt64(math.Round(cast.ToFloat64(count) / secElapsed))
			byteRate = cast.ToInt64(math.Round(cast.ToFloat64(bytes) / secElapsed))
		} else {
			rowRate = cast.ToInt64(math.Round(cast.ToFloat64((count - t.prevRowCount) / cast.ToUint64(secWindow))))
			byteRate = cast.ToInt64(math.Round(cast.ToFloat64((bytes - t.prevByteCount) / cast.ToUint64(secWindow))))
			if t.prevRowCount < count {
				t.lastIncrement = time.Now()
			}
			t.prevRowCount = count
			t.prevByteCount = bytes
		}
	} else {
		st := *t.StartTime
		et := *t.EndTime
		secElapsed = cast.ToFloat64(et.UnixNano()-st.UnixNano()) / 1000000000.0
		rowRate = cast.ToInt64(math.Round(cast.ToFloat64(count) / secElapsed))
		byteRate = cast.ToInt64(math.Round(cast.ToFloat64(bytes) / secElapsed))
	}
	return
}

func (t *TaskExecution) setGetMetadata() (metadata iop.Metadata) {
	if t.Config.MetadataLoadedAt != nil && *t.Config.MetadataLoadedAt {
		metadata.LoadedAt.Key = slingLoadedAtColumn
		if os.Getenv("SLING_LOADED_AT_COLUMN") == "timestamp" {
			metadata.LoadedAt.Value = *t.StartTime
		} else {
			metadata.LoadedAt.Value = t.StartTime.Unix()
		}
	}
	if t.Config.MetadataStreamURL {
		metadata.StreamURL.Key = slingStreamURLColumn
	}

	if t.Config.MetadataRowID {
		metadata.RowID.Key = slingRowIDColumn
	}

	if t.Config.MetadataExecID {
		metadata.ExecID.Key = slingExecIDColumn
		metadata.ExecID.Value = t.ExecID
	}

	if t.Config.MetadataRowNum {
		metadata.RowNum.Key = slingRowNumColumn
	}

	// StarRocks: add _sling_row_id column if there is no primary,
	// duplicate or hash key defined and set as Hash Key
	if t.Config.TgtConn.Type == dbio.TypeDbStarRocks {
		addRowIDCol := true
		if t.Config.Target.Options.TableKeys != nil {
			for tableKey := range t.Config.Target.Options.TableKeys {
				if g.In(tableKey, iop.PrimaryKey, iop.HashKey, iop.DuplicateKey, iop.UniqueKey, iop.AggregateKey) {
					addRowIDCol = false
				}
			}
		}

		if addRowIDCol && t.Config.Source.HasPrimaryKey() {
			// set primary key for StarRocks
			t.Config.Target.Options.TableKeys[iop.PrimaryKey] = t.Config.Source.PrimaryKey()
			addRowIDCol = false
		}

		if addRowIDCol {
			metadata.RowID.Key = slingRowIDColumn
			t.Config.Target.Options.TableKeys[iop.HashKey] = []string{slingRowIDColumn}
		}
	}

	return metadata
}

func (t *TaskExecution) isUsingPool() bool {
	if val := os.Getenv("SLING_POOL"); val != "" && !cast.ToBool(val) {
		return false
	}
	return cast.ToBool(os.Getenv("SLING_CLI")) && t.Config.ReplicationMode()
}

func (t *TaskExecution) getTargetObjectValue() string {

	switch t.Type {
	case FileToDB:
		return t.Config.Target.Object
	case DbToDb:
		return t.Config.Target.Object
	case DbToFile:
		if t.Config.Options.StdOut {
			return "stdout"
		}
		return t.Config.TgtConn.URL()
	case FileToFile:
		if t.Config.Options.StdOut {
			return "stdout"
		}
		return t.Config.TgtConn.URL()
	}

	return ""
}

func (t *TaskExecution) AddCleanupTaskFirst(f func()) {
	t.Context.Mux.Lock()
	defer t.Context.Mux.Unlock()
	t.cleanupFuncs = append([]func(){f}, t.cleanupFuncs...)
}

func (t *TaskExecution) AddCleanupTaskLast(f func()) {
	t.Context.Mux.Lock()
	defer t.Context.Mux.Unlock()
	t.cleanupFuncs = append(t.cleanupFuncs, f)
}

func (t *TaskExecution) Cleanup() {
	t.Context.Mux.Lock()
	defer t.Context.Mux.Unlock()

	for i, f := range t.cleanupFuncs {
		f()
		t.cleanupFuncs[i] = func() {} // in case it gets called again
	}
	if t.df != nil {
		t.df.CleanUp()
	}
}

// shouldWriteViaDuckDB determines whether we should use duckdb
// at the moment, use duckdb only for parquet or partitioned
// target parquet or csv files
func (t *TaskExecution) shouldWriteViaDuckDB(uri string) bool {
	if g.In(t.Config.Target.ObjectFileFormat(), dbio.FileTypeParquet, dbio.FileTypeCsv) && len(iop.ExtractPartitionFields(uri)) > 0 {
		return true
	}
	return g.In(t.Config.Target.ObjectFileFormat(), dbio.FileTypeParquet)
}

// isIncrementalWithUpdateKey means it has an update_key and is incremental mode
func (t *TaskExecution) isIncrementalWithUpdateKey() bool {
	return t.Config.Source.HasUpdateKey() && t.Config.Mode == IncrementalMode
}

// isIncrementalStateWithUpdateKey means it has an update_key, with provided sling state and is incremental mode
func (t *TaskExecution) isIncrementalStateWithUpdateKey() bool {
	return os.Getenv("SLING_STATE") != "" && t.isIncrementalWithUpdateKey()
}

// hasStateWithUpdateKey means it has an update_key and with provided sling state
func (t *TaskExecution) hasStateWithUpdateKey() bool {
	return os.Getenv("SLING_STATE") != "" && t.Config.Source.HasUpdateKey()
}

func (t *TaskExecution) getOptionsMap() (options map[string]any) {
	options = g.M()
	g.Unmarshal(g.Marshal(t.Config.Source.Options), &options)

	if columns := t.Config.ColumnsPrepared(); len(columns) > 0 {
		// set as string so that StreamProcessor parses it
		options["columns"] = g.Marshal(columns)
	}

	if colTransforms := t.Config.TransformsPrepared(); len(colTransforms) > 0 {
		// set as string so that StreamProcessor parses it
		options["transforms"] = g.Marshal(colTransforms)
	}
	return
}

// apply column casing
func applyColumnCasingToDf(df *iop.Dataflow, connType dbio.Type, casing *iop.ColumnCasing) {

	if casing == nil {
		return
	}

	// convert to target system casing
	for i, col := range df.Columns {
		df.Columns[i].Name = casing.Apply(col.Name, connType)
	}

	// propagate names to streams
	for _, ds := range df.Streams {
		for i, col := range ds.Columns {
			ds.Columns[i].Name = casing.Apply(col.Name, connType)
		}

		if ds.CurrentBatch != nil {
			for i, col := range ds.CurrentBatch.Columns {
				ds.CurrentBatch.Columns[i].Name = casing.Apply(col.Name, connType)
			}
		}
	}
}

const (
	raiseIssueNotice = "Feel free to open an issue @ https://github.com/slingdata-io/sling-cli"
)

func ErrorHelper(err error) (helpString string) {
	if err != nil {
		errString := strings.ToLower(err.Error())
		E, ok := err.(*g.ErrType)
		if ok && E.Debug() != "" {
			errString = strings.ToLower(E.Full())
		}

		contains := func(text ...string) bool {
			met := true
			for _, t := range text {
				if !strings.Contains(errString, strings.ToLower(t)) {
					met = false
				}
			}
			return met
		}

		switch {
		case contains("utf8") || contains("ascii"):
			helpString = "Perhaps the 'transforms' source option could help with encodings? Also try `replace_non_printable`. See https://docs.slingdata.io/sling-cli/run/configuration#source"
		case contains("failed to verify certificate"):
			helpString = "Perhaps specifying `encrypt=true` and `TrustServerCertificate=true` properties could help? See https://docs.slingdata.io/connections/database-connections/sqlserver"
		case contains("ssl is not enabled on the server"):
			helpString = "Perhaps setting the 'sslmode' option could help? See https://docs.slingdata.io/connections/database-connections/postgres"
		case contains("invalid input syntax for type") || (contains(" value ") && contains("is not recognized")) || contains("invalid character value") || contains(" exceeds ") || contains(`could not convert`) || contains("provided schema does not match") || contains("Number out of representable range") || contains("Numeric value", " is not recognized") || contains("out of range") || contains("value too long") || contains("converting", "to", "is unsupported") || contains("stl_load_errors"):
			helpString = "Perhaps setting a higher 'SAMPLE_SIZE' environment variable could help? This represents the number of records to process in order to infer column types (especially for file sources). The default is 900. Try 2000 or even higher.\nYou can also manually specify the column types with the `columns` source option. See https://docs.slingdata.io/sling-cli/run/configuration#source\nFurthermore, you can try the `target_options.adjust_column_type` setting to allow Sling to automatically alter the column type on the target side."
		case contains("bcp import"):
			helpString = "If facing issues with Microsoft's BCP, try disabling Bulk Loading with `use_bulk=false`. See https://docs.slingdata.io/sling-cli/run/configuration#target"
		case contains("[AppendRow]: converting"):
			helpString = "Perhaps using the `adjust_column_type: true` target option could help? See https://docs.slingdata.io/sling-cli/run/configuration#target"
		case contains("mkdir", "permission denied"):
			helpString = "Perhaps setting the SLING_TEMP_DIR environment variable to a writable folder will help."
		case contains("canceling statement due to conflict with recovery"):
			helpString = "Perhaps adjusting the `max_standby_archive_delay` and `max_standby_streaming_delay` settings in the source PG Database could help. See https://stackoverflow.com/questions/14592436/postgresql-error-canceling-statement-due-to-conflict-with-recovery"
		case contains("wrong number of fields"):
			helpString = "Perhaps setting the delimiter (source_options.delimiter) would help? See https://docs.slingdata.io/sling-cli/run/configuration#source"
		case contains("not implemented makeGoLangScanType"):
			helpString = "This is related to the Microsoft go-mssqldb driver, which willingly calls a panic for certain column types (such as geometry columns). See https://github.com/microsoft/go-mssqldb/issues/79 and https://github.com/microsoft/go-mssqldb/pull/32. The workaround is to use Custom SQL, and convert the problematic column type into a varchar."
		case contains("cannot create parquet value") && contains("from go value of type"):
			helpString = "This is happening when the column type changes mid-stream. Try casting the problematic column to a proper type with the `columns` property."
		}
	}
	return
}
