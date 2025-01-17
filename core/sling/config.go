package sling

import (
	"context"
	"database/sql/driver"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/spf13/cast"

	"github.com/flarco/g"
	jsoniter "github.com/json-iterator/go"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"gopkg.in/yaml.v2"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

// Mode is a load mode
type Mode string

const (
	// TruncateMode is to truncate
	TruncateMode Mode = "truncate"
	// FullRefreshMode is to drop
	FullRefreshMode Mode = "full-refresh"
	// IncrementalMode is to incremental
	IncrementalMode Mode = "incremental"
	// SnapshotMode is to snapshot
	SnapshotMode Mode = "snapshot"
	// BackfillMode is to backfill
	BackfillMode Mode = "backfill"
)

var AllMode = []struct {
	Value  Mode
	TSName string
}{
	{FullRefreshMode, "FullRefreshMode"},
	{IncrementalMode, "IncrementalMode"},
	{TruncateMode, "TruncateMode"},
	{SnapshotMode, "SnapshotMode"},
	{BackfillMode, "BackfillMode"},
}

// NewConfig return a config object from a YAML / JSON string
func NewConfig(cfgStr string) (cfg *Config, err error) {
	// set default, unmarshalling will overwrite
	cfg = &Config{}

	err = cfg.Unmarshal(cfgStr)
	if err != nil {
		err = g.Error(err, "Unable to parse config payload")
		return
	}

	err = cfg.Prepare()
	if err != nil {
		err = g.Error(err, "Unable to prepare config")
		return
	}
	return
}

// SetDefault sets default options
func (cfg *Config) SetDefault() {

	// set source options
	var sourceOptions SourceOptions
	switch cfg.SrcConn.Type.Kind() {
	case dbio.KindFile:
		sourceOptions = SourceFileOptionsDefault
	case dbio.KindDatabase:
		sourceOptions = SourceDBOptionsDefault
	default:
		sourceOptions = SourceDBOptionsDefault
	}

	if cfg.Source.Options == nil {
		cfg.Source.Options = &SourceOptions{}
	}
	cfg.Source.Options.SetDefaults(sourceOptions)

	// https://github.com/slingdata-io/sling-cli/issues/348
	if g.IsNil(cfg.Target.Columns) && !g.IsNil(cfg.Source.Options.Columns) {
		cfg.Target.Columns = cfg.Source.Options.Columns // accepts legacy config
		cfg.Source.Options.Columns = nil
	}
	if g.IsNil(cfg.Transforms) && !g.IsNil(cfg.Source.Options.Transforms) {
		cfg.Transforms = cfg.Source.Options.Transforms // accepts legacy config
		cfg.Source.Options.Transforms = nil
	}

	// set target options
	var targetOptions TargetOptions
	switch cfg.TgtConn.Type.Kind() {
	case dbio.KindFile:
		targetOptions = TargetFileOptionsDefault
	case dbio.KindDatabase:
		targetOptions = TargetDBOptionsDefault
	default:
		targetOptions = TargetDBOptionsDefault
	}

	if cfg.Target.Options == nil {
		cfg.Target.Options = &TargetOptions{}
	}
	cfg.Target.Options.SetDefaults(targetOptions)

	if cfg.Target.Options.AdjustColumnType == nil && (cfg.SrcConn.Type.Kind() == dbio.KindFile || cfg.Options.StdIn) {
		// if source stream is file, we have no schema reference
		cfg.Target.Options.AdjustColumnType = g.Bool(false)
	}

	// set max_decimals
	switch cfg.TgtConn.Type {
	case dbio.TypeDbBigQuery, dbio.TypeDbBigTable:
		cfg.Source.Options.MaxDecimals = g.Int(9)
		cfg.Target.Options.MaxDecimals = g.Int(9)
	case dbio.TypeDbClickhouse, dbio.TypeDbProton:
		cfg.Source.Options.MaxDecimals = g.Int(11)
		cfg.Target.Options.MaxDecimals = g.Int(11)
		if cfg.Target.Options.BatchLimit == nil {
			// set default batch_limit to limit memory usage. Bug in clickhouse driver?
			// see https://github.com/ClickHouse/clickhouse-go/issues/1293
			cfg.Target.Options.BatchLimit = g.Int64(100000)
		}
	}

	// set default transforms
	switch cfg.SrcConn.Type {
	case dbio.TypeDbMySQL, dbio.TypeDbMariaDB, dbio.TypeDbStarRocks:
		// parse_bit for MySQL
		cfg.extraTransforms = append(cfg.extraTransforms, "parse_bit")
	}

	// set default metadata
	switch {
	case g.In(cfg.TgtConn.Type, dbio.TypeDbStarRocks):
		cfg.extraTransforms = append(cfg.extraTransforms, "parse_bit")
	case g.In(cfg.TgtConn.Type, dbio.TypeDbBigQuery):
		cfg.Target.Options.DatetimeFormat = "2006-01-02 15:04:05.000000-07"
	}

	// set vars
	for k, v := range cfg.Env {
		os.Setenv(k, v)
	}

	// default mode
	if cfg.Mode == "" {
		cfg.Mode = FullRefreshMode
	}

	if val := os.Getenv("SLING_LOADED_AT_COLUMN"); val != "" {
		if cast.ToBool(val) || val == "unix" || val == "timestamp" {
			cfg.MetadataLoadedAt = g.Bool(true)
		} else {
			cfg.MetadataLoadedAt = g.Bool(false)
		}
	}
	if val := os.Getenv("SLING_STREAM_URL_COLUMN"); val != "" {
		cfg.MetadataStreamURL = cast.ToBool(val)
	}
	if val := os.Getenv("SLING_ROW_ID_COLUMN"); val != "" {
		cfg.MetadataRowID = cast.ToBool(val)
	}
	if val := os.Getenv("SLING_EXEC_ID_COLUMN"); val != "" {
		cfg.MetadataExecID = cast.ToBool(val)
	}
	if val := os.Getenv("SLING_ROW_NUM_COLUMN"); val != "" {
		cfg.MetadataRowNum = cast.ToBool(val)
	}
	if val := os.Getenv("SAMPLE_SIZE"); val != "" {
		iop.SampleSize = cast.ToInt(val)
	}
}

// Unmarshal parse a configuration file path or config text
func (cfg *Config) Unmarshal(cfgStr string) (err error) {
	// Add panic recovery
	defer func() {
		if r := recover(); r != nil {
			err = g.Error(r, "Panic occurred while unmarshalling config")
		}
	}()

	// expand variables
	cfgStr = expandEnvVars(cfgStr)

	cfgBytes := []byte(cfgStr)
	_, errStat := os.Stat(cfgStr)
	if errStat == nil {
		cfgFile, err := os.Open(cfgStr)
		if err != nil {
			return g.Error(err, "Unable to open cfgStr: "+cfgStr)
		}

		cfgBytes, err = io.ReadAll(cfgFile)
		if err != nil {
			return g.Error(err, "could not read from cfgFile")
		}
	}

	err = yaml.Unmarshal(cfgBytes, cfg)
	if err != nil {
		if errStat != nil && !strings.Contains(cfgStr, "\n") && !strings.Contains(cfgStr, ": ") {
			return g.Error(errStat, "Error parsing config. Invalid path or raw config provided")
		} else if strings.Contains(string(cfgBytes), "streams:") || strings.Contains(string(cfgBytes), `"streams"`) {
			return g.Error("Error parsing config. Is your config file a replication? If so, please use the -r flag instead of -c")
		}
		return g.Error(err, "Error parsing config")
	}

	if cfg.Env == nil {
		cfg.Env = map[string]string{}
	}

	if cfg.Source.Data == nil {
		cfg.Source.Data = g.M()
	}

	if cfg.SrcConn.Data == nil {
		cfg.SrcConn.Data = g.M()
	}

	if cfg.Target.Data == nil {
		cfg.Target.Data = g.M()
	}

	if cfg.TgtConn.Data == nil {
		cfg.TgtConn.Data = g.M()
	}

	// add config path
	if g.PathExists(cfgStr) && !cfg.ReplicationMode() {
		cfg.Env["SLING_CONFIG_PATH"] = cfgStr
	}

	return nil
}

// setSchema sets the default schema
func setSchema(schema string, obj string) string {

	// fill table schema if needed
	if schema != "" && obj != "" && !strings.Contains(obj, ".") {
		obj = g.F("%s.%s", schema, obj)
	}

	return obj
}

func (cfg *Config) sourceIsFile() bool {
	return cfg.Options.StdIn || cfg.SrcConn.Info().Type.IsFile()
}

func (cfg *Config) DetermineType() (Type JobType, err error) {

	srcFileProvided := cfg.sourceIsFile()
	tgtFileProvided := cfg.Options.StdOut || cfg.TgtConn.Info().Type.IsFile()
	srcDbProvided := cfg.SrcConn.Info().Type.IsDb()
	tgtDbProvided := cfg.TgtConn.Info().Type.IsDb()
	srcStreamProvided := cfg.Source.Stream != ""

	summary := g.F("srcFileProvided: %t, tgtFileProvided: %t, srcDbProvided: %t, tgtDbProvided: %t, srcStreamProvided: %t", srcFileProvided, tgtFileProvided, srcDbProvided, tgtDbProvided, srcStreamProvided)
	g.Trace(summary)

	if cfg.Mode == "" {
		if cfg.Source.PrimaryKeyI != nil || cfg.Source.UpdateKey != "" {
			cfg.Mode = IncrementalMode
		} else {
			cfg.Mode = FullRefreshMode
		}
	}

	validMode := g.In(cfg.Mode, FullRefreshMode, IncrementalMode, BackfillMode, SnapshotMode, TruncateMode)
	if !validMode {
		err = g.Error("must specify valid mode: full-refresh, incremental, backfill, snapshot or truncate")
		return
	}

	if cfg.Mode == IncrementalMode {
		if cfg.SrcConn.Info().Type == dbio.TypeDbBigTable {
			// use default keys if none are provided
			if len(cfg.Source.PrimaryKey()) == 0 {
				cfg.Source.PrimaryKeyI = []string{"_bigtable_key"}
			}

			if cfg.Source.UpdateKey == "" {
				cfg.Source.UpdateKey = "_bigtable_timestamp"
			}
		} else if srcFileProvided && cfg.Source.UpdateKey == slingLoadedAtColumn {
			// need to loaded_at column for file incremental
			cfg.MetadataLoadedAt = g.Bool(true)
		} else if cfg.Source.UpdateKey == "" && len(cfg.Source.PrimaryKey()) == 0 {
			err = g.Error("must specify value for 'update_key' and/or 'primary_key' for incremental mode. See docs for more details: https://docs.slingdata.io/sling-cli/run/configuration")
			if args := os.Getenv("SLING_CLI_ARGS"); strings.Contains(args, "-src-conn") || strings.Contains(args, "-tgt-conn") {
				err = g.Error("must specify value for '--update-key' and/or '--primary-key' for incremental mode. See docs for more details: https://docs.slingdata.io/sling-cli/run/configuration")
			}
			return
		}
	} else if cfg.Mode == BackfillMode {
		if cfg.Source.UpdateKey == "" || len(cfg.Source.PrimaryKey()) == 0 {
			err = g.Error("must specify value for 'update_key' and 'primary_key' for backfill mode. See docs for more details: https://docs.slingdata.io/sling-cli/run/configuration")
			if args := os.Getenv("SLING_CLI_ARGS"); strings.Contains(args, "-src-conn") || strings.Contains(args, "-tgt-conn") {
				err = g.Error("must specify value for '--update-key' and '--primary-key' for backfill mode. See docs for more details: https://docs.slingdata.io/sling-cli/run/configuration")
			}
			return
		}
		if cfg.Source.Options == nil || cfg.Source.Options.Range == nil {
			err = g.Error("must specify range (source.options.range or --range) for backfill mode. See docs for more details: https://docs.slingdata.io/sling-cli/run/configuration")
			return
		} else if rangeArr := strings.Split(*cfg.Source.Options.Range, ","); len(rangeArr) != 2 {
			err = g.Error("must specify valid range value for backfill mode separated by one comma, for example `2021-01-01,2021-02-01`. See docs for more details: https://docs.slingdata.io/sling-cli/run/configuration")
			return
		}
	} else if cfg.Mode == SnapshotMode {
		cfg.MetadataLoadedAt = g.Bool(true) // needed for snapshot mode
	}

	if srcDbProvided && tgtDbProvided {
		Type = DbToDb
	} else if srcFileProvided && tgtDbProvided {
		Type = FileToDB
		if cfg.MetadataLoadedAt == nil {
			cfg.MetadataLoadedAt = g.Bool(true) // default when source is file
		}
	} else if srcDbProvided && srcStreamProvided && !tgtDbProvided && tgtFileProvided {
		Type = DbToFile
	} else if srcFileProvided && !srcDbProvided && !tgtDbProvided && tgtFileProvided {
		Type = FileToFile
	} else if tgtDbProvided && cfg.Target.Options != nil && cfg.Target.Options.PostSQL != nil {
		cfg.Target.Object = *cfg.Target.Options.PostSQL
		Type = DbSQL
	}

	if Type == "" {
		// g.PP(t)
		sourceErrMsg := ""
		targetErrMsg := ""

		if !cfg.Options.StdIn {
			if cfg.SrcConn.Name == "" {
				targetErrMsg = g.F("source connection is missing, need to provide")
			} else if cfg.SrcConn.Name != "" && cfg.SrcConn.Info().Type.IsUnknown() {
				sourceErrMsg = g.F("source connection '%s' not valid / found in environment", cfg.SrcConn.Name)
			}
		}

		if !cfg.Options.StdOut {
			if cfg.TgtConn.Name == "" {
				targetErrMsg = g.F("target connection is missing, need to provide")
			} else if cfg.TgtConn.Name != "" && cfg.TgtConn.Info().Type.IsUnknown() {
				targetErrMsg = g.F("target connection '%s' not valid / found in environment", cfg.TgtConn.Name)
			}
		}

		output := []string{}
		if sourceErrMsg != "" {
			output = append(output, g.F("error -> %s", sourceErrMsg))
		}
		if targetErrMsg != "" {
			output = append(output, g.F("error -> %s", targetErrMsg))
		}

		err = g.Error("invalid Task Configuration. Must specify valid source conn / file or target connection / output.\n  %s", strings.Join(output, "\n  "))
	}
	return Type, err
}

func (cfg *Config) HasWildcard() bool {
	if strings.HasSuffix(cfg.Source.Stream, ".*") {
		return true
	}

	if strings.Contains(cfg.Source.Stream, `/*.`) || strings.Contains(cfg.Source.Stream, `\*.`) {
		return true
	}

	return false
}

func (cfg *Config) AsReplication() (rc ReplicationConfig) {
	rc = ReplicationConfig{
		Source: cfg.Source.Conn,
		Target: cfg.Target.Conn,
		Defaults: ReplicationStreamConfig{
			SourceOptions: cfg.Source.Options,
			TargetOptions: cfg.Target.Options,
			Select:        cfg.Source.Select,
			Where:         cfg.Source.Where,
			Object:        cfg.Target.Object,
			Mode:          cfg.Mode,
			PrimaryKeyI:   cfg.Source.PrimaryKeyI,
			UpdateKey:     cfg.Source.UpdateKey,
		},
		Streams: map[string]*ReplicationStreamConfig{
			cfg.Source.Stream: {},
		},
	}

	return rc
}

// Prepare prepares the config
func (cfg *Config) Prepare() (err error) {
	if cfg.Prepared {
		return
	}

	// get local connections
	connsMap := lo.KeyBy(connection.GetLocalConns(), func(c connection.ConnEntry) string {
		return strings.ToLower(c.Connection.Name)
	})

	// Check Inputs
	if !cfg.Options.StdIn && cfg.Source.Conn == "" && cfg.Target.Conn == "" {
		return g.Error("invalid source connection (blank or not found)")
	}
	if !cfg.Options.StdOut && cfg.Target.Conn == "" && cfg.Target.Object == "" {
		if cast.ToBool(os.Getenv("SLING_CLI")) {
			return g.Error("invalid target connection (blank or not found). Did you mean to use the `--stdout` flag?")
		}
		return g.Error("invalid target connection (blank or not found)")
	}
	if !cfg.Options.StdOut && cfg.Target.Conn != "" && cfg.Target.Object == "" {
		return g.Error("invalid target object (blank or not found)")
	}

	if cfg.Options.Debug && os.Getenv("DEBUG") == "" {
		os.Setenv("DEBUG", "LOW")
	}
	if cfg.Options.StdIn && cfg.Source.Stream == "" {
		cfg.Source.Stream = "stdin"
	}

	// set from shell env variable, if value starts with $ and found
	if cfg.Env == nil {
		cfg.Env = map[string]string{}
	}

	for k, v := range cfg.Env {
		cfg.Env[k] = os.ExpandEnv(v)
	}

	// Set Target
	cfg.Target.Object = strings.TrimSpace(cfg.Target.Object)
	if cfg.Target.Data == nil || len(cfg.Target.Data) == 0 {
		cfg.Target.Data = g.M()
		if c, ok := connsMap[strings.ToLower(cfg.Target.Conn)]; ok {
			cfg.TgtConn = *c.Connection.Copy()
		} else if connType := connection.SchemeType(cfg.Target.Conn); !connType.IsUnknown() {
			cfg.TgtConn, err = connection.NewConnectionFromURL(connType.String(), cfg.Target.Conn)
			if err != nil {
				return g.Error(err, "could not init target connection")
			}
		} else if !strings.Contains(cfg.Target.Conn, "://") && cfg.Target.Conn != "" && cfg.TgtConn.Data == nil {
			return g.Error("could not find connection %s", cfg.Target.Conn)
		} else if cfg.TgtConn.Data == nil {
			cfg.TgtConn.Data = g.M()
		}
		cfg.Target.Data = cfg.TgtConn.Data
	}

	if connection.SchemeType(cfg.Target.Object).IsFile() {
		if connection.SchemeType(cfg.Target.Object) == dbio.TypeFileLocal {
			cfg.Target.Object = strings.ReplaceAll(cfg.Target.Object, `\`, `/`) // windows path fix
		}

		// format target name, especially variable hostname
		err = cfg.FormatTargetObjectName()
		if err != nil {
			return g.Error(err, "could not format target object name")
		}
	} else {
		if cfg.TgtConn.Type.IsFile() && cfg.Target.Object != "" {
			fc, err := cfg.TgtConn.AsFile(true)
			if err != nil {
				return g.Error(err, "could not init file connection")
			}
			// object is not url, but relative path, needs to be normalized
			cfg.Target.Data["url"] = filesys.NormalizeURI(fc, cfg.Target.Object)
		}
	}

	if cfg.TgtConn.Type.IsUnknown() {
		tgtConn, err := connection.NewConnectionFromMap(g.M(
			"name", cfg.Target.Conn, "data", cfg.Target.Data,
		))
		if err != nil {
			return g.Error(err, "could not create data conn for target")
		}
		cfg.TgtConn = tgtConn
	}

	if cfg.Options.StdOut && os.Getenv("CONCURRENCY") == "" {
		os.Setenv("CONCURRENCY", "1")
	}

	if cfg.Target.Options == nil {
		cfg.Target.Options = &TargetOptions{}
	}

	// Set Source
	cfg.Source.Stream = strings.TrimSpace(cfg.Source.Stream)
	if cfg.Source.Data == nil || len(cfg.Source.Data) == 0 {
		cfg.Source.Data = g.M()
		if c, ok := connsMap[strings.ToLower(cfg.Source.Conn)]; ok {
			cfg.SrcConn = *c.Connection.Copy()
		} else if connType := connection.SchemeType(cfg.Source.Conn); !connType.IsUnknown() {
			cfg.SrcConn, err = connection.NewConnectionFromURL(connType.String(), cfg.Source.Conn)
			if err != nil {
				return g.Error(err, "could not init source connection")
			}
		} else if !strings.Contains(cfg.Source.Conn, "://") && cfg.Source.Conn != "" && cfg.SrcConn.Data == nil {
			return g.Error("could not find connection %s", cfg.Source.Conn)
		} else if cfg.SrcConn.Data == nil {
			cfg.SrcConn.Data = g.M()
		}
		cfg.Source.Data = cfg.SrcConn.Data
	}

	if connection.SchemeType(cfg.Source.Stream) == dbio.TypeFileLocal {
		cfg.Source.Stream = strings.ReplaceAll(cfg.Source.Stream, `\`, `/`) // windows path fix
	}

	// set sql to stream if source conn is db
	if cfg.SrcConn.Type.IsDb() && cfg.Source.Query != "" {
		cfg.Source.Stream = strings.TrimSpace(cfg.Source.Query)
	}

	if connection.SchemeType(cfg.Source.Stream).IsFile() && !strings.HasSuffix(cfg.Source.Stream, ".sql") {
		cfg.Source.Data["url"] = cfg.Source.Stream
		cfg.SrcConn.Data["url"] = cfg.Source.Stream
	} else {
		if cfg.SrcConn.Type.IsFile() && cfg.Source.Stream != "" {
			// stream is not url, but relative path
			fc, err := cfg.SrcConn.AsFile(true)
			if err != nil {
				return g.Error(err, "could not init file connection")
			}
			// object is not url, but relative path, needs to be normalized
			cfg.Source.Data["url"] = filesys.NormalizeURI(fc, cfg.Source.Stream)
		}
	}

	if cfg.SrcConn.Type.IsUnknown() {
		srcConn, err := connection.NewConnectionFromMap(g.M("name", cfg.Source.Conn, "data", cfg.Source.Data))
		if err != nil {
			return g.Error(err, "could not create data conn for source")
		}
		cfg.SrcConn = srcConn
	}

	// format target name, now we have source info
	err = cfg.FormatTargetObjectName()
	if err != nil {
		return g.Error(err, "could not format target object name")
	} else if strings.Contains(cfg.Target.Object, "{") {
		words := []string{}
		for _, m := range g.Matches(cfg.Target.Object, `\{([^}]+)\}`) {
			if len(m.Group) > 0 && !strings.HasPrefix(m.Group[0], "part_") {
				words = append(words, m.Group[0])
			}
		}

		if len(words) > 0 {
			g.Debug("Could not successfully format target object name. Blank values for: %s", strings.Join(words, ", "))
			for _, word := range words {
				cfg.Target.Object = strings.ReplaceAll(cfg.Target.Object, "{"+word+"}", "")
			}
		}
		if cfg.ReplicationStream != nil {
			cfg.ReplicationStream.Object = cfg.Target.Object
		}
	}

	// add md5 of options, so that wee reconnect for various options
	// see variable `connPool`
	cfg.SrcConn.Data["_source_options_md5"] = g.MD5(g.Marshal(cfg.Source.Options))
	cfg.TgtConn.Data["_target_options_md5"] = g.MD5(g.Marshal(cfg.Target.Options))

	// set conn types
	cfg.Source.Type = cfg.SrcConn.Type
	cfg.Target.Type = cfg.TgtConn.Type

	// validate capability to write
	switch cfg.Target.Type {
	case dbio.TypeDbPrometheus, dbio.TypeDbMongoDB, dbio.TypeDbElasticsearch, dbio.TypeDbBigTable:
		return g.Error("sling cannot currently write to %s", cfg.Target.Type)
	}

	// validate table keys
	if tkMap := cfg.Target.Options.TableKeys; tkMap != nil {
		for _, kt := range lo.Keys(tkMap) {
			// append _key
			ktAllowed := g.Marshal(iop.KeyTypes)
			ktAllowed = strings.TrimSuffix(strings.TrimPrefix(ktAllowed, "["), "]")
			if !g.In(kt, iop.KeyTypes...) {
				g.Warn("%s is not a valid table key type. Valid table key types are: %s", kt, ktAllowed)
				delete(cfg.Target.Options.TableKeys, kt)
			}
		}
	}

	// validate conn data keys
	for key := range cfg.SrcConn.Data {
		if strings.Contains(key, ":") {
			g.Warn("source connection %s has an invalid property -> %s", cfg.Source.Conn, key)
		}
	}

	for key := range cfg.TgtConn.Data {
		if strings.Contains(key, ":") {
			g.Warn("target connection %s has an invalid property -> %s", cfg.Target.Conn, key)
		}
	}

	// to expand variables for custom SQL
	fMap, err := cfg.GetFormatMap()
	if err != nil {
		return g.Error(err, "could not get format map for sql")
	}

	// sql & where prop
	cfg.Source.Where = g.Rm(cfg.Source.Where, fMap)
	cfg.Source.Query = g.R(g.Rm(cfg.Source.Query, fMap), "where_cond", cfg.Source.Where)
	if cfg.ReplicationStream != nil {
		cfg.ReplicationStream.SQL = cfg.Source.Query
	}

	// check if referring to a SQL file, and set stream text
	if cfg.SrcConn.Type.IsDb() {

		sTable, _ := database.ParseTableName(cfg.Source.Stream, cfg.SrcConn.Type)
		if connection.SchemeType(cfg.Source.Stream).IsFile() && g.PathExists(strings.TrimPrefix(cfg.Source.Stream, "file://")) {
			// for incremental, need to put `{incremental_where_cond}` for proper selecting
			sqlFromFile, err := GetSQLText(cfg.Source.Stream)
			if err != nil {
				err = g.Error(err, "Could not get getSQLText for: "+cfg.Source.Stream)
				if sTable.Name == "" {
					return err
				} else {
					err = nil // don't return error in case the table full name ends with .sql
				}
			} else {
				cfg.Source.Stream = g.Rm(sqlFromFile, fMap)
				if cfg.ReplicationStream != nil {
					cfg.ReplicationStream.SQL = cfg.Source.Stream
				}
			}
		} else if sTable.IsQuery() {
			cfg.Source.Stream = g.Rm(sTable.SQL, fMap)
			if cfg.ReplicationStream != nil {
				cfg.ReplicationStream.SQL = cfg.Source.Stream
			}
		}
	}

	// compile pre and post sql
	if cfg.TgtConn.Type.IsDb() {

		// pre SQL
		if preSQL := cfg.Target.Options.PreSQL; preSQL != nil && *preSQL != "" {
			sql, err := GetSQLText(*preSQL)
			if err != nil {
				return g.Error(err, "could not get pre-sql body")
			}
			cfg.Target.Options.PreSQL = g.String(g.Rm(sql, fMap))
			if cfg.ReplicationStream != nil {
				cfg.ReplicationStream.TargetOptions.PreSQL = cfg.Target.Options.PreSQL
			}
		}

		// post SQL
		if postSQL := cfg.Target.Options.PostSQL; postSQL != nil && *postSQL != "" {
			sql, err := GetSQLText(*postSQL)
			if err != nil {
				return g.Error(err, "could not get post-sql body")
			}
			cfg.Target.Options.PostSQL = g.String(g.Rm(sql, fMap))
			if cfg.ReplicationStream != nil {
				cfg.ReplicationStream.TargetOptions.PostSQL = cfg.Target.Options.PostSQL
			}
		}
	}

	// done
	cfg.Prepared = true
	return
}

func (cfg *Config) FormatTargetObjectName() (err error) {
	m, err := cfg.GetFormatMap()
	if err != nil {
		return g.Error(err, "could not get formatting variables")
	}

	// clean values for replacing, these need to be clean to be used in the object name
	dateMap := iop.GetISO8601DateMap(time.Now())
	for k, v := range m {
		if _, ok := dateMap[k]; ok {
			continue // don't clean the date values
		}
		if g.In(k, "run_timestamp", "object_full_name", "stream_full_name") {
			continue // don't clean those keys, will add an underscore prefix
		}
		m[k] = iop.CleanName(cast.ToString(v))
	}

	// replace placeholders
	cfg.Target.Object = strings.TrimSpace(g.Rm(cfg.Target.Object, m))

	if cfg.TgtConn.Type.IsDb() {
		// normalize casing of object names
		table, err := database.ParseTableName(cfg.Target.Object, cfg.TgtConn.Type)
		if err != nil {
			return g.Error(err, "could not parse target table name")
		} else if table.IsQuery() {
			return g.Error("invalid table name: %s", table.Raw)
		} else if table.Schema == "" {
			table.Schema = cast.ToString(cfg.Target.Data["schema"])
		}
		cfg.Target.Object = table.FullName()

		// fill in temp table name if specified
		if tgtOpts := cfg.Target.Options; tgtOpts != nil {
			tgtOpts.TableTmp = strings.TrimSpace(g.Rm(tgtOpts.TableTmp, m))
			if tgtOpts.TableTmp != "" {
				tableTmp, err := database.ParseTableName(tgtOpts.TableTmp, cfg.TgtConn.Type)
				if err != nil {
					return g.Error(err, "could not parse temp table name")
				} else if tableTmp.Schema == "" {
					tableTmp.Schema = cast.ToString(cfg.Target.Data["schema"])
				}
				tgtOpts.TableTmp = tableTmp.FullName()
			}
		}
	}

	if connection.SchemeType(cfg.Target.Object).IsFile() {
		cfg.Target.Data["url"] = cfg.Target.Object
		cfg.TgtConn.Data["url"] = cfg.Target.Object
	} else if cfg.TgtConn.Type.IsFile() {
		url := cast.ToString(cfg.Target.Data["url"])
		cfg.Target.Data["url"] = strings.TrimSpace(g.Rm(url, m))
	}

	// set on ReplicationStream
	if cfg.ReplicationStream != nil {
		cfg.ReplicationStream.Object = cfg.Target.Object
	}

	return nil
}

// GetFormatMap returns a map to format a string with provided with variables
func (cfg *Config) GetFormatMap() (m map[string]any, err error) {
	replacePattern := regexp.MustCompile("[^_0-9a-zA-Z]+") // to clean name
	cleanUp := func(o string) string {
		return string(replacePattern.ReplaceAll([]byte(o), []byte("_")))
	}

	m = g.M(
		"run_timestamp", time.Now().Format("2006_01_02_150405"),
	)

	if cfg.SrcConn.Type.String() != "" {
		m["source_type"] = string(cfg.SrcConn.Type)
		m["source_kind"] = string(cfg.SrcConn.Type.Kind())
	}
	if cfg.TgtConn.Type.String() != "" {
		m["target_type"] = string(cfg.TgtConn.Type)
		m["target_kind"] = string(cfg.TgtConn.Type.Kind())
	}

	if cfg.Source.Conn != "" {
		m["source_name"] = strings.ToLower(cfg.Source.Conn)
	}

	if cfg.Target.Conn != "" {
		m["target_name"] = strings.ToLower(cfg.Target.Conn)
	}

	if cfg.SrcConn.Type.IsDb() {
		table, err := database.ParseTableName(cfg.Source.Stream, cfg.SrcConn.Type)
		if err != nil {
			return m, g.Error(err, "could not parse stream table name")
		}

		if table.SQL != "" && cfg.StreamName != "" {
			table, err = database.ParseTableName(cfg.StreamName, cfg.SrcConn.Type)
			if err != nil {
				return m, g.Error(err, "could not parse stream name: %s", cfg.StreamName)
			}
		}
		if table.Schema != "" {
			m["stream_schema"] = table.Schema
		}
		if table.Name != "" {
			m["stream_table"] = table.Name
			m["stream_full_name"] = table.FDQN()
		}

		if cfg.StreamName != "" {
			m["stream_name"] = strings.ToLower(cfg.StreamName)
		}
	}

	if cfg.TgtConn.Type.IsDb() {
		m["object_name"] = cfg.Target.Object

		table, err := database.ParseTableName(cfg.Target.Object, cfg.TgtConn.Type)
		if err != nil {
			return m, g.Error(err, "could not parse target table name")
		} else if table.IsQuery() {
			return m, g.Error("invalid table name: %s", table.Raw)
		}

		m["object_schema"] = table.Schema
		m["object_table"] = table.Name

		if targetSchema := cast.ToString(cfg.Target.Data["schema"]); targetSchema != "" {
			m["target_schema"] = targetSchema
			if table.Schema == "" {
				table.Schema = targetSchema
				m["object_schema"] = targetSchema
			}
		}
		m["object_full_name"] = table.FDQN()

		// legacy
		m["target_table"] = m["object_table"]
	}

	if cfg.SrcConn.Type.IsFile() {
		uri := cfg.SrcConn.URL()
		m["stream_name"] = strings.ToLower(cfg.Source.Stream)
		m["stream_full_name"] = cfg.Source.Stream

		fc, err := cfg.SrcConn.AsFile(true)
		if err != nil {
			return m, g.Error(err, "could not init source conn as file")
		}

		filePath, err := fc.GetPath(uri)
		if err != nil {
			return m, g.Error(err, "could not parse file path")
		}
		if filePath != "" {
			m["stream_file_path"] = cleanUp(filePath)
		}

		pathArr := strings.Split(strings.TrimPrefix(strings.TrimSuffix(filePath, "/"), "/"), "/")
		fileName := pathArr[len(pathArr)-1]
		m["stream_file_name"] = cleanUp(fileName)

		fileFolder := ""
		if len(pathArr) > 1 {
			fileFolder = pathArr[len(pathArr)-2]
			m["stream_file_folder"] = cleanUp(strings.TrimPrefix(fileFolder, "/"))
		}

		switch cfg.SrcConn.Type {
		case dbio.TypeFileS3, dbio.TypeFileGoogle:
			m["source_bucket"] = cfg.SrcConn.Data["bucket"]
		case dbio.TypeFileAzure:
			m["source_account"] = cfg.SrcConn.Data["account"]
			m["source_container"] = cfg.SrcConn.Data["container"]
		}

		if fileNameArr := strings.Split(fileName, "."); len(fileNameArr) > 1 {
			// remove extension
			m["stream_file_ext"] = fileNameArr[len(fileNameArr)-1]
			if len(fileNameArr) >= 3 {
				// in case of compression (2 extension tokens)
				for _, suff := range []string{"gz", "zst", "snappy"} {
					if m["stream_file_ext"] == suff {
						m["stream_file_ext"] = fileNameArr[len(fileNameArr)-2] + "_" + fileNameArr[len(fileNameArr)-1]
						break
					}
				}
			}

			m["stream_file_name"] = strings.TrimSuffix(
				cast.ToString(m["stream_file_name"]),
				"_"+cast.ToString(m["stream_file_ext"]),
			)
		}

		// duckdb sql on files, make `stream_scanner`
		if cfg.Source.Query != "" {
			// get file format in order to match scanner
			fileFormat := dbio.FileTypeNone
			if cfg.Source.Options != nil && cfg.Source.Options.Format != nil {
				fileFormat = *cfg.Source.Options.Format
			}
			if fileFormat == dbio.FileTypeNone {
				fileFormat = filesys.InferFileFormat(uri, dbio.FileTypeNone)
			}
			if fileFormat == dbio.FileTypeNone {
				g.Warn("%s: stream format is empty, cannot determine stream_scanner", cfg.StreamName)
			} else {
				duck := iop.NewDuckDb(context.Background())
				streamScanner := dbio.TypeDbDuckDb.GetTemplateValue("function." + duck.GetScannerFunc(fileFormat))
				m["stream_scanner"] = g.R(streamScanner, "uri", strings.TrimPrefix(uri, "file://"))
			}
		}
	}

	if t := connection.SchemeType(cfg.Target.Object); t.IsFile() {
		m["object_name"] = strings.ToLower(cfg.Target.Object)
		m["object_full_name"] = cfg.Target.Object

		switch t {
		case dbio.TypeFileS3:
			m["target_bucket"] = cfg.Target.Data["bucket"]
		case dbio.TypeFileGoogle:
			m["target_bucket"] = cfg.Target.Data["bucket"]
		case dbio.TypeFileAzure:
			m["target_account"] = cfg.Target.Data["account"]
			m["target_container"] = cfg.Target.Data["container"]
		}
	}

	// pass env values
	for k, v := range cfg.Env {
		if _, found := m[k]; !found && v != "" {
			m[k] = v
		}
	}

	// check that no value is blank
	blankKeys := []string{}
	for k, v := range m {
		if cast.ToString(v) == "" {
			blankKeys = append(blankKeys, k)
		}
	}

	if len(blankKeys) > 0 {
		// return g.Error("blank values for: %s", strings.Join(blankKeys, ", "))
		g.Warn("Could not successfully get format values. Blank values for: %s", strings.Join(blankKeys, ", "))
	}

	now := time.Now()

	// nested formatting for jmespath lookup
	nm := map[string]map[string]any{
		"timestamp": {
			"file_name": now.Format("2006_01_02_150405"),
			"rfc3339":   now.Format(time.RFC3339),
			"date":      now.Format(time.DateOnly),
			"datetime":  now.Format(time.DateTime),
		},
		"source": {},
		"target": {},
		"stream": {},
		"object": {},
	}

	// apply date variables
	for k, v := range iop.GetISO8601DateMap(now) {
		m[k] = v
		nm["timestamp"][k] = v
	}

	for origKey, v := range m {
		for _, key := range lo.Keys(nm) {
			if strings.HasPrefix(origKey, key+"_") {
				nm[key][strings.TrimPrefix(origKey, key+"_")] = v
			}
		}
	}

	// copy over (keep old for legacy)
	for k, v := range nm {
		m[k] = v
	}

	// log format map
	g.Trace("object format map: %s", g.Marshal(m))

	return
}

// Config is the new config struct
type Config struct {
	Source     Source            `json:"source,omitempty" yaml:"source,omitempty"`
	Target     Target            `json:"target" yaml:"target"`
	Mode       Mode              `json:"mode,omitempty" yaml:"mode,omitempty"`
	Transforms any               `json:"transforms,omitempty" yaml:"transforms,omitempty"`
	Options    ConfigOptions     `json:"options,omitempty" yaml:"options,omitempty"`
	Env        map[string]string `json:"env,omitempty" yaml:"env,omitempty"`

	StreamName        string                   `json:"stream_name,omitempty" yaml:"stream_name,omitempty"`
	ReplicationStream *ReplicationStreamConfig `json:"replication_stream,omitempty" yaml:"replication_stream,omitempty"`

	SrcConn  connection.Connection `json:"-" yaml:"-"`
	TgtConn  connection.Connection `json:"-" yaml:"-"`
	Prepared bool                  `json:"-" yaml:"-"`

	IncrementalVal string `json:"incremental_val" yaml:"incremental_val"`
	IncrementalGTE bool   `json:"incremental_gte,omitempty" yaml:"incremental_gte,omitempty"`

	MetadataLoadedAt  *bool `json:"-" yaml:"-"`
	MetadataStreamURL bool  `json:"-" yaml:"-"`
	MetadataRowNum    bool  `json:"-" yaml:"-"`
	MetadataRowID     bool  `json:"-" yaml:"-"`
	MetadataExecID    bool  `json:"-" yaml:"-"`

	extraTransforms []string `json:"-" yaml:"-"`
}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (cfg *Config) Scan(value interface{}) error {
	return g.JSONScanner(cfg, value)
}

// ReplicationMode returns true for replication mode
func (cfg *Config) ReplicationMode() bool {
	return cfg.ReplicationStream != nil
}

// IgnoreExisting returns true target_options.ignore_existing is true
func (cfg *Config) IgnoreExisting() bool {
	return cfg.Target.Options.IgnoreExisting != nil && *cfg.Target.Options.IgnoreExisting
}

// HasIncrementalVal returns true there is a non-null incremental value
func (cfg *Config) HasIncrementalVal() bool {
	return cfg.IncrementalVal != "" && cfg.IncrementalVal != "null"
}

// ColumnsPrepared returns the prepared columns
func (cfg *Config) ColumnsPrepared() (columns iop.Columns) {

	if cfg.Target.Columns != nil {
		switch colsCasted := cfg.Target.Columns.(type) {
		case map[string]any:
			for colName, colType := range colsCasted {
				col := iop.Column{
					Name: colName,
					Type: iop.ColumnType(cast.ToString(colType)),
				}
				columns = append(columns, col)
			}
		case map[any]any:
			for colName, colType := range colsCasted {
				col := iop.Column{
					Name: cast.ToString(colName),
					Type: iop.ColumnType(cast.ToString(colType)),
				}
				columns = append(columns, col)
			}
		case []map[string]any:
			for _, colItem := range colsCasted {
				col := iop.Column{}
				g.Unmarshal(g.Marshal(colItem), &col)
				columns = append(columns, col)
			}
		case []any:
			for _, colItem := range colsCasted {
				col := iop.Column{}
				g.Unmarshal(g.Marshal(colItem), &col)
				columns = append(columns, col)
			}
		case iop.Columns:
			columns = colsCasted
		default:
			g.Warn("Config.Source.Options.Columns not handled: %T", cfg.Source.Options.Columns)
		}

		// parse constraint, length, precision, scale
		for i := range columns {
			columns[i].SetConstraint()
			columns[i].SetLengthPrecisionScale()
		}

		// set as string so that StreamProcessor parses it
		columns = iop.NewColumns(columns...)
	}

	return
}

// TransformsPrepared returns the transforms columns
func (cfg *Config) TransformsPrepared() (colTransforms map[string][]string) {

	if transforms := cfg.Transforms; transforms != nil {
		colTransforms = map[string][]string{}

		makeTransformArray := func(val any) []string {
			switch tVal := val.(type) {
			case []any:
				transformsArray := make([]string, len(tVal))
				for i := range tVal {
					transformsArray[i] = cast.ToString(tVal[i])
				}
				return transformsArray
			case []string:
				return tVal
			default:
				g.Warn("did not handle transforms value input: %#v", val)
			}
			return nil
		}

		switch tVal := transforms.(type) {
		case []any, []string:
			colTransforms["*"] = makeTransformArray(tVal)
		case map[string]any:
			for k, v := range tVal {
				colTransforms[k] = makeTransformArray(v)
			}
		case map[any]any:
			for k, v := range tVal {
				colTransforms[cast.ToString(k)] = makeTransformArray(v)
			}
		case map[string][]string:
			for k, v := range tVal {
				colTransforms[k] = makeTransformArray(v)
			}
		case map[string][]any:
			for k, v := range tVal {
				colTransforms[k] = makeTransformArray(v)
			}
		case map[any][]string:
			for k, v := range tVal {
				colTransforms[cast.ToString(k)] = makeTransformArray(v)
			}
		case map[any][]any:
			for k, v := range tVal {
				colTransforms[cast.ToString(k)] = makeTransformArray(v)
			}
		default:
			g.Warn("did not handle transforms input: %#v", transforms)
		}

		for _, transf := range cfg.extraTransforms {
			if _, ok := colTransforms["*"]; !ok {
				colTransforms["*"] = []string{transf}
			} else {
				colTransforms["*"] = append(colTransforms["*"], transf)
			}
		}

	}
	return
}

// Value return json value, implement driver.Valuer interface
func (cfg Config) Value() (driver.Value, error) {
	jBytes, err := json.Marshal(cfg)
	if err != nil {
		return nil, g.Error(err, "could not marshal")
	}

	out := string(jBytes)
	out = strings.ReplaceAll(out, `,"_src_conn":{}`, ``)
	out = strings.ReplaceAll(out, `,"_tgt_conn":{}`, ``)
	out = strings.ReplaceAll(out, `,"primary_key":null`, ``)

	return []byte(out), err
}

func (cfg *Config) MD5() string {
	payload := g.Marshal([]any{
		g.M("source", cfg.Source.MD5()),
		g.M("target", cfg.Target.MD5()),
		g.M("mode", cfg.Mode),
		g.M("options", cfg.Options),
	})

	// clean up
	if strings.Contains(cfg.Source.Conn, "://") {
		payload = cleanConnURL(payload, cfg.Source.Conn)
	}

	if strings.Contains(cfg.Target.Conn, "://") {
		payload = cleanConnURL(payload, cfg.Target.Conn)
	}

	return g.MD5(payload)
}

func (cfg *Config) SrcConnMD5() string {
	return g.MD5(cfg.SrcConn.URL())
}

func (cfg *Config) TgtConnMD5() string {
	return g.MD5(cfg.TgtConn.URL())
}

func (cfg *Config) StreamID() string {
	return g.MD5(cfg.Source.Conn, cfg.Target.Conn, cfg.StreamName, cfg.Target.Object)
}

// ConfigOptions are configuration options
type ConfigOptions struct {
	Debug   bool `json:"debug,omitempty" yaml:"debug,omitempty"`
	StdIn   bool `json:"-"`                                          // whether stdin is passed
	StdOut  bool `json:"stdout,omitempty" yaml:"stdout,omitempty"`   // whether to output to stdout
	Dataset bool `json:"dataset,omitempty" yaml:"dataset,omitempty"` // whether to output to dataset
}

// Source is a source of data
type Source struct {
	Conn        string         `json:"conn,omitempty" yaml:"conn,omitempty"`
	Type        dbio.Type      `json:"type,omitempty" yaml:"type,omitempty"`
	Stream      string         `json:"stream,omitempty" yaml:"stream,omitempty"`
	Select      []string       `json:"select,omitempty" yaml:"select,omitempty"` // Select or exclude columns. Exclude with prefix "-".
	Where       string         `json:"where,omitempty" yaml:"where,omitempty"`
	Query       string         `json:"query,omitempty" yaml:"query,omitempty"`
	PrimaryKeyI any            `json:"primary_key,omitempty" yaml:"primary_key,omitempty"`
	UpdateKey   string         `json:"update_key,omitempty" yaml:"update_key,omitempty"`
	Options     *SourceOptions `json:"options,omitempty" yaml:"options,omitempty"`

	Data map[string]interface{} `json:"-" yaml:"-"`
}

func (s *Source) Limit() int {
	if val := os.Getenv("SLING_LIMIT"); val != "" {
		return cast.ToInt(val)
	}

	if s.Options.Limit == nil {
		return 0
	}
	return *s.Options.Limit
}

func (s *Source) Offset() int {
	if s.Options.Offset == nil {
		return 0
	}
	return *s.Options.Offset
}

func (s *Source) HasUpdateKey() bool {
	return s.UpdateKey != ""
}

func (s *Source) HasPrimaryKey() bool {
	return strings.Join(s.PrimaryKey(), "") != ""
}

func (s *Source) PrimaryKey() []string {
	return castKeyArray(s.PrimaryKeyI)
}

func (s *Source) MD5() string {
	payload := g.Marshal([]any{
		g.M("conn", s.Conn),
		g.M("type", s.Type),
		g.M("stream", s.Stream),
	})

	if strings.Contains(s.Conn, "://") {
		payload = cleanConnURL(payload, s.Conn)
	}

	return g.MD5(payload)
}

// Target is a target of data
type Target struct {
	Conn    string         `json:"conn,omitempty" yaml:"conn,omitempty"`
	Type    dbio.Type      `json:"type,omitempty" yaml:"type,omitempty"`
	Object  string         `json:"object,omitempty" yaml:"object,omitempty"`
	Columns any            `json:"columns,omitempty" yaml:"columns,omitempty"`
	Options *TargetOptions `json:"options,omitempty" yaml:"options,omitempty"`

	Data map[string]interface{} `json:"-" yaml:"-"`

	TmpTableCreated bool        `json:"-" yaml:"-"`
	columns         iop.Columns `json:"-" yaml:"-"`
}

func (t *Target) ObjectFileFormat() dbio.FileType {
	if t.Options != nil && t.Options.Format != dbio.FileTypeNone {
		return t.Options.Format
	}
	return filesys.InferFileFormat(t.Object)
}

func (t *Target) MD5() string {
	payload := g.Marshal([]any{
		g.M("conn", t.Conn),
		g.M("type", t.Type),
		g.M("object", t.Object),
	})

	if strings.Contains(t.Conn, "://") {
		payload = cleanConnURL(payload, t.Conn)
	}

	return g.MD5(payload)
}

// SourceOptions are connection and stream processing options
type SourceOptions struct {
	EmptyAsNull    *bool               `json:"empty_as_null,omitempty" yaml:"empty_as_null,omitempty"`
	Header         *bool               `json:"header,omitempty" yaml:"header,omitempty"`
	Flatten        *bool               `json:"flatten,omitempty" yaml:"flatten,omitempty"`
	FieldsPerRec   *int                `json:"fields_per_rec,omitempty" yaml:"fields_per_rec,omitempty"`
	Compression    *iop.CompressorType `json:"compression,omitempty" yaml:"compression,omitempty"`
	Format         *dbio.FileType      `json:"format,omitempty" yaml:"format,omitempty"`
	NullIf         *string             `json:"null_if,omitempty" yaml:"null_if,omitempty"`
	DatetimeFormat string              `json:"datetime_format,omitempty" yaml:"datetime_format,omitempty"`
	SkipBlankLines *bool               `json:"skip_blank_lines,omitempty" yaml:"skip_blank_lines,omitempty"`
	Delimiter      string              `json:"delimiter,omitempty" yaml:"delimiter,omitempty"`
	Escape         string              `json:"escape,omitempty" yaml:"escape,omitempty"`
	Quote          string              `json:"quote,omitempty" yaml:"quote,omitempty"`
	MaxDecimals    *int                `json:"max_decimals,omitempty" yaml:"max_decimals,omitempty"`
	JmesPath       *string             `json:"jmespath,omitempty" yaml:"jmespath,omitempty"`
	Sheet          *string             `json:"sheet,omitempty" yaml:"sheet,omitempty"`
	Range          *string             `json:"range,omitempty" yaml:"range,omitempty"`
	Limit          *int                `json:"limit,omitempty" yaml:"limit,omitempty"`
	Offset         *int                `json:"offset,omitempty" yaml:"offset,omitempty"`
	FileSelect     *[]string           `json:"file_select,omitempty" yaml:"file_select,omitempty"` // include/exclude files
	ParallelChunks *int                `json:"parallel_chunks,omitempty" yaml:"parallel_chunks,omitempty"`

	// columns & transforms were moved out of source_options
	// https://github.com/slingdata-io/sling-cli/issues/348
	Columns    any `json:"columns,omitempty" yaml:"columns,omitempty"`       // legacy
	Transforms any `json:"transforms,omitempty" yaml:"transforms,omitempty"` // legacy
}

// TargetOptions are target connection and stream processing options
type TargetOptions struct {
	Header           *bool               `json:"header,omitempty" yaml:"header,omitempty"`
	Compression      *iop.CompressorType `json:"compression,omitempty" yaml:"compression,omitempty"`
	Concurrency      int                 `json:"concurrency,omitempty" yaml:"concurrency,omitempty"`
	BatchLimit       *int64              `json:"batch_limit,omitempty" yaml:"batch_limit,omitempty"`
	DatetimeFormat   string              `json:"datetime_format,omitempty" yaml:"datetime_format,omitempty"`
	Delimiter        string              `json:"delimiter,omitempty" yaml:"delimiter,omitempty"`
	FileMaxRows      *int64              `json:"file_max_rows,omitempty" yaml:"file_max_rows,omitempty"`
	FileMaxBytes     *int64              `json:"file_max_bytes,omitempty" yaml:"file_max_bytes,omitempty"`
	Format           dbio.FileType       `json:"format,omitempty" yaml:"format,omitempty"`
	MaxDecimals      *int                `json:"max_decimals,omitempty" yaml:"max_decimals,omitempty"`
	UseBulk          *bool               `json:"use_bulk,omitempty" yaml:"use_bulk,omitempty"`
	IgnoreExisting   *bool               `json:"ignore_existing,omitempty" yaml:"ignore_existing,omitempty"`
	DeleteMissing    *string             `json:"delete_missing,omitempty" yaml:"delete_missing,omitempty"`
	AddNewColumns    *bool               `json:"add_new_columns,omitempty" yaml:"add_new_columns,omitempty"`
	AdjustColumnType *bool               `json:"adjust_column_type,omitempty" yaml:"adjust_column_type,omitempty"`
	ColumnCasing     *iop.ColumnCasing   `json:"column_casing,omitempty" yaml:"column_casing,omitempty"`

	TableKeys database.TableKeys `json:"table_keys,omitempty" yaml:"table_keys,omitempty"`
	TableTmp  string             `json:"table_tmp,omitempty" yaml:"table_tmp,omitempty"`
	TableDDL  *string            `json:"table_ddl,omitempty" yaml:"table_ddl,omitempty"`
	PreSQL    *string            `json:"pre_sql,omitempty" yaml:"pre_sql,omitempty"`
	PostSQL   *string            `json:"post_sql,omitempty" yaml:"post_sql,omitempty"`
}

var SourceFileOptionsDefault = SourceOptions{
	EmptyAsNull:    g.Bool(true),
	Header:         g.Bool(true),
	Flatten:        g.Bool(false),
	Compression:    iop.CompressorTypePtr(iop.AutoCompressorType),
	NullIf:         g.String("NULL"),
	DatetimeFormat: "AUTO",
	SkipBlankLines: g.Bool(false),
	// Delimiter:      ",",
	FieldsPerRec: g.Int(-1),
	MaxDecimals:  g.Int(-1),
}

var SourceDBOptionsDefault = SourceOptions{
	EmptyAsNull:    g.Bool(false),
	NullIf:         g.String("NULL"),
	DatetimeFormat: "AUTO",
	MaxDecimals:    g.Int(-1),
}

var TargetFileOptionsDefault = TargetOptions{
	Header: g.Bool(true),
	Compression: lo.Ternary(
		os.Getenv("COMPRESSION") != "",
		iop.CompressorTypePtr(iop.CompressorType(strings.ToLower(os.Getenv("COMPRESSION")))),
		iop.CompressorTypePtr(iop.AutoCompressorType),
	),
	Concurrency: lo.Ternary(
		os.Getenv("CONCURRENCY") != "",
		cast.ToInt(os.Getenv("CONCURRENCY")),
		7,
	),
	FileMaxRows: lo.Ternary(
		os.Getenv("FILE_MAX_ROWS") != "",
		g.Int64(cast.ToInt64(os.Getenv("FILE_MAX_ROWS"))),
		g.Int64(0),
	),
	FileMaxBytes: lo.Ternary(
		os.Getenv("FILE_MAX_BYTES") != "",
		g.Int64(cast.ToInt64(os.Getenv("FILE_MAX_BYTES"))),
		g.Int64(0),
	),
	Format:         dbio.FileTypeNone,
	UseBulk:        g.Bool(true),
	AddNewColumns:  g.Bool(true),
	DatetimeFormat: "auto",
	Delimiter:      ",",
	MaxDecimals:    g.Int(-1),
	ColumnCasing:   g.Ptr(iop.SourceColumnCasing),
}

var TargetDBOptionsDefault = TargetOptions{
	FileMaxRows: lo.Ternary(
		os.Getenv("FILE_MAX_ROWS") != "",
		g.Int64(cast.ToInt64(os.Getenv("FILE_MAX_ROWS"))),
		g.Int64(0),
	),
	UseBulk:          g.Bool(true),
	AddNewColumns:    g.Bool(true),
	AdjustColumnType: g.Bool(false),
	DatetimeFormat:   "auto",
	MaxDecimals:      g.Int(-1),
	ColumnCasing:     g.Ptr(iop.SourceColumnCasing),
}

func (o *SourceOptions) SetDefaults(sourceOptions SourceOptions) {

	if o == nil {
		o = &sourceOptions
	}
	if o.EmptyAsNull == nil {
		o.EmptyAsNull = sourceOptions.EmptyAsNull
	}
	if o.Header == nil {
		o.Header = sourceOptions.Header
	}
	if o.Compression == nil {
		o.Compression = sourceOptions.Compression
	}
	if o.NullIf == nil {
		o.NullIf = sourceOptions.NullIf
	}
	if o.FieldsPerRec == nil {
		o.FieldsPerRec = sourceOptions.FieldsPerRec
	}
	if o.JmesPath == nil {
		o.JmesPath = sourceOptions.JmesPath
	}
	if o.Sheet == nil {
		o.Sheet = sourceOptions.Sheet
	}
	if o.Range == nil {
		o.Range = sourceOptions.Range
	}
	if o.DatetimeFormat == "" {
		o.DatetimeFormat = sourceOptions.DatetimeFormat
	}
	if o.SkipBlankLines == nil {
		o.SkipBlankLines = sourceOptions.SkipBlankLines
	}
	if o.Delimiter == "" {
		o.Delimiter = sourceOptions.Delimiter
	}
	if o.Escape == "" {
		o.Escape = sourceOptions.Escape
	}
	if o.Quote == "" {
		o.Quote = sourceOptions.Quote
	}
	if o.MaxDecimals == nil {
		o.MaxDecimals = sourceOptions.MaxDecimals
	}
	if o.Columns == nil {
		o.Columns = sourceOptions.Columns // legacy
	}
	if o.Transforms == nil {
		o.Transforms = sourceOptions.Transforms // legacy
	}

}

func (o *TargetOptions) SetDefaults(targetOptions TargetOptions) {

	if o == nil {
		o = &targetOptions
	}
	if o.Header == nil {
		o.Header = targetOptions.Header
	}
	if o.Compression == nil {
		o.Compression = targetOptions.Compression
	}
	if o.Compression != nil {
		o.Compression = o.Compression.Normalize()
	}

	if o.Format == dbio.FileTypeNone {
		o.Format = targetOptions.Format
	}
	if o.Concurrency == 0 {
		o.Concurrency = targetOptions.Concurrency
	}
	if o.BatchLimit == nil {
		o.BatchLimit = targetOptions.BatchLimit
	}
	if o.FileMaxRows == nil {
		o.FileMaxRows = targetOptions.FileMaxRows
	}
	if o.FileMaxBytes == nil {
		o.FileMaxBytes = targetOptions.FileMaxBytes
	}
	if o.UseBulk == nil {
		o.UseBulk = targetOptions.UseBulk
	}
	if o.IgnoreExisting == nil {
		o.IgnoreExisting = targetOptions.IgnoreExisting
	}
	if o.DeleteMissing == nil {
		o.DeleteMissing = targetOptions.DeleteMissing
	}
	if o.PreSQL == nil {
		o.PreSQL = targetOptions.PreSQL
	}
	if o.PostSQL == nil {
		o.PostSQL = targetOptions.PostSQL
	}
	if o.TableTmp == "" {
		o.TableTmp = targetOptions.TableTmp
	}
	if o.TableDDL == nil {
		o.TableDDL = targetOptions.TableDDL
	}
	if o.AdjustColumnType == nil {
		o.AdjustColumnType = targetOptions.AdjustColumnType
	}

	if o.AddNewColumns == nil {
		o.AddNewColumns = targetOptions.AddNewColumns
	}
	if o.DatetimeFormat == "" {
		o.DatetimeFormat = targetOptions.DatetimeFormat
	}
	if o.Delimiter == "" {
		o.Delimiter = targetOptions.Delimiter
	}
	if o.MaxDecimals == nil {
		o.MaxDecimals = targetOptions.MaxDecimals
	}
	if o.ColumnCasing == nil {
		o.ColumnCasing = targetOptions.ColumnCasing
	}
	if o.TableKeys == nil {
		o.TableKeys = targetOptions.TableKeys
		if o.TableKeys == nil {
			o.TableKeys = database.TableKeys{}
		}
	}
}

func castKeyArray(keyI any) (key []string) {
	switch keyV := keyI.(type) {
	case nil:
		return []string{}
	case []string:
		if keyV == nil {
			return []string{}
		}
		return keyV
	case string:
		if keyV == "" {
			return []string{}
		}
		return []string{keyV}
	case *string:
		if keyV == nil || *keyV == "" {
			return []string{}
		}
		return []string{*keyV}
	case []any:
		for _, v := range keyV {
			val := cast.ToString(v)
			if val == "" {
				continue
			}
			key = append(key, val)
		}
		return key
	}
	return
}

// expandEnvVars replaces $KEY or ${KEY} with its environment variable value
// only if the variable is present in the environment.
// If not present, $KEY or ${KEY} will remain in the config text.
func expandEnvVars(text string) string {
	for key, value := range g.KVArrToMap(os.Environ()...) {
		text = strings.ReplaceAll(text, "$"+key+"", value)
		text = strings.ReplaceAll(text, "${"+key+"}", value)
	}
	return text
}

func cleanConnURL(payload, connURL string) string {
	cleanSource := strings.Split(connURL, "://")[0] + "://"
	payload = strings.ReplaceAll(payload, g.Marshal(connURL), g.Marshal(cleanSource))
	return payload
}
