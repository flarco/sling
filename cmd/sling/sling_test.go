package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jmespath/go-jmespath"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"syreclabs.com/go/faker"

	"github.com/slingdata-io/sling-cli/core/sling"

	"github.com/flarco/g"
	"github.com/flarco/g/csv"
	"github.com/flarco/g/net"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	d "github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
)

var testMux sync.Mutex
var testContext = g.NewContext(context.Background())

var conns = connection.GetLocalConns()

type testDB struct {
	name string
	conn d.Connection
}

type connTest struct {
	name      string
	schema    string
	root      string
	useBulk   *bool
	adjustCol *bool
}

var connMap = map[dbio.Type]connTest{
	dbio.TypeDbAzure:             {name: "azuresql"},
	dbio.TypeDbAzureDWH:          {name: "azuredwh"},
	dbio.TypeDbBigQuery:          {name: "bigquery"},
	dbio.TypeDbBigTable:          {name: "bigtable"},
	dbio.TypeDbClickhouse:        {name: "clickhouse", schema: "default", useBulk: g.Bool(true)},
	dbio.Type("clickhouse_http"): {name: "clickhouse_http", schema: "default", useBulk: g.Bool(true)},
	dbio.TypeDbDuckDb:            {name: "duckdb"},
	dbio.TypeDbMariaDB:           {name: "mariadb", schema: "mariadb"},
	dbio.TypeDbMotherDuck:        {name: "motherduck"},
	dbio.TypeDbMySQL:             {name: "mysql", schema: "mysql"},
	dbio.TypeDbOracle:            {name: "oracle", schema: "oracle", useBulk: g.Bool(false)},
	dbio.Type("oracle_sqlldr"):   {name: "oracle", schema: "oracle", useBulk: g.Bool(true)},
	dbio.TypeDbPostgres:          {name: "postgres"},
	dbio.TypeDbRedshift:          {name: "redshift"},
	dbio.TypeDbSnowflake:         {name: "snowflake"},
	dbio.TypeDbSQLite:            {name: "sqlite", schema: "main"},
	dbio.TypeDbD1:                {name: "d1", schema: "main"},
	dbio.TypeDbSQLServer:         {name: "mssql", schema: "dbo", useBulk: g.Bool(false)},
	dbio.Type("sqlserver_bcp"):   {name: "mssql", schema: "dbo", useBulk: g.Bool(true), adjustCol: g.Bool(false)},
	dbio.TypeDbStarRocks:         {name: "starrocks"},
	dbio.TypeDbTrino:             {name: "trino", adjustCol: g.Bool(false)},
	dbio.TypeDbMongoDB:           {name: "mongo", schema: "default"},
	dbio.TypeDbElasticsearch:     {name: "elasticsearch", schema: "default"},
	dbio.TypeDbPrometheus:        {name: "prometheus", schema: "prometheus"},
	dbio.TypeDbProton:            {name: "proton", schema: "default", useBulk: g.Bool(true)},

	dbio.TypeFileLocal:  {name: "local"},
	dbio.TypeFileSftp:   {name: "sftp"},
	dbio.TypeFileAzure:  {name: "azure_storage"},
	dbio.TypeFileS3:     {name: "aws_s3"},
	dbio.TypeFileGoogle: {name: "google_storage"},
	dbio.TypeFileFtp:    {name: "ftp_test_url"},
}

func init() {
	env.InitLogger()
	core.Version = "test"
}

func TestOptions(t *testing.T) {
	testCases := []struct {
		input       string
		shouldError bool
		expected    map[string]any
	}{
		{
			input:       ``,
			shouldError: false,
		},
		{
			input:       `.`,
			shouldError: true,
		},
		{
			input:    `{"use_bulk":false}`,
			expected: map[string]any{"use_bulk": false},
		},
		{
			input:    `{"use_bulk": false}`,
			expected: map[string]any{"use_bulk": false},
		},
		{
			input:    `{use_bulk: false}`,
			expected: map[string]any{"use_bulk": false},
		},
		{
			input:    `use_bulk: false`,
			expected: map[string]any{"use_bulk": false},
		},
		{
			input:       `{use_bulk:false}`,
			shouldError: true,
		},
		{
			input:       `use_bulk:false`,
			shouldError: true,
		},
	}

	for _, testCase := range testCases {
		options, err := parsePayload(testCase.input, true)
		msg := g.F("with input: `%s`", testCase.input)
		if testCase.shouldError {
			assert.Error(t, err, msg)
		} else {
			assert.NoError(t, err, msg)
			if testCase.expected != nil {
				assert.Equal(t, testCase.expected, options, msg)
			}
		}
	}
}

func TestCfgPath(t *testing.T) {

	testCfg := func(path string) (err error) {
		cfg, err := sling.NewConfig(path)
		if !g.AssertNoError(t, err) {
			return
		}

		assert.EqualValues(t, "testing", cfg.SrcConn.Info().Name)
		assert.EqualValues(t, "testing", cfg.Source.Stream)
		assert.EqualValues(t, "testing", cfg.SrcConn.URL())
		assert.EqualValues(t, "testing", cfg.TgtConn.Info().Name)
		assert.EqualValues(t, "testing", cfg.Target.Object)
		assert.EqualValues(t, "testing", cfg.TgtConn.URL())
		assert.EqualValues(t, "testing", cfg.Mode)
		assert.EqualValues(t, 111, cfg.Source.Limit())
		assert.EqualValues(t, "testing", cfg.Target.Options.TableDDL)
		assert.EqualValues(t, "testing", cfg.Target.Options.TableTmp)
		assert.EqualValues(t, "testing", cfg.Target.Options.PostSQL)

		return err
	}

	err := testCfg("tests/test1.yaml")
	g.AssertNoError(t, err)

	err = testCfg("tests/test1.json")
	g.AssertNoError(t, err)
}

func TestExtract(t *testing.T) {
	core.Version = "v1.0.43"

	checkUpdate(true)
	assert.NotEmpty(t, updateVersion)

	printUpdateAvailable()

	err := g.ExtractTarGz(g.UserHomeDir()+"/Downloads/sling/sling_1.0.44_darwin_all.tar.gz", g.UserHomeDir()+"/Downloads/sling")
	g.AssertNoError(t, err)
}

func testSuite(t *testing.T, connType dbio.Type, testSelect ...string) {
	defer time.Sleep(100 * time.Millisecond) // for log to flush

	if t.Failed() {
		return
	}

	conn, ok := connMap[connType]
	if !assert.True(t, ok) {
		return
	}

	templateFilePath := "tests/suite.db.template.tsv"
	if connType.IsFile() {
		templateFilePath = "tests/suite.file.template.tsv"
	}

	tempFilePath := g.F("/tmp/tests.%s.tsv", connType.String())
	folderPath := g.F("tests/suite/%s", connType.String())
	testSchema := lo.Ternary(conn.schema == "", "sling_test", conn.schema)
	testFolder := lo.Ternary(conn.schema == "", "sling_test", conn.schema)
	testTable := g.F("test1k_%s", connType.String())
	testWideFilePath, err := generateLargeDataset(300, 100, false)
	g.LogFatal(err)

	if g.In(connType, dbio.TypeFileLocal, dbio.TypeFileSftp) {
		testFolder = g.F("/tmp/%s/sling_test", connType)
	} else if g.In(connType, dbio.TypeFileFtp) {
		testFolder = g.F("tmp/%s/sling_test", connType)
	}

	// generate files
	os.RemoveAll(folderPath) // clean up
	os.MkdirAll(folderPath, 0777)

	// replace vars
	filebytes, err := os.ReadFile(templateFilePath)
	g.LogFatal(err)

	fileContent := string(filebytes)
	fileContent = strings.ReplaceAll(fileContent, "[conn]", conn.name)
	fileContent = strings.ReplaceAll(fileContent, "[schema]", testSchema)
	fileContent = strings.ReplaceAll(fileContent, "[folder]", testFolder)
	fileContent = strings.ReplaceAll(fileContent, "[table]", testTable)
	fileContent = strings.ReplaceAll(fileContent, "[wide_file_path]", testWideFilePath)

	err = os.WriteFile(tempFilePath, []byte(fileContent), 0777)
	g.LogFatal(err)

	data, err := iop.ReadCsv(tempFilePath)
	if !g.AssertNoError(t, err) {
		return
	}

	// rewrite correctly for displaying in Github
	testMux.Lock()
	dataT, err := iop.ReadCsv(templateFilePath)
	if !g.AssertNoError(t, err) {
		return
	}
	c := iop.CSV{Path: templateFilePath, Delimiter: '\t'}
	c.WriteStream(dataT.Stream())
	testMux.Unlock()

	for i, rec := range data.Records() {
		testName := cast.ToString(rec["test_name"])
		streamConfig, _ := g.UnmarshalMap(cast.ToString(rec["stream_config"]))
		sourceOptions, _ := g.UnmarshalMap(cast.ToString(rec["source_options"]))
		targetOptions, _ := g.UnmarshalMap(cast.ToString(rec["target_options"]))
		env, _ := g.UnmarshalMap(cast.ToString(rec["env"]))

		if val := cast.ToString(rec["source_primary_key"]); val != "" {
			if g.In(connType, dbio.TypeDbStarRocks) {
				val = "id" // starrocks can't have a decimal as PK
			}
			streamConfig["primary_key"] = strings.Split(val, ",")
		}
		if val := cast.ToString(rec["source_update_key"]); val != "" {
			streamConfig["update_key"] = val
		}

		if conn.useBulk != nil {
			targetOptions["use_bulk"] = *conn.useBulk
		}
		if conn.adjustCol != nil {
			targetOptions["adjust_column_type"] = *conn.adjustCol
			sourceOptions["columns"] = g.M("code", "decimal")
		}

		streamName := strings.TrimSpace(cast.ToString(rec["source_stream"]))
		streamConfig["mode"] = cast.ToString(rec["mode"])
		streamConfig["object"] = cast.ToString(rec["target_object"])
		streamConfig["source_options"] = sourceOptions
		streamConfig["target_options"] = targetOptions
		if strings.HasPrefix(streamName, "select") {
			streamConfig["sql"] = g.String(streamName)
			streamName = testName
		}
		if where, ok := sourceOptions["where"]; ok {
			streamConfig["where"] = where
		}

		replicationConfig := g.M(
			"source", cast.ToString(rec["source_conn"]),
			"target", cast.ToString(rec["target_conn"]),
			"streams", g.M(
				streamName, streamConfig,
			),
			"env", env,
		)
		configPath := filepath.Join(folderPath, g.F("%02d.%s.json", i+1, testName))
		configBytes := []byte(g.Pretty(replicationConfig))
		err = os.WriteFile(configPath, configBytes, 0777)
		g.AssertNoError(t, err)
	}

	time.Sleep(500 * time.Millisecond)
	files, _ := g.ListDir(folderPath)

	testNumbers := []int{}
	testNames := []string{}
	tns := os.Getenv("TESTS")
	if tns == "" && len(testSelect) > 0 {
		tns = testSelect[0]
	}

	if tns != "" {
		for _, tn := range strings.Split(tns, ",") {
			if strings.HasSuffix(tn, "+") {
				start := cast.ToInt(strings.TrimSuffix(tn, "+"))

				for i := range files {
					if i+1 < start {
						continue
					}
					testNumbers = append(testNumbers, i+1)
				}
			} else if parts := strings.Split(tn, "-"); len(parts) == 2 {
				start := cast.ToInt(parts[0])
				end := cast.ToInt(parts[1])
				for testNumber := start; testNumber <= end; testNumber++ {
					testNumbers = append(testNumbers, testNumber)
				}
			} else if testNumber := cast.ToInt(tn); testNumber > 0 {
				testNumbers = append(testNumbers, testNumber)
			} else {
				//  accepts strings
				testNames = append(testNames, tn)
			}
		}
	}

	for i, file := range files {
		if len(testNumbers) > 0 && !g.In(i+1, testNumbers...) {
			continue
		}
		if len(testNames) > 0 {
			found := false
			for _, testName := range testNames {
				if strings.Contains(file.Name, testName) {
					found = true
				}
			}
			if !found {
				continue
			}
		}
		t.Run(g.F("%s/%s", connType, file.RelPath), func(t *testing.T) {
			runOneTask(t, file, connType)
		})
		if t.Failed() {
			g.LogError(g.Error("Test `%s` Failed for => %s", file.Name, connType))
			testContext.Cancel()
			return
		}
	}
}

func runOneTask(t *testing.T, file g.FileItem, connType dbio.Type) {
	os.Setenv("SLING_LOADED_AT_COLUMN", "TRUE")
	os.Setenv("SLING_CHECKSUM_ROWS", "10000") // so that it errors when checksums don't match
	println()

	bars := "---------------------------"
	g.Info("%s Testing %s (%s) %s", bars, file.RelPath, connType, bars)

	if strings.Contains(file.FullPath, ".discover") {

		fileBytes, err := os.ReadFile(file.FullPath)
		if !g.AssertNoError(t, err) {
			return
		}

		m, err := g.UnmarshalMap(string(fileBytes))
		if !g.AssertNoError(t, err) {
			return
		}

		pattern, err := jmespath.Search("object", cast.ToStringMap(m["streams"])[""])
		if !g.AssertNoError(t, err) {
			return
		}

		testDiscover(t, cast.ToString(pattern), cast.ToStringMap(m["env"]), connType)
		return

	}

	replicationConfig, err := sling.LoadReplicationConfigFromFile(file.FullPath)
	if !g.AssertNoError(t, err) {
		return
	}

	err = replicationConfig.Compile(nil)
	if !g.AssertNoError(t, err) {
		return
	}

	taskCfg := replicationConfig.Tasks[0]

	var streamCfg *sling.ReplicationStreamConfig
	for _, streamCfg = range replicationConfig.Streams {
	}

	env := replicationConfig.Env

	g.Debug("replication config => %s", g.Marshal(replicationConfig))

	// validate object name
	if name, ok := env["validation_object"]; ok {
		if !assert.Equal(t, name, streamCfg.Object) {
			return
		}
	}

	if doDelete := env["delete_duck_db"]; cast.ToBool(doDelete) {
		os.Remove(strings.TrimPrefix(taskCfg.TgtConn.URL(), "duckdb://"))
	}

	// process PostSQL for different drop_view syntax
	if taskCfg.TgtConn.Type.IsDb() {
		dbConn, err := taskCfg.TgtConn.AsDatabase()
		if err == nil {
			table, _ := database.ParseTableName(taskCfg.Target.Object, dbConn.GetType())
			table.Name = strings.TrimSuffix(table.Name, "_pg") + "_vw"
			if dbConn.GetType().DBNameUpperCase() {
				table.Name = strings.ToUpper(table.Name)
			}
			viewName := table.FullName()
			dropViewSQL := g.R(dbConn.GetTemplateValue("core.drop_view"), "view", viewName)
			dropViewSQL = strings.TrimSpace(dropViewSQL)
			if preSQL := taskCfg.Target.Options.PreSQL; preSQL != nil {
				taskCfg.Target.Options.PreSQL = g.String(g.R(
					*preSQL,
					"drop_view", dropViewSQL,
				))
			}
			if postSQL := taskCfg.Target.Options.PostSQL; postSQL != nil {
				taskCfg.Target.Options.PostSQL = g.String(g.R(
					*postSQL,
					"drop_view", dropViewSQL,
				))
			}
		}
	}
	g.Debug("task config => %s", g.Marshal(taskCfg))

	task := sling.NewTask("", taskCfg)
	if g.AssertNoError(t, task.Err) {
		task.Context = g.NewContext(testContext.Ctx)
		err = task.Execute()
		if !g.AssertNoError(t, err) {
			return
		}
		task.Config.SrcConn.Close()
		task.Config.TgtConn.Close()
	} else {
		return
	}

	defer taskCfg.TgtConn.Close()

	if valRowCountVal := cast.ToString(env["validation_stream_row_count"]); valRowCountVal != "" {
		taskCount := cast.ToInt(task.GetCount())
		if strings.HasPrefix(valRowCountVal, ">") {
			valRowCount := cast.ToInt(strings.TrimPrefix(valRowCountVal, ">"))
			assert.Greater(t, taskCount, valRowCount, "validation_stream_row_count (%s)", file.Name)
		} else {
			valRowCount := cast.ToInt(valRowCountVal)
			assert.EqualValues(t, valRowCount, taskCount, "validation_stream_row_count (%s)", file.Name)
		}
	}

	// validate count
	if valRowCountVal := cast.ToString(env["validation_row_count"]); valRowCountVal != "" {
		g.Debug("getting count for validation_row_count")
		conn, err := taskCfg.TgtConn.AsDatabase()
		if g.AssertNoError(t, err) {
			countU, err := conn.GetCount(taskCfg.Target.Object)
			g.AssertNoError(t, err)
			count := cast.ToInt(countU)
			if strings.HasPrefix(valRowCountVal, ">") {
				valRowCount := cast.ToInt(strings.TrimPrefix(valRowCountVal, ">"))
				assert.Greater(t, count, valRowCount, "validation_row_count (%s)", file.Name)
			} else {
				valRowCount := cast.ToInt(valRowCountVal)
				assert.EqualValues(t, valRowCount, count, "validation_row_count (%s)", file.Name)
			}
		}
	} else if taskCfg.Mode == sling.FullRefreshMode && taskCfg.TgtConn.Type.IsDb() {
		g.Debug("getting count for test validation")
		conn, err := taskCfg.TgtConn.AsDatabase()
		if g.AssertNoError(t, err) {
			count, err := conn.GetCount(taskCfg.Target.Object)
			g.AssertNoError(t, err)
			assert.EqualValues(t, cast.ToInt(task.GetCount()), cast.ToInt(count))
			conn.Close()
		}
	}

	// validate file
	if val, ok := env["validation_file"]; ok {
		valFile := strings.TrimPrefix(cast.ToString(val), "file://")
		dataFile, err := iop.ReadCsv(valFile)
		if !g.AssertNoError(t, err) {
			return
		}
		orderByStr := "1"
		if len(taskCfg.Source.PrimaryKey()) > 0 {
			orderByStr = strings.Join(taskCfg.Source.PrimaryKey(), ", ")
		}
		sql := g.F("select * from %s order by %s", taskCfg.Target.Object, orderByStr)
		conn, _ := taskCfg.TgtConn.AsDatabase()
		dataDB, err := conn.Query(sql)
		g.AssertNoError(t, err)
		conn.Close()
		valCols := strings.Split(cast.ToString(env["validation_cols"]), ",")

		if g.AssertNoError(t, err) {
			for _, valColS := range valCols {
				valCol := cast.ToInt(valColS)
				valuesFile := dataFile.ColValues(valCol)
				valuesDb := dataDB.ColValues(valCol)

				// clickhouse fails regularly due to some local contention, unable to pin down
				if g.In(connType, dbio.TypeDbClickhouse, dbio.Type("clickhouse_http")) && len(valuesFile) == 1002 && len(valuesDb) == 1004 && runtime.GOOS != "darwin" {
					continue
				}

				if assert.Equal(t, len(valuesFile), len(valuesDb), file.Name) {
					for i := range valuesDb {
						valDb := dataDB.Sp.ParseString(cast.ToString(valuesDb[i]))
						valFile := dataDB.Sp.ParseString(cast.ToString(valuesFile[i]))
						msg := g.F("row %d, col %d (%s vs %s), in test %s => %#v vs %#v", i+1, valCol, dataFile.Columns[valCol].Name, dataDB.Columns[valCol].Name, file.Name, valDb, valFile)
						if !assert.EqualValues(t, valFile, valDb, msg) {
							g.Warn(msg)
							return
						}
					}
				} else {
					break
				}
			}
		}
	}

	// validate target column types in database
	if payload, ok := env["validation_types"]; ok {

		correctTypeMap := map[string]iop.ColumnType{}
		err = g.Unmarshal(g.Marshal(payload), &correctTypeMap)
		if !g.AssertNoError(t, err) {
			g.Warn("%#v", payload)
			return
		}

		// get column level types
		opt := connection.DiscoverOptions{
			Level:   d.SchemataLevelColumn,
			Pattern: taskCfg.Target.Object,
		}
		_, nodes, schemata, err := taskCfg.TgtConn.Discover(&opt)
		var columns iop.Columns
		if g.AssertNoError(t, err) {
			if taskCfg.TgtConn.Type.IsDb() {
				for _, table := range schemata.Tables() {
					columns = table.Columns
					break
				}
			}
			if taskCfg.TgtConn.Type.IsFile() {
				for _, node := range nodes {
					columns = node.Columns
					break
				}
			}
		} else {
			return
		}

		failed := false
		srcType := taskCfg.SrcConn.Type
		tgtType := taskCfg.TgtConn.Type
		isMySQLLike := func(t dbio.Type) bool {
			return t == dbio.TypeDbMySQL || t == dbio.TypeDbMariaDB || t == dbio.TypeDbStarRocks
		}

		for colName, correctType := range correctTypeMap {
			// skip those
			if g.In(srcType, dbio.TypeDbMongoDB) || g.In(tgtType, dbio.TypeDbMongoDB) {
				continue
			}

			// correct correctType
			switch {
			case isMySQLLike(srcType) && tgtType == dbio.TypeDbPostgres:
				if strings.EqualFold(colName, "target") {
					correctType = iop.TextType // mysql/mariadb doesn't have bool
				}
				if strings.EqualFold(colName, "update_dt") {
					correctType = iop.TimestampType // mysql/mariadb doesn't have time zone
				}
				if srcType == dbio.TypeDbMariaDB && strings.EqualFold(colName, "json_data") {
					correctType = iop.TextType // mariadb's `json` type is `longtext`
				}
				if srcType == dbio.TypeDbStarRocks && strings.EqualFold(colName, "json_data") {
					correctType = iop.TextType // starrocks's `json` type is `varchar(65500)`
				}
			case isMySQLLike(tgtType):
				if srcType == dbio.TypeDbPostgres && strings.EqualFold(colName, "target") {
					correctType = iop.TextType // mysql/mariadb doesn't have bool
				}
				if correctType == iop.BoolType {
					correctType = iop.StringType // mysql/mariadb doesn't have bool
				}
				if g.In(correctType, iop.TimestampType, iop.TimestampzType) {
					correctType = iop.DatetimeType // mysql/mariadb uses datetime
				}
				if tgtType == dbio.TypeDbMariaDB && strings.EqualFold(colName, "json_data") {
					correctType = iop.TextType // mariadb's `json` type is `longtext`
				}
			case tgtType == dbio.TypeDbSQLite || srcType == dbio.TypeDbSQLite:
				if correctType.IsDatetime() || correctType.IsDate() {
					correctType = iop.TextType // sqlite uses text for timestamps
				}
			case tgtType == dbio.TypeDbD1 || srcType == dbio.TypeDbD1:
				if correctType.IsDatetime() || correctType.IsDate() {
					correctType = iop.TextType // d1 (sqlite) uses text for timestamps
				}
			case tgtType == dbio.TypeDbOracle:
				if srcType == dbio.TypeDbPostgres && strings.EqualFold(colName, "target") {
					correctType = iop.StringType // oracle doesn't have bool
				}
				if srcType == dbio.TypeDbPostgres && strings.EqualFold(colName, "date") {
					correctType = iop.TimestampType /// oracle uses datetime for date
				}
				if correctType.IsDate() {
					correctType = iop.DatetimeType // oracle uses datetime for date
				}
				if correctType == iop.BoolType {
					correctType = iop.StringType // oracle doesn't have bool
				}
				if correctType == iop.TimestampzType {
					correctType = iop.TimestampType // oracle uses timestampz
				}
				if correctType == iop.JsonType {
					correctType = iop.TextType // oracle uses clob for json
				}
			case srcType == dbio.TypeDbOracle && tgtType == dbio.TypeDbPostgres:
				if correctType.IsDate() {
					correctType = iop.TimestampType // oracle uses datetime for date
				}
				if correctType == iop.BoolType {
					correctType = iop.TextType // oracle doesn't have bool
				}
				if correctType == iop.JsonType {
					correctType = iop.TextType // oracle uses clob for json
				}
			case tgtType == dbio.TypeDbSQLServer:
				if correctType == iop.TimestampType {
					correctType = iop.DatetimeType // sqlserver uses datetime
				}
				if correctType == iop.BoolType {
					correctType = iop.TextType // sqlserver doesn't have bool
				}
				if correctType == iop.JsonType {
					correctType = iop.TextType // sqlserver uses varchar(max) for json
				}
			case tgtType == dbio.TypeDbRedshift:
				if correctType == iop.JsonType {
					correctType = iop.TextType // redshift uses text for json
				}
			case srcType == dbio.TypeDbRedshift && tgtType == dbio.TypeDbPostgres:
				if correctType == iop.JsonType {
					correctType = iop.TextType // redshift uses text for json
				}
			case srcType == dbio.TypeDbSQLServer && tgtType == dbio.TypeDbPostgres:
				if correctType == iop.BoolType {
					correctType = iop.TextType // sqlserver doesn't have bool
				}
				if correctType == iop.JsonType {
					correctType = iop.TextType // sqlserver uses varchar(max) for json
				}
			case tgtType == dbio.TypeDbMongoDB:
				if correctType == iop.TimestampType {
					correctType = iop.DateType
				}
			case srcType == dbio.TypeDbMongoDB && tgtType == dbio.TypeDbPostgres:
				if correctType == iop.TimestampType {
					correctType = iop.DateType
				}
			case tgtType == dbio.TypeDbBigQuery:
				if correctType == iop.TimestampzType {
					correctType = iop.TimestampType // bigquery doesn't have timestampz
				}
			case srcType == dbio.TypeDbBigQuery && tgtType == dbio.TypeDbPostgres:
				if correctType == iop.TimestampzType {
					correctType = iop.TimestampType // bigquery doesn't have timestampz
				}
			case tgtType == dbio.TypeDbTrino:
				if correctType == iop.TimestampzType {
					correctType = iop.TimestampType // trino doesn't have timestampz
				}
			case tgtType == dbio.TypeDbClickhouse:
				if correctType == iop.TimestampType || correctType == iop.TimestampzType {
					correctType = iop.DatetimeType // clickhouse uses datetime
				}
				if correctType == iop.BoolType {
					correctType = iop.TextType // clickhouse doesn't have bool
				}
				if correctType == iop.JsonType {
					correctType = iop.TextType // clickhouse uses varchar(max) for json
				}
			case srcType == dbio.TypeDbClickhouse && tgtType == dbio.TypeDbPostgres:
				if correctType == iop.BoolType {
					correctType = iop.TextType // clickhouse doesn't have bool
				}
				if correctType == iop.JsonType {
					correctType = iop.TextType // clickhouse uses varchar(max) for json
				}
				if correctType == iop.TimestampzType {
					correctType = iop.TimestampType // clickhouse uses datetime
				}
			}

			col := columns.GetColumn(colName)
			if assert.NotEmpty(t, col, "missing column: %s", colName) {
				if !assert.Equal(t, correctType, col.Type, "column type must match for %s", col.Name) {
					failed = true
				}
			}
		}

		if failed {
			g.Warn("actual column types: " + g.Marshal(columns.Types()))
		}
	}
}

func TestSuiteDatabasePostgres(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbPostgres)
}

func TestSuiteDatabaseRedshift(t *testing.T) {
	t.Skip()
	os.Setenv("SAMPLE_SIZE", "1200") // adjust_column_type does not work in Redshift
	testSuite(t, dbio.TypeDbRedshift)
}

func TestSuiteDatabaseStarRocks(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbStarRocks)
}

func TestSuiteDatabaseMySQL(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbMySQL)
}

func TestSuiteDatabaseMariaDB(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbMariaDB)
}

func TestSuiteDatabaseOracle(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbOracle)
	if _, err := exec.LookPath("sqlldr"); err == nil {
		testSuite(t, dbio.Type("oracle_sqlldr"), "1-5")
	}
}

// func TestSuiteDatabaseBigTable(t *testing.T) {
// 	t.Parallel()
// 	testSuite(t, dbio.TypeDbBigTable)
// }

func TestSuiteDatabaseBigQuery(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbBigQuery)
}

func TestSuiteDatabaseSnowflake(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbSnowflake)
}

func TestSuiteDatabaseSQLite(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbSQLite)
}

func TestSuiteDatabaseD1(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbD1, "1-5,7-9,11+") // skip wide tests
}

func TestSuiteDatabaseDuckDb(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbDuckDb)
}

func TestSuiteDatabaseMotherDuck(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbMotherDuck)
}

func TestSuiteDatabaseSQLServer(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbSQLServer)
	if _, err := exec.LookPath("bcp"); err == nil {
		testSuite(t, dbio.Type("sqlserver_bcp"))
	}
}

// func TestSuiteDatabaseAzure(t *testing.T) {
// 	t.Parallel()
// 	testSuite(t, dbio.TypeDbAzure)
// }

// func TestSuiteDatabaseAzureDWH(t *testing.T) {
// 	t.Parallel()
// 	testSuite(t, dbio.TypeDbAzureDWH)
// }

func TestSuiteDatabaseClickhouse(t *testing.T) {
	if cast.ToBool(os.Getenv("SKIP_CLICKHOUSE")) {
		return
	}
	t.Parallel()
	testSuite(t, dbio.TypeDbClickhouse)
	testSuite(t, dbio.Type("clickhouse_http"))
}

func TestSuiteDatabaseProton(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbProton, "discover_schemas")
}

func TestSuiteDatabaseTrino(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbTrino, "csv_full_refresh,discover_table,table_full_refresh_into_postgres,table_full_refresh_from_postgres,discover_schemas")
}

func TestSuiteDatabaseMongo(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbMongoDB, "table_full_refresh_into_postgres,discover_schemas")
}

func TestSuiteDatabasePrometheus(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeDbPrometheus, "discover_schemas")
}

// generate large dataset or use cache
func generateLargeDataset(numCols, numRows int, force bool) (path string, err error) {
	testMux.Lock()
	defer testMux.Unlock()

	var data iop.Dataset

	path = g.F("/tmp/dataset_%dx%d.csv", numRows, numCols)
	if _, err = os.Stat(path); err == nil && !force {
		return
	}

	g.Info("generating %s", path)

	type FakeField struct {
		name string
		gen  func() interface{}
	}

	words := make([]string, 100)
	for i := range words {
		words[i] = g.RandString(g.AlphaRunes, g.RandInt(15))
		if len(words[i]) == 10 {
			words[i] = words[i] + "\n"
		}
	}

	fieldsFuncTemplate := []*FakeField{
		{"name", func() interface{} { return faker.Name().Name() }},
		{"url", func() interface{} { return faker.Internet().Url() }},
		{"date_time", func() interface{} { return faker.Date().Forward(100 * time.Minute).Format("2006-01-02 15:04:05") }},
		{"address", func() interface{} { return faker.Address().SecondaryAddress() }},
		{"price", func() interface{} { return faker.Commerce().Price() }},
		{"my_int", func() interface{} { return faker.Number().NumberInt64(5) }},
		{"email", func() interface{} { return faker.Internet().Email() }},
		{"creditcardexpirydate", func() interface{} { return faker.Date().Forward(1000000 * time.Minute).Format("2006-01-02") }},
		{"latitude", func() interface{} { return faker.Address().Latitude() }},
		{"longitude", func() interface{} { return faker.Address().Longitude() }},
		{"interested", func() interface{} { return g.RandInt(2) == 1 }},
		{"paragraph", func() interface{} { return strings.Join(words[:g.RandInt(100)], " ") }},
	}

	fieldsFunc := make([]*FakeField, numCols)
	for i := range fieldsFunc {
		fieldsFunc[i] = fieldsFuncTemplate[i%len(fieldsFuncTemplate)]
	}

	makeRow := func() (row []interface{}) {
		row = make([]interface{}, len(fieldsFunc))
		c := 0
		for _, ff := range fieldsFunc {
			row[c] = ff.gen()
			c++
		}

		return row
	}

	getFields := func() (fields []string) {
		fields = make([]string, len(fieldsFunc))
		for j, ff := range fieldsFunc {
			fields[j] = g.F("%s_%03d", (*ff).name, j+1)
		}
		return
	}

	data = iop.NewDataset(nil)
	data.Rows = make([][]interface{}, numRows)
	data.SetFields(getFields())

	for i := 0; i < 50; i++ {
		data.Rows[i] = makeRow()
	}

	file, _ := os.Create(path)

	w := csv.NewWriter(file)
	defer w.Flush()

	tbw, err := w.Write(append([]string{"id"}, data.GetFields()...))
	if err != nil {
		return path, g.Error(err, "error write row to csv file")
	}

	nullCycle := 0
	for i := 0; i < numRows; i++ {
		rec := make([]string, numCols+1)
		rec[0] = cast.ToString(i + 1) // id
		for j := 1; j < numCols+1; j++ {
			nullCycle++
			if nullCycle%100 == 0 {
				rec[j] = ""
			} else {
				val := data.Rows[i%50][j-1]
				rec[j] = data.Sp.CastToString(j, val, data.Columns[j].Type)
			}
		}

		bw, err := w.Write(rec)
		if err != nil {
			return path, g.Error(err, "error write row to csv file")
		}
		tbw = tbw + bw

		if i%100000 == 0 {
			print(g.F("%d ", i))
		}
	}

	println()

	return path, nil

}

func TestGenerateWideFile(t *testing.T) {
	_, err := generateLargeDataset(300, 100, true)
	g.LogFatal(err)
}

func TestReplicationDefaults(t *testing.T) {
	replicationCfg := `
source: local
target: postgres

defaults:
  mode: full-refresh
  object: my_schema1.table1
  select: [col1, col2, col3]
  primary_key: [col1, col2]
  update_key: col3
  source_options:
    delimiter: ","
  target_options:
    file_max_rows: 500000
    add_new_columns: false
    post_sql: some sql

streams:
  stream_0: {}

  stream_1: 
    mode: incremental
    object: my_schema2.table2
    select: [col1]
    primary_key: col3
    update_key: col2
    source_options:
			columns: { pro: 'decimal(10,4)', pro2: 'string' }
      delimiter: "|"
			transforms: [trim_space]
    target_options:
      file_max_rows: 600000
      add_new_columns: true

  stream_2: 
    select: []
    primary_key: []
    update_key: null
		columns: { id: 'string(100)' }
		transforms: [trim_space]
    target_options:
      file_max_rows: 0
      post_sql: ""
    disabled: true

  file://tests/files/parquet/*.parquet:
		
		single: true
    object: my_schema3.table3

  file://tests/files/*.csv:
    object: my_schema3.table3
`
	replication, err := sling.LoadReplicationConfig(strings.ReplaceAll(replicationCfg, "\t", "  "))
	if !g.AssertNoError(t, err) {
		return
	}

	err = replication.Compile(nil)
	if !g.AssertNoError(t, err) {
		return
	}

	if !assert.GreaterOrEqual(t, len(replication.Tasks), 5) {
		streams := []string{}
		for _, task := range replication.Tasks {
			streams = append(streams, task.StreamName)
		}
		g.Warn(g.F("streams: %#v", streams))
		return
	}

	{
		// First Stream: stream_0
		config := replication.Tasks[0]
		config.SetDefault()
		assert.Equal(t, sling.FullRefreshMode, config.Mode)
		assert.Equal(t, "local", config.Source.Conn)
		assert.Equal(t, "stream_0", config.Source.Stream)
		assert.Equal(t, []string{"col1", "col2", "col3"}, config.Source.Select)
		assert.Equal(t, []string{"col1", "col2"}, config.Source.PrimaryKey())
		assert.Equal(t, "col3", config.Source.UpdateKey)
		assert.Equal(t, ",", config.Source.Options.Delimiter)

		assert.Equal(t, "postgres", config.Target.Conn)
		assert.Equal(t, g.Bool(false), config.Target.Options.AddNewColumns)
		assert.EqualValues(t, g.Int64(500000), config.Target.Options.FileMaxRows)
		assert.EqualValues(t, false, config.ReplicationStream.Disabled)
	}

	{
		// Second Stream: stream_1
		config := replication.Tasks[1]
		config.SetDefault()
		assert.Equal(t, sling.IncrementalMode, config.Mode)
		assert.Equal(t, "stream_1", config.Source.Stream)
		assert.Equal(t, []string{"col1"}, config.Source.Select)
		assert.Equal(t, []string{"col3"}, config.Source.PrimaryKey())
		assert.Equal(t, "col2", config.Source.UpdateKey)
		assert.Equal(t, "|", config.Source.Options.Delimiter)
		assert.Equal(t, "[{\"name\":\"pro\",\"type\":\"decimal(10,4)\"},{\"name\":\"pro2\",\"type\":\"string\"}]", g.Marshal(config.Target.Columns))
		assert.Equal(t, `["trim_space"]`, g.Marshal(config.Transforms))

		assert.Equal(t, `"my_schema2"."table2"`, config.Target.Object)
		assert.Equal(t, g.Bool(true), config.Target.Options.AddNewColumns)
		assert.EqualValues(t, g.Int64(600000), config.Target.Options.FileMaxRows)
		assert.EqualValues(t, g.String("some sql"), config.Target.Options.PostSQL)
		assert.EqualValues(t, false, config.ReplicationStream.Disabled)
	}

	{
		// Third Stream: stream_2
		config := replication.Tasks[2]
		config.SetDefault()
		assert.Equal(t, "stream_2", config.Source.Stream)
		assert.Equal(t, []string{}, config.Source.Select)
		assert.Equal(t, []string{}, config.Source.PrimaryKey())
		assert.Equal(t, "", config.Source.UpdateKey)
		assert.EqualValues(t, g.Int64(0), config.Target.Options.FileMaxRows)
		assert.EqualValues(t, g.String(""), config.Target.Options.PostSQL)
		assert.EqualValues(t, true, config.ReplicationStream.Disabled)
		assert.Equal(t, "[{\"name\":\"id\",\"type\":\"string(100)\"}]", g.Marshal(config.Target.Columns))
		assert.Equal(t, `["trim_space"]`, g.Marshal(config.Transforms))
	}

	{
		// Fourth Stream: file://tests/files/parquet/*.parquet
		// single, wildcard not expanded
		config := replication.Tasks[3]
		config.SetDefault()
		assert.Equal(t, config.Source.Stream, "file://tests/files/parquet/*.parquet")
		assert.Equal(t, `"my_schema3"."table3"`, config.Target.Object)
	}

	{
		// Fifth Stream: file://tests/files/*.csv
		// wildcard expanded
		config := replication.Tasks[4]
		assert.True(t, strings.HasPrefix(config.Source.Stream, "file://tests/files/"))
		assert.NotEqual(t, config.Source.Stream, "tests/files/*.csv")
		assert.Equal(t, `"my_schema3"."table3"`, config.Target.Object)
		// g.Info(g.Pretty(config))
	}

	// g.Debug(g.Pretty(taskConfigs))
}

func Test1Replication(t *testing.T) {
	sling.ShowProgress = false
	os.Setenv("DEBUG", "LOW")
	os.Setenv("SLING_CLI", "TRUE")
	os.Setenv("SLING_LOADED_AT_COLUMN", "TRUE")
	os.Setenv("CONCURRENCY_LIMIT", "2")
	replicationCfgPath := "tests/replications/r.test.yaml"
	err := runReplication(replicationCfgPath, nil)
	if g.AssertNoError(t, err) {
		return
	}
}

func Test1Task(t *testing.T) {
	os.Setenv("SLING_CLI", "TRUE")
	config := &sling.Config{}
	cfgStr := `
source:
  conn: duckdb
  stream: main.call_center
target:
  conn: starrocks
  object: public.call_center
mode: full-refresh
`
	err := config.Unmarshal(cfgStr)
	if g.AssertNoError(t, err) {
		err = config.Prepare()
		if g.AssertNoError(t, err) {

			task := sling.NewTask("", config)
			g.AssertNoError(t, task.Err)

			// run task
			err = task.Execute()
			g.AssertNoError(t, err)
		}
	}
}

func testDiscover(t *testing.T, pattern string, env map[string]any, connType dbio.Type) {

	conn := connMap[connType]

	opt := connection.DiscoverOptions{
		Pattern:   pattern,
		Level:     database.SchemataLevel(cast.ToString(env["level"])),
		Recursive: cast.ToBool(env["recursive"]),
	}

	if g.In(connType, dbio.TypeFileLocal, dbio.TypeFileSftp) && opt.Pattern == "" {
		opt.Pattern = g.F("/tmp/%s/", connType)
	}
	if g.In(connType, dbio.TypeFileFtp) && opt.Pattern == "" {
		opt.Pattern = g.F("tmp/%s/", connType)
	}

	g.Info("sling conns discover %s %s", conn.name, g.Marshal(opt))
	files, schemata, err := conns.Discover(conn.name, &opt)
	if !g.AssertNoError(t, err) {
		return
	}

	valContains := strings.Split(cast.ToString(env["validation_contains"]), ",")
	valNotContains := strings.Split(cast.ToString(env["validation_not_contains"]), ",")
	valContains = lo.Filter(valContains, func(v string, i int) bool { return v != "" })
	valNotContains = lo.Filter(valNotContains, func(v string, i int) bool { return v != "" })

	valRowCount := cast.ToInt(env["validation_row_count"])
	valRowCountMin := -1
	if val := cast.ToString(env["validation_row_count"]); strings.HasPrefix(val, ">") {
		valRowCountMin = cast.ToInt(strings.TrimPrefix(val, ">"))
	}

	containsMap := map[string]bool{}
	for _, word := range append(valContains, valNotContains...) {
		if word == "" {
			continue
		}
		containsMap[word] = false
	}

	if connType.IsDb() {
		schemas := lo.Values(schemata.Database().Schemas)
		tables := lo.Values(schemata.Tables())
		columns := iop.Columns(lo.Values(schemata.Columns()))
		if valRowCount > 0 {
			switch opt.Level {
			case d.SchemataLevelSchema:
				assert.Equal(t, valRowCount, len(schemas), lo.Keys(schemata.Database().Schemas))
			case d.SchemataLevelTable:
				assert.Equal(t, valRowCount, len(tables), lo.Keys(schemata.Tables()))
			case d.SchemataLevelColumn:
				assert.Equal(t, valRowCount, len(columns), columns.Names())
			}
		} else {
			switch opt.Level {
			case d.SchemataLevelSchema:
				assert.Greater(t, len(schemas), 0)
			case d.SchemataLevelTable:
				assert.Greater(t, len(tables), 0)
			case d.SchemataLevelColumn:
				assert.Greater(t, len(columns), 0)
			}
		}

		if valRowCountMin > -1 {
			switch opt.Level {
			case d.SchemataLevelSchema:
				assert.Greater(t, len(schemas), valRowCountMin, lo.Keys(schemata.Database().Schemas))
			case d.SchemataLevelTable:
				assert.Greater(t, len(tables), valRowCountMin, lo.Keys(schemata.Tables()))
			case d.SchemataLevelColumn:
				assert.Greater(t, len(columns), valRowCountMin, columns.Names())
			}
		}

		if len(containsMap) > 0 {
			var resultType string
			switch opt.Level {
			case d.SchemataLevelSchema:
				resultType = "schemas"
				for _, schema := range schemas {
					for word := range containsMap {
						if strings.EqualFold(word, schema.Name) {
							containsMap[word] = true
						}
					}
				}

			case d.SchemataLevelTable:
				resultType = "tables"
				for _, table := range tables {
					for word := range containsMap {
						if strings.EqualFold(word, table.Name) {
							containsMap[word] = true
						}
					}
				}

			case d.SchemataLevelColumn:
				resultType = "columns"
				for _, col := range columns {
					for word := range containsMap {
						if strings.EqualFold(word, col.Name) {
							containsMap[word] = true
						}
					}
				}
			}

			for _, word := range valContains {
				found := containsMap[word]
				assert.True(t, found, "did not find '%s' in %s for %s", word, resultType, connType)
			}

			for _, word := range valNotContains {
				found := containsMap[word]
				assert.False(t, found, "found '%s' in %s for %s", word, resultType, connType)
			}
		}
	}

	if connType.IsFile() {
		g.Debug("returned into test: " + g.Marshal(files.Paths()))
		// basic tests
		assert.Greater(t, len(files), 0)
		for _, uri := range files.URIs() {
			u, err := net.NewURL(uri)
			if !g.AssertNoError(t, err) {
				break
			}

			if !g.In(connType, dbio.TypeFileSftp) {
				if !assert.NotContains(t, strings.TrimPrefix(uri, u.U.Scheme+"://"), "//") {
					break
				}
			}
		}

		if len(files) == 0 {
			return
		}

		columns := iop.Columns(lo.Values(files.Columns()))

		if valRowCount > 0 {
			if opt.Level == d.SchemataLevelColumn {
				assert.Equal(t, valRowCount, len(files[0].Columns), g.Marshal(files[0].Columns.Names()))
			} else {
				assert.Equal(t, valRowCount, len(files), g.Marshal(files.Paths()))
			}
		}

		if valRowCountMin > -1 {
			if opt.Level == d.SchemataLevelColumn {
				assert.Greater(t, len(files[0].Columns), valRowCountMin)
			} else {
				assert.Greater(t, len(files), valRowCountMin)
			}
		}

		if len(containsMap) > 0 {
			resultType := "files"

			if opt.Level == d.SchemataLevelColumn {
				resultType = "columns"
				for _, col := range columns {
					for word := range containsMap {
						if strings.EqualFold(word, col.Name) {
							containsMap[word] = true
						}
					}
				}
			} else {
				for _, file := range files {
					for word := range containsMap {
						if strings.Contains(file.URI, word) {
							containsMap[word] = true
						}
					}
				}
			}

			for _, word := range valContains {
				found := containsMap[word]
				assert.True(t, found, "did not find '%s' in %s for %s", word, resultType, connType)
			}

			for _, word := range valNotContains {
				found := containsMap[word]
				assert.False(t, found, "found '%s' in %s for %s", word, resultType, connType)
			}
		}
	}

	g.Info("valContains = %#v", valContains)
	g.Info("valNotContains = %#v", valNotContains)
	g.Info("containsMap = %#v", containsMap)
}

func TestSuiteFileS3(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeFileS3)
}

func TestSuiteFileGoogle(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeFileGoogle)
}

func TestSuiteFileAzure(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeFileAzure)
}

func TestSuiteFileLocal(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeFileLocal)
}

func TestSuiteFileSftp(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeFileSftp)
}

func TestSuiteFileFtp(t *testing.T) {
	t.Parallel()
	testSuite(t, dbio.TypeFileFtp)
}
