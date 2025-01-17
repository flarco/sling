package database

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"
	"runtime"
	"strings"

	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"

	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/env"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"github.com/xo/dburl"
)

// MsSQLServerConn is a Microsoft SQL Server connection
type MsSQLServerConn struct {
	BaseConn
	URL        string
	isAzureSQL bool
	isAzureDWH bool
}

// Init initiates the object
func (conn *MsSQLServerConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbSQLServer
	conn.BaseConn.defaultPort = 1433

	conn.SetProp("use_bcp_map_parallel", "false")

	if conn.BaseConn.GetProp("allow_bulk_import") == "" {
		conn.BaseConn.SetProp("allow_bulk_import", "true")
	}

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	// https://github.com/slingdata-io/sling-cli/issues/310
	// If both a portNumber and instanceName are used, the portNumber will take precedence and the instanceName will be ignored.
	// therefore, if instanceName is provided, don't set a defaultPort
	if u, err := dburl.Parse(conn.URL); err == nil {
		if instanceName := strings.TrimPrefix(u.Path, "/"); instanceName != "" {
			// set as 0 so BaseConn.Init won't inject port number
			conn.BaseConn.defaultPort = 0
		}
	}

	return conn.BaseConn.Init()
}

// GetURL returns the processed URL
func (conn *MsSQLServerConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}

	url, err := dburl.Parse(connURL)
	if err != nil {
		g.LogError(err, "could not parse SQL Server URL")
		return connURL
	}

	return url.String()
}

type SqlServerLogger struct{}

func (l *SqlServerLogger) Printf(format string, v ...any) {
	env.Print(g.F(format, v...))
}
func (l *SqlServerLogger) Println(v ...any) {
	if len(v) == 1 {
		env.Println(cast.ToString(v[0]))
	}
	if len(v) > 1 {
		env.Println(g.F(cast.ToString(v[0]), v...))
	}
}

func (conn *MsSQLServerConn) ConnString() string {

	propMapping := map[string]string{
		"connection timeout": "connection timeout",
		"connection_timeout": "connection timeout",

		"dial timeout": "dial timeout",
		"dial_timeout": "dial timeout",

		"encrypt": "encrypt",

		"authenticator": "authenticator",

		"app name": "app name",
		"app_name": "app name",

		"keepAlive":  "keepAlive",
		"keep_alive": "keepAlive",

		"failoverpartner":  "failoverpartner",
		"failover_partner": "failoverpartner",

		"failoverport":  "failoverport",
		"failover_port": "failoverport",

		"packet size": "packet size",
		"packet_size": "packet size",

		"log": "log",

		"TrustServerCertificate":   "TrustServerCertificate",
		"trust_server_certificate": "TrustServerCertificate",

		"TrustedConnection":  "TrustedConnection",
		"trusted_connection": "TrustedConnection",

		"certificate": "certificate",

		"hostNameInCertificate":   "hostNameInCertificate",
		"hostname_in_certificate": "hostNameInCertificate",

		"tlsmin":  "tlsmin",
		"tls_min": "tlsmin",

		"ServerSPN":  "ServerSPN",
		"server_spn": "ServerSPN",

		"Workstation ID": "Workstation ID",
		"workstation_id": "Workstation ID",

		"ApplicationIntent":  "ApplicationIntent",
		"application_intent": "ApplicationIntent",

		"protocol":          "protocol",
		"columnencryption":  "columnencryption",
		"column_encryption": "columnencryption",

		"multisubnetfailover":   "multisubnetfailover",
		"multi_subnet_failover": "multisubnetfailover",

		"pipe": "pipe",

		// Kerberos
		"krb5-configfile":         "krb5-configfile",
		"krb5-realm":              "krb5-realm",
		"krb5-keytabfile":         "krb5-keytabfile",
		"krb5-credcachefile":      "krb5-credcachefile",
		"krb5-dnslookupkdc":       "krb5-dnslookupkdc",
		"krb5-udppreferencelimit": "krb5-udppreferencelimit",

		"krb5_config_file":          "krb5-configfile",
		"krb5_realm":                "krb5-realm",
		"krb5_keytab_file":          "krb5-keytabfile",
		"krb5_cred_cache_file":      "krb5-credcachefile",
		"krb5_dns_lookup_kdc":       "krb5-dnslookupkdc",
		"krb5_udp_preference_limit": "krb5-udppreferencelimit",

		// Azure Active Directory authentication (https://github.com/microsoft/go-mssqldb?tab=readme-ov-file#azure-active-directory-authentication)
		"fedauth":  "fedauth",
		"fed_auth": "fedauth",
	}

	U, _ := net.NewURL(conn.GetURL())
	for key, new_key := range propMapping {
		if val := conn.GetProp(key); val != "" {
			U.SetParam(new_key, val)
		}
	}

	if val := conn.GetProp("log"); val != "" {
		mssql.SetLogger(&SqlServerLogger{})
	}

	return U.String()
}

func (conn *MsSQLServerConn) Connect(timeOut ...int) (err error) {
	err = conn.BaseConn.Connect(timeOut...)
	if err != nil {
		return err
	}

	// get version
	data, _ := conn.Query(`select @@version v` + noDebugKey)
	if len(data.Rows) > 0 {
		version := cast.ToString(data.Rows[0][0])
		if strings.Contains(strings.ToLower(version), "sql azure") {
			conn.isAzureSQL = true
			conn.Type = dbio.TypeDbAzure
		} else if strings.Contains(strings.ToLower(version), "azure sql data warehouse") {
			conn.isAzureDWH = true
			conn.Type = dbio.TypeDbAzureDWH
		}
	}

	return nil
}

func (conn *MsSQLServerConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (string, error) {

	table.Columns = data.Columns
	ddl, err := conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return ddl, g.Error(err)
	}

	ddl, err = table.AddPrimaryKeyToDDL(ddl, data.Columns)
	if err != nil {
		return ddl, g.Error(err)
	}

	for _, index := range table.Indexes(data.Columns) {
		ddl = ddl + ";\n" + index.CreateDDL()
	}

	return ddl, nil
}

// BulkImportFlow bulk import flow
func (conn *MsSQLServerConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	if conn.isAzureDWH {
		return conn.CopyViaAzure(tableFName, df)
	}

	return conn.BaseConn.BulkImportFlow(tableFName, df)
}

func (conn *MsSQLServerConn) GetTableColumns(table *Table, fields ...string) (columns iop.Columns, err error) {
	columns, err = conn.BaseConn.GetTableColumns(table, fields...)
	if err != nil {
		// try synonym

		// get database first (synonym could point to other db)
		values := g.M("schema", table.Schema, "table", table.Name)
		data, err1 := conn.SubmitTemplate("single", conn.template.Metadata, "synonym_database", values)
		if err1 != nil {
			return columns, g.Error(err1, "could not get table or synonym database")
		} else if len(data.Rows) == 0 {
			return columns, g.Error("did not find table or synonym")
		}

		synDbName := cast.ToString(data.Records(true)[0]["database_name"])
		conn.SetProp("synonym_database", synDbName)
		conn.SetProp("get_synonym", "true")
		columns, err1 = conn.BaseConn.GetTableColumns(table, fields...)
		if err1 != nil {
			return columns, err1
		} else if len(columns) > 0 {
			err = nil
		}
		conn.SetProp("get_synonym", "false")
		conn.SetProp("synonym_database", "") // clear
	}
	return
}

func (conn *MsSQLServerConn) SubmitTemplate(level string, templateMap map[string]string, name string, values map[string]interface{}) (data iop.Dataset, err error) {
	if cast.ToBool(conn.GetProp("get_synonym")) && name == "columns" {
		name = "columns_synonym"
		values["base_object_database"] = conn.GetProp("synonym_database")
	}
	return conn.BaseConn.SubmitTemplate(level, templateMap, name, values)
}

// BulkImportStream bulk import stream
func (conn *MsSQLServerConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	conn.Commit() // cannot have transaction lock table

	// return conn.BaseConn.InsertBatchStream(tableFName, ds)
	_, err = exec.LookPath(conn.bcpPath())
	if err != nil {
		g.Debug("bcp not found in path. Using cursor...")
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	} else if conn.GetProp("allow_bulk_import") != "true" {
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	}

	// needs to get columns to shape stream
	columns, err := conn.GetColumns(tableFName)
	if err != nil {
		err = g.Error(err, "could not get column list")
		return
	}

	ds, err = ds.Shape(columns)
	if err != nil {
		err = g.Error(err, "could not shape stream")
		return
	}

	return conn.BcpImportFileParrallel(tableFName, ds)
}

// BcpImportFileParrallel uses goroutine to import partitioned files
func (conn *MsSQLServerConn) BcpImportFileParrallel(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	fileRowLimit := cast.ToInt(conn.GetProp("FILE_MAX_ROWS"))
	if fileRowLimit == 0 {
		fileRowLimit = 200000
	}

	delimiterRep := `$~d$~`
	quoteRep := `$~q$~`
	newlRep := `$~n$~`
	carrRep := `$~r$~`
	postUpdateCol := map[int]uint64{}

	// transformation to correctly post process quotes, newlines, and delimiter afterwards
	// https://stackoverflow.com/questions/782353/sql-server-bulk-insert-of-csv-file-with-inconsistent-quotes
	// reduces performance by ~25%, but is correct, and still 10x faster then insert into with batch VALUES
	// If we use the parallel way, we gain back the speed by using more power. We also loose order.
	transf := func(row []interface{}) (nRow []interface{}) {
		nRow = row
		for i, val := range row {

			switch v := val.(type) {
			case string:
				nRow[i] = strings.ReplaceAll(
					val.(string), ",", delimiterRep,
				)
				nRow[i] = strings.ReplaceAll(
					nRow[i].(string), `"`, quoteRep,
				)
				nRow[i] = strings.ReplaceAll(
					nRow[i].(string), "\r", carrRep,
				)
				nRow[i] = strings.ReplaceAll(
					nRow[i].(string), "\n", newlRep,
				)
				if nRow[i].(string) != val.(string) {
					if _, ok := postUpdateCol[i]; ok {
						postUpdateCol[i]++
					} else {
						postUpdateCol[i] = 1
					}
				}
			default:
				_ = v
			}
		}
		return
	}

	doImport := func(tableFName string, filePath string) {
		defer ds.Context.Wg.Write.Done()

		// delete csv
		defer func() { env.RemoveLocalTempFile(filePath) }()

		_, err := conn.BcpImportFile(tableFName, filePath)
		ds.Context.CaptureErr(err)
	}

	for batch := range ds.BatchChan {

		if batch.ColumnsChanged() || batch.IsFirst() {
			ds.Context.Lock()
			columns, err := conn.GetColumns(tableFName, batch.Columns.Names()...)
			if err != nil {
				return count, g.Error(err, "could not get matching list of columns from table")
			}
			ds.Context.Unlock()

			err = batch.Shape(columns)
			if err != nil {
				return count, g.Error(err, "could not shape batch stream")
			}
		}

		ds.Pause()
		batch.AddTransform(transf)
		ds.Unpause()

		// Write the ds to a temp file

		filePath := path.Join(env.GetTempFolder(), g.NewTsID(g.F("sqlserver.%s", env.CleanTableName(tableFName)))+g.F("%d.csv", len(ds.Batches)))
		csvRowCnt, err := writeCsvWithoutQuotes(filePath, batch, fileRowLimit)

		if err != nil {
			os.Remove(filePath)
			err = g.Error(err, "Error csv.WriteStream(ds) to "+filePath)
			ds.Context.CaptureErr(err)
			ds.Context.Cancel()
			return 0, g.Error(err)
		} else if csvRowCnt == 0 {
			// no data from source
			os.Remove(filePath)
			continue
		}

		ds.Context.Wg.Write.Add()
		go doImport(tableFName, filePath)
	}

	ds.Context.Wg.Write.Wait()

	// post process if strings have been modified
	if len(postUpdateCol) > 0 && ds.Err() == nil {
		setCols := []string{}
		for i, col := range ds.Columns {
			if _, ok := postUpdateCol[i]; !ok {
				continue
			}

			replExpr1 := g.R(
				`REPLACE(CONVERT(VARCHAR(MAX), {field}), '{delimiterRep}', '{delimiter}')`,
				"field", col.Name,
				"delimiterRep", delimiterRep,
				"delimiter", ",",
			)
			replExpr2 := g.R(
				`REPLACE({replExpr1}, '{quoteRep}', '{quote}')`,
				"replExpr1", replExpr1,
				"quoteRep", quoteRep,
				"quote", `"`,
			)
			replExpr3 := g.R(
				`REPLACE({replExpr2}, '{newlRep}', {newl})`,
				"replExpr2", replExpr2,
				"newlRep", carrRep,
				"newl", `CHAR(13)`,
			)
			replExpr4 := g.R(
				`REPLACE({replExpr2}, '{newlRep}', {newl})`,
				"replExpr2", replExpr3,
				"newlRep", newlRep,
				"newl", `CHAR(10)`,
			)
			setCols = append(
				setCols, fmt.Sprintf(`%s = %s`, col.Name, replExpr4),
			)
		}

		// do update statement if needed
		if len(setCols) > 0 {
			setColsStr := strings.Join(setCols, ", ")
			sql := fmt.Sprintf(`UPDATE %s SET %s`, tableFName, setColsStr) + noDebugKey
			_, err = conn.Exec(sql)
			if err != nil {
				err = g.Error(err, "could not apply post update query")
				return
			}
		}
	}

	return ds.Count, ds.Err()
}

func (conn *MsSQLServerConn) bcpPath() string {
	if val := conn.GetProp("bcp_path"); val != "" {
		return val
	}
	return "bcp"
}

// BcpImportFile Import using bcp tool
// https://docs.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver15
// bcp dbo.test1 in '/tmp/LargeDataset.csv' -S tcp:sqlserver.host,51433 -d master -U sa -P 'password' -c -t ',' -b 5000
// Limitation: if comma or delimite is in field, it will error.
// need to use delimiter not in field, or do some other transformation
func (conn *MsSQLServerConn) BcpImportFile(tableFName, filePath string) (count uint64, err error) {
	var stderr, stdout bytes.Buffer

	connURL := conn.URL
	if su := conn.GetProp("ssh_url"); su != "" {
		connURL = su // use ssh url if specified
	}

	url, err := dburl.Parse(connURL)
	if err != nil {
		return
	}

	// get version
	version := 14
	versionOut, err := exec.Command(conn.bcpPath(), "-v").Output()
	if err != nil {
		g.Debug("could not get bcp version running `bcp -v` (%s)", err.Error())
		version = 0
	}
	regex := *regexp.MustCompile(`Version: (\d+)`)
	verRes := regex.FindStringSubmatch(string(versionOut))
	if len(verRes) == 2 {
		version = cast.ToInt(verRes[1])
	}
	g.Debug("bcp version is %d", version)

	// Import to Database
	batchSize := 50000
	password, _ := url.User.Password()
	port := url.Port()
	host := strings.ReplaceAll(url.Host, ":"+port, "")
	instance := strings.ReplaceAll(url.Path, "/", "")
	database := url.Query().Get("database")
	user := url.User.Username()

	if port == "" {
		port = cast.ToString(conn.GetType().DefPort())
	}
	hostPort := fmt.Sprintf("tcp:%s,%s", host, port)
	if instance != "" {
		hostPort = g.F("%s\\%s", hostPort, instance)
	}
	errPath := "/dev/stderr"
	if runtime.GOOS == "windows" || true {
		errPath = path.Join(env.GetTempFolder(), g.NewTsID(g.F("sqlserver.%s", env.CleanTableName(tableFName)))+".error")
		defer os.Remove(errPath)
	}

	bcpArgs := []string{
		strings.ReplaceAll(tableFName, `"`, ""),
		"in", filePath,
		"-S", hostPort,
		"-d", database,
		"-t", ",",
		"-m", "1",
		"-w",
		"-q",
		"-b", cast.ToString(batchSize),
		"-F", "2",
		"-e", errPath,
	}

	if bcpAuthString := conn.GetProp("bcp_auth_string"); bcpAuthString != "" {
		bcpAuthParts := []string{}
		err = g.Unmarshal(bcpAuthString, &bcpAuthParts)
		if err != nil {
			err = g.Error("could not parse property `bcp_auth_string` (%s). Is an array of strings provided?", err.Error())
			return
		}
		bcpArgs = append(bcpArgs, bcpAuthParts...)
	} else {
		if g.In(conn.GetProp("authenticator"), "winsspi") || conn.isTrusted() {
			bcpArgs = append(bcpArgs, "-T")
		} else {
			bcpArgs = append(bcpArgs, "-U", user, "-P", password)
		}

		if cast.ToBool(conn.GetProp("bcp_entra_auth")) {
			bcpArgs = append(bcpArgs, "-G")
		}
	}

	if bcpExtraArgs := conn.GetProp("bcp_extra_args"); bcpExtraArgs != "" {
		bcpExtraParts := []string{}
		err = g.Unmarshal(bcpExtraArgs, &bcpExtraParts)
		if err != nil {
			err = g.Error("could not parse property `bcp_extra_args` (%s). Is an array of strings provided?", err.Error())
			return
		}
		bcpArgs = append(bcpArgs, bcpExtraParts...)
	}

	proc := exec.Command(conn.bcpPath(), bcpArgs...)
	proc.Stderr = &stderr
	proc.Stdout = &stdout

	if version <= 14 {
		g.Warn("bcp version %d is old. This may give issues with sling, consider upgrading.", version)
	} else if version >= 18 {
		// add u for version 18
		proc.Args = append(proc.Args, "-u")
	}

	// build cmdStr
	args := lo.Map(proc.Args, func(v string, i int) string {
		if !g.In(v, "in", "-S", "-d", "-U", "-P", "-t", "-u", "-m", "-c", "-q", "-b", "-F", "-e", conn.bcpPath()) {
			v = strings.ReplaceAll(v, hostPort, "****")
			if password != "" {
				v = strings.ReplaceAll(v, password, "****")
			}
			return `'` + strings.ReplaceAll(v, `'`, `''`) + `'`
		}
		return v
	})
	cmdStr := strings.Join(args, ` `)
	g.Debug(cmdStr)

	// run and wait for finish
	err = proc.Run()

	// get count
	regex = *regexp.MustCompile(`(?s)(\d+) rows copied.`)
	res := regex.FindStringSubmatch(stdout.String())
	if len(res) == 2 {
		count = cast.ToUint64(res[1])
	}

	if err != nil {
		errOut := stderr.String()
		if errPath != "/dev/stderr" {
			errOutB, _ := os.ReadFile(errPath)
			errOut = string(errOutB)
		}

		err = g.Error(
			err,
			fmt.Sprintf(
				"SQL Server BCP Import Command -> %s\nSQL Server BCP Import Error  -> %s\n%s",
				cmdStr, errOut, stdout.String(),
			),
		)
		return
	}

	return
}

// BcpExport exports data to datastream
func (conn *MsSQLServerConn) BcpExport() (err error) {
	return
}

// sqlcmd -S localhost -d BcpSampleDB -U sa -P <your_password> -I -Q "select * from TestEmployees;"

// EXPORT
// bcp TestEmployees out ~/test_export.txt -S localhost -U sa -P <your_password> -d BcpSampleDB -c -t ','
// bcp dbo.test1 out ~/test_export.csv -S server.database.windows.net -U user -P 'password' -d db1 -c -t ',' -q -b 10000

// importing from blob
// https://azure.microsoft.com/en-us/updates/files-from-azure-blob-storage-to-sql-database/

//UPSERT
// https://vladmihalcea.com/how-do-upsert-and-merge-work-in-oracle-sql-server-postgresql-and-mysql/

// GenerateUpsertSQL generates the upsert SQL
func (conn *MsSQLServerConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	sqlTempl := `
	merge into {tgt_table} tgt
	using (select *	from {src_table}) src
	ON ({src_tgt_pk_equal})
	WHEN MATCHED THEN
		UPDATE SET {set_fields}
	WHEN NOT MATCHED THEN
		INSERT ({insert_fields}) values  ({src_fields});
	`

	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"set_fields", upsertMap["set_fields"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", strings.ReplaceAll(upsertMap["placeholder_fields"], "ph.", "src."),
	)

	return
}

// CopyViaAzure uses the Azure DWH COPY INTO Table command
func (conn *MsSQLServerConn) CopyViaAzure(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	if !conn.BaseConn.credsProvided("AZURE") {
		err = g.Error("Need to set 'AZURE_SAS_SVC_URL', 'AZURE_CONTAINER' and 'AZURE_ACCOUNT' to copy to azure dwh from container")
		return
	}

	azPath := fmt.Sprintf(
		"https://%s.blob.core.windows.net/%s/%s-%s",
		conn.GetProp("AZURE_ACCOUNT"),
		conn.GetProp("AZURE_CONTAINER"),
		tempCloudStorageFolder,
		tableFName,
	)

	azFs, err := filesys.NewFileSysClient(dbio.TypeFileAzure, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for S3")
		return
	}

	err = filesys.Delete(azFs, azPath+"*")
	if err != nil {
		return count, g.Error(err, "Could not Delete: "+azPath)
	}

	// defer func() { azFs.Delete(azPath + "*") }() // cleanup

	fileReadyChn := make(chan filesys.FileReady, 10000)
	go func() {
		var bw int64
		bw, err = azFs.WriteDataflowReady(df, azPath, fileReadyChn, iop.DefaultStreamConfig())
		g.DebugLow("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), azPath)

		if err != nil {
			err = g.Error(err, "Error writing dataflow to azure blob: "+azPath)
			return
		}
	}()

	g.Info("writing to azure container for import")

	doCopy := func(file filesys.FileReady) {
		defer df.Context.Wg.Write.Done()
		cnt, err := conn.CopyFromAzure(tableFName, file.Node.URI)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "could not copy to azure dwh"))
		} else {
			count += cnt
		}
	}

	for file := range fileReadyChn {
		df.Context.Wg.Write.Add()
		go doCopy(file)
	}

	df.Context.Wg.Write.Wait()
	if df.Err() != nil {
		err = g.Error(df.Err())
	}

	return df.Count(), err
}

func (conn *MsSQLServerConn) isTrusted() bool {
	return strings.Contains(conn.ConnString(), "TrustedConnection")
}

// CopyFromAzure uses the COPY INTO Table command from Azure
// https://docs.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=azure-sqldw-latest
func (conn *MsSQLServerConn) CopyFromAzure(tableFName, azPath string) (count uint64, err error) {
	azSasURL := conn.GetProp("AZURE_SAS_SVC_URL")
	if azSasURL == "" {
		err = g.Error("Need to set 'AZURE_SAS_SVC_URL' to copy to azure dwh from container")
		return
	}

	azSasURLArr := strings.Split(azSasURL, "?")
	if len(azSasURLArr) != 2 {
		err = g.Error(
			g.Error("Invalid provided AZURE_SAS_SVC_URL"),
			"",
		)
		return
	}
	azToken := azSasURLArr[1]

	sql := g.R(
		conn.template.Core["copy_from_azure"],
		"table", tableFName,
		"azure_path", azPath,
		"azure_sas_token", azToken,
		"date_format", "ymd",
	)

	conn.SetProp("azToken", azToken) // to not log it in debug logging

	g.Info("copying into azure DWH")
	g.Debug("url: " + azPath)
	_, err = conn.Exec(sql)
	if err != nil {
		conn.SetProp("azToken", azToken)
		return 0, g.Error(err, "SQL Error:\n"+env.Clean(conn.Props(), sql))
	}

	return 0, nil
}

// writeCsvWithoutQuotes writes a CSV file without quotes
// It takes the following parameters:
//   - path: the file path where the CSV will be written
//   - batch: an iop.Batch containing the data to be written
//   - limit: the maximum number of rows to write (0 for no limit)
//
// The function returns:
//   - cnt: the number of rows written
//   - err: any error encountered during the process
//
// The function performs the following steps:
// 1. Creates a new file at the specified path
// 2. Sets up a UTF-16LE encoder and writer
// 3. Writes the UTF-16LE Byte Order Mark (BOM)
// 4. Writes the header row using column names from the batch
// 5. Iterates through the batch rows, writing each as a CSV line
// 6. Handles data type conversions using a Sp (StreamProcessor) object
// 7. Uses platform-specific newline characters
// 8. Writes data without surrounding quotes, separating fields with commas
//
// Note: This function is specifically designed for SQL Server bulk import
// operations, which require UTF-16LE encoding and no quoting of fields.
func writeCsvWithoutQuotes(path string, batch *iop.Batch, limit int) (cnt uint64, err error) {
	file, err := os.Create(path)
	if err != nil {
		return cnt, g.Error(err, "could not create file")
	}
	defer file.Close()

	// Create a UTF-16LE encoder
	encoder := unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewEncoder()

	// Create a writer that transforms UTF-8 to UTF-16LE
	writer := transform.NewWriter(file, encoder)
	defer writer.Close()

	// Write the BOM (Byte Order Mark) for UTF-16LE
	_, err = writer.Write([]byte{0xFF, 0xFE})
	if err != nil {
		return cnt, g.Error(err, "could not write BOM to file")
	}

	fields := batch.Columns.Names()

	newLine := "\n"
	if runtime.GOOS == "windows" {
		newLine = "\r\n"
	}

	// Write header
	_, err = writer.Write([]byte(strings.Join(fields, ",") + newLine))
	if err != nil {
		return cnt, g.Error(err, "could not write header to file")
	}

	Sp := batch.Ds().Sp
	timestampLayout := dbio.TypeDbSQLServer.GetTemplateValue("variable.timestamp_layout")
	Sp.SetConfig(map[string]string{"datetime_format": timestampLayout})

	for row0 := range batch.Rows {
		cnt++
		row := make([]string, len(row0))
		for i, val := range row0 {
			row[i] = Sp.CastToString(i, val, batch.Columns[i].Type)
		}

		// Write row
		_, err = writer.Write([]byte(strings.Join(row, ",") + newLine))
		if err != nil {
			return cnt, g.Error(err, "could not write row to file")
		}

		if batch.Count == int64(limit) {
			batch.Close()
			break
		}
	}

	return cnt, nil
}

// CastColumnForSelect casts to the correct target column type
func (conn *MsSQLServerConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) (selectStr string) {
	qName := conn.Self().Quote(srcCol.Name)
	srcDbType := strings.ToLower(srcCol.DbType)
	tgtDbType := strings.ToLower(tgtCol.DbType)

	switch {
	// maintain lower case if inserting from uniqueidentifier into nvarchar
	case srcDbType == "uniqueidentifier" && tgtDbType == "nvarchar":
		selectStr = g.F("cast(%s as nvarchar(max))", qName)
	default:
		selectStr = qName
	}

	return selectStr
}
