package main

import (
	"context"
	"embed"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/denisbrodbeck/machineid"
	"github.com/fatih/color"
	"github.com/getsentry/sentry-go"
	"github.com/rudderlabs/analytics-go"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"

	"github.com/flarco/dbio/database"
	"github.com/flarco/g"
	"github.com/integrii/flaggy"
	"github.com/spf13/cast"
)

//go:embed *
var slingFolder embed.FS
var examples = ``
var ctx = g.NewContext(context.Background())
var telemetryMap = g.M("begin_time", time.Now().UnixMicro())
var telemetry = true
var interrupted = false
var machineID = ""
var sentryOptions = sentry.ClientOptions{
	// Either set your DSN here or set the SENTRY_DSN environment variable.
	Dsn: "https://abb36e36341a4a2fa7796b6f9a0b3766@o881232.ingest.sentry.io/5835484",
	// Either set environment and release here or set the SENTRY_ENVIRONMENT
	// and SENTRY_RELEASE environment variables.
	Environment: lo.Ternary(core.Version == "dev", "Development", "Production"),
	Release:     "sling@" + core.Version,
	// Enable printing of SDK debug messages.
	// Useful when getting started or trying to figure something out.
	Debug: false,
}

var cliRun = &g.CliSC{
	Name:                  "run",
	Description:           "Execute a run",
	AdditionalHelpPrepend: "\nSee more examples and configuration details at https://docs.slingdata.io/sling-cli/",
	Flags: []g.Flag{
		{
			Name:        "config",
			ShortName:   "c",
			Type:        "string",
			Description: "The task config string or file to use (JSON or YAML).",
		},
		{
			Name:        "replication",
			ShortName:   "r",
			Type:        "string",
			Description: "The replication config file to use (JSON or YAML).\n",
		},
		{
			Name:        "src-conn",
			ShortName:   "",
			Type:        "string",
			Description: "The source database / storage connection (name, conn string or URL).",
		},
		{
			Name:        "src-stream",
			ShortName:   "",
			Type:        "string",
			Description: "The source table (schema.table), local / cloud file path.\n                       Can also be the path of sql file or in-line text to use as query. Use `file://` for local paths.",
		},
		{
			Name:        "src-options",
			Type:        "string",
			Description: "in-line options to further configure source (JSON or YAML).\n",
		},
		{
			Name:        "tgt-conn",
			ShortName:   "",
			Type:        "string",
			Description: "The target database connection (name, conn string or URL).",
		},
		{
			Name:        "tgt-object",
			ShortName:   "",
			Type:        "string",
			Description: "The target table (schema.table) or local / cloud file path. Use `file://` for local paths.",
		},
		{
			Name:        "tgt-options",
			Type:        "string",
			Description: "in-line options to further configure target (JSON or YAML).\n",
		},
		{
			Name:        "select",
			ShortName:   "",
			Type:        "string",
			Description: "Select specific streams to run from a replication. (comma separated)",
		},
		{
			Name:        "stdout",
			ShortName:   "",
			Type:        "bool",
			Description: "Output the stream to standard output (STDOUT).",
		},
		{
			Name:        "env",
			ShortName:   "",
			Type:        "string",
			Description: "in-line environment variable map to pass in (JSON or YAML).",
		},
		{
			Name:        "mode",
			ShortName:   "m",
			Type:        "string",
			Description: "The target load mode to use: backfill, incremental, truncate, snapshot, full-refresh.\n                       Default is full-refresh. For incremental, must provide `update-key` and `primary-key` values.\n                       All modes load into a new temp table on tgtConn prior to final load.",
		},
		{
			Name:        "limit",
			ShortName:   "l",
			Type:        "string",
			Description: "The maximum number of rows to pull.",
		},
		{
			Name:        "range",
			ShortName:   "",
			Type:        "string",
			Description: "The range to use for backfill mode, separated by a single comma. Example: `2021-01-01,2021-02-01` or `1,10000`",
		},
		{
			Name:        "primary-key",
			ShortName:   "",
			Type:        "string",
			Description: "The primary key to use for incremental. For composite key, put comma delimited values.",
		},
		{
			Name:        "update-key",
			ShortName:   "",
			Type:        "string",
			Description: "The update key to use for incremental.\n",
		},
		{
			Name:        "debug",
			ShortName:   "d",
			Type:        "bool",
			Description: "Set logging level to DEBUG.",
		},
		{
			Name:        "examples",
			ShortName:   "e",
			Type:        "bool",
			Description: "Shows some examples.",
		},
	},
	ExecProcess: processRun,
}

var cliInteractive = &g.CliSC{
	Name:        "it",
	Description: "launch interactive mode",
	ExecProcess: slingPrompt,
}

var cliUpdate = &g.CliSC{
	Name:        "update",
	Description: "Update Sling to the latest version",
	ExecProcess: updateCLI,
}

var cliCloud = &g.CliSC{
	Name:                  "cloud",
	Singular:              "cloud",
	Description:           "Deploy and trigger replications on the cloud",
	AdditionalHelpPrepend: "\nSee more details at https://docs.slingdata.io/sling-cli/",
	SubComs: []*g.CliSC{
		{
			Name:        "deploy",
			Description: "deploy a replication to the cloud",
			PosFlags: []g.Flag{
				{
					Name:        "path",
					ShortName:   "",
					Type:        "string",
					Description: "The file or folder path of YAML file(s)",
				},
			},
		},
		{
			Name:        "export",
			Description: "export a replication to a YAML file",
			PosFlags: []g.Flag{
				{
					Name:        "id",
					Type:        "string",
					Description: "The ID of the replication",
				},
				{
					Name:        "path",
					Type:        "string",
					Description: "The folder path to export to",
				},
			},
		},
		{
			Name:        "list",
			Description: "list replications / streams deployed on the cloud",
		},
		// {
		// 	Name:        "trigger",
		// 	Description: "Trigger a replication on the cloud",
		// 	Flags: []g.Flag{
		// 		{
		// 			Name:        "source",
		// 			Type:        "string",
		// 			Description: "The name of the source connection",
		// 		},
		// 		{
		// 			Name:        "target",
		// 			Type:        "string",
		// 			Description: "The name of the target connection",
		// 		},
		// 		{
		// 			Name:        "stream",
		// 			Type:        "string",
		// 			Description: "The name os the streams to trigger (optional)",
		// 		},
		// 	},
		// },
	},
	ExecProcess: processCloud,
}

var cliConns = &g.CliSC{
	Name:                  "conns",
	Singular:              "local connection",
	Description:           "Manage local connections in the sling env file",
	AdditionalHelpPrepend: "\nSee more details at https://docs.slingdata.io/sling-cli/",
	SubComs: []*g.CliSC{
		{
			Name:        "discover",
			Description: "list available streams in connection",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the connection to test",
				},
			},
			Flags: []g.Flag{
				{
					Name:        "filter",
					ShortName:   "f",
					Type:        "string",
					Description: "filter stream name by pattern (e.g. account_*)",
				},
				{
					Name:        "folder",
					Type:        "string",
					Description: "discover streams in a specific folder (for file connections)",
				},
				{
					Name:        "schema",
					Type:        "string",
					Description: "discover streams in a specific schema (for database connections)",
				},
				{
					Name:        "recursive",
					ShortName:   "",
					Type:        "bool",
					Description: "List all files recursively.",
				},
			},
		},
		{
			Name:        "list",
			Description: "list local connections detected",
		},
		{
			Name:        "test",
			Description: "test a local connection",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the connection to test",
				},
			},
		},
		{
			Name:        "unset",
			Description: "remove a connection from the sling env file",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the connection to remove",
				},
			},
		},
		{
			Name:        "set",
			Description: "set a connection in the sling env file",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the connection to set",
				},
				{
					Name:        "key=value properties...",
					ShortName:   "",
					Type:        "string",
					Description: "The key=value properties to set. See https://docs.slingdata.io/sling-cli/environment#set-connections",
				},
			},
		},
	},
	ExecProcess: processConns,
}

func init() {

	if val := os.Getenv("SLING_DISABLE_TELEMETRY"); val != "" {
		telemetry = !cast.ToBool(val)
	}

	// collect examples
	examplesBytes, _ := slingFolder.ReadFile("examples.sh")
	examples = string(examplesBytes)

	// cliInteractive.Make().Add()
	// cliAuth.Make().Add()
	// cliCloud.Make().Add()
	cliConns.Make().Add()
	// cliProject.Make().Add()
	cliRun.Make().Add()
	cliUpdate.Make().Add()
	// cliUi.Make().Add()

	if telemetry {
		if projectID == "" {
			projectID = os.Getenv("GITHUB_REPOSITORY_ID")
		}
		machineID, _ = machineid.ProtectedID("sling")
		if projectID != "" {
			machineID = g.MD5(projectID) // hashed
		}
		sentry.Init(sentryOptions)
	}
}

func Track(event string, props ...map[string]interface{}) {
	if !telemetry || core.Version == "dev" {
		return
	}

	// rsClient := analytics.New(env.RudderstackKey, env.RudderstackURL)
	rudderConfig := analytics.Config{Logger: analytics.StdLogger(log.New(io.Discard, "sling ", log.LstdFlags))}
	rsClient, err := analytics.NewWithConfig(env.RudderstackKey, env.RudderstackURL, rudderConfig)
	if err != nil {
		g.Trace("RudderClient Error: %s", err.Error())
		return
	}

	properties := analytics.NewProperties().
		Set("application", "sling-cli").
		Set("version", core.Version).
		Set("package", getSlingPackage()).
		Set("os", runtime.GOOS).
		Set("emit_time", time.Now().UnixMicro())

	for k, v := range telemetryMap {
		properties.Set(k, v)
	}

	if len(props) > 0 {
		for k, v := range props[0] {
			properties.Set(k, v)
		}
	}

	if g.CliObj != nil {
		properties.Set("command", g.CliObj.Name)
		if g.CliObj.UsedSC() != "" {
			properties.Set("sub-command", g.CliObj.UsedSC())
		}
	}

	rsClient.Enqueue(analytics.Track{
		UserId:     machineID,
		Event:      event,
		Properties: properties,
	})
	rsClient.Close()
}

func main() {

	exitCode := 11
	done := make(chan struct{})
	interrupt := make(chan os.Signal, 1)
	kill := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	signal.Notify(kill, syscall.SIGTERM)

	sling.ShowProgress = os.Getenv("SLING_SHOW_PROGRESS") != "false"
	database.UseBulkExportFlowCSV = cast.ToBool(os.Getenv("SLING_BULK_EXPORT_FLOW_CSV"))

	go func() {
		defer close(done)
		exitCode = cliInit()
	}()

	select {
	case <-done:
		os.Exit(exitCode)
	case <-kill:
		println("\nkilling process...")
		os.Exit(111)
	case <-interrupt:
		if cliRun.Sc.Used {
			println("\ninterrupting...")
			interrupted = true
			ctx.Cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		}
		os.Exit(exitCode)
		return
	}
}

func cliInit() int {
	env.InitLogger()

	// Set your program's name and description.  These appear in help output.
	flaggy.SetName("sling")
	flaggy.SetDescription("An Extract-Load tool | https://slingdata.io")
	flaggy.DefaultParser.ShowHelpOnUnexpected = true
	flaggy.DefaultParser.AdditionalHelpPrepend = "Slings data from a data source to a data target.\nVersion " + core.Version

	flaggy.SetVersion(core.Version)
	for _, cli := range g.CliArr {
		flaggy.AttachSubcommand(cli.Sc, 1)
	}

	flaggy.ShowHelpOnUnexpectedDisable()
	flaggy.Parse()

	ok, err := g.CliProcess()
	if err != nil || telemetryMap["error"] != nil {
		if err == nil && telemetryMap["error"] != nil {
			err = g.Error(cast.ToString(telemetryMap["error"]))
		}

		if g.In(g.CliObj.Name, "conns", "update") || telemetryMap["error"] == nil {
			telemetryMap["error"] = getErrString(err)

			eventName := g.CliObj.Name
			if g.CliObj.UsedSC() != "" {
				eventName = g.CliObj.Name + "_" + g.CliObj.UsedSC()
			}
			Track(eventName)
		}

		// sentry details
		if telemetry {

			evt := sentry.NewEvent()
			evt.Environment = sentryOptions.Environment
			evt.Release = sentryOptions.Release
			evt.Level = sentry.LevelError
			evt.Exception = []sentry.Exception{
				{
					Type: err.Error(),
					// Value:      err.Error(),
					Stacktrace: sentry.ExtractStacktrace(err),
				},
			}

			E, ok := err.(*g.ErrType)
			if ok {
				evt.Exception[0].Type = E.Err
				evt.Exception[0].Value = E.Full()
			}

			sentry.ConfigureScope(func(scope *sentry.Scope) {
				scope.SetUser(sentry.User{ID: machineID})
				scope.SetTransaction(E.Err)
			})

			sentry.CaptureEvent(evt)
			// eid := sentry.CaptureException(err)
		}

		if eh := sling.ErrorHelper(err); eh != "" {
			println()
			println(color.MagentaString(eh))
			println()
		}

		g.LogFatal(err)
	} else if !ok {
		flaggy.ShowHelp("")
	}

	switch {
	case g.CliObj.Name == "conns" && g.In(g.CliObj.UsedSC(), "test", "discover"):
		Track("conns_" + g.CliObj.UsedSC())
	case g.CliObj.Name == "update":
		Track("update")
	}

	return 0
}

func getErrString(err error) (errString string) {
	if err != nil {
		errString = err.Error()
		E, ok := err.(*g.ErrType)
		if ok && E.Debug() != "" {
			errString = E.Debug()
		}
	}
	return
}
