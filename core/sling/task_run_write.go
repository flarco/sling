package sling

import (
	"bufio"
	"context"
	"database/sql"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// WriteToFile writes to a target file
func (t *TaskExecution) WriteToFile(cfg *Config, df *iop.Dataflow) (cnt uint64, err error) {
	var bw int64
	defer t.PBar.Finish()

	if cfg.TgtConn.URL() != "" {
		dateMap := iop.GetISO8601DateMap(time.Now())
		cfg.TgtConn.Set(g.M("url", g.Rm(cfg.TgtConn.URL(), dateMap)))

		// construct props by merging with options
		options := g.M()
		g.Unmarshal(g.Marshal(cfg.Target.Options), &options)
		props := append(
			g.MapToKVArr(cfg.TgtConn.DataS()),
			g.MapToKVArr(g.ToMapString(options))...,
		)

		fs, err := filesys.NewFileSysClientFromURLContext(t.Context.Ctx, cfg.TgtConn.URL(), props...)
		if err != nil {
			err = g.Error(err, "Could not obtain client for: %s", cfg.TgtConn.Type)
			return cnt, err
		}

		// apply column casing
		applyColumnCasingToDf(df, fs.FsType(), t.Config.Target.Options.ColumnCasing)

		bw, err = fs.WriteDataflow(df, cfg.TgtConn.URL())
		if err != nil {
			err = g.Error(err, "Could not FileSysWriteDataflow")
			return cnt, err
		}
		cnt = df.Count()
	} else if cfg.Options.StdOut {
		// apply column casing
		applyColumnCasingToDf(df, dbio.TypeFileLocal, t.Config.Target.Options.ColumnCasing)

		options := map[string]string{"delimiter": ","}
		g.Unmarshal(g.Marshal(cfg.Target.Options), &options)
		for stream := range df.StreamCh {
			stream.SetConfig(options)
			for batchR := range stream.NewCsvReaderChnl(0, 0) {
				if len(batchR.Columns) != len(df.Columns) {
					err = g.Error(err, "number columns have changed, not compatible with stdout.")
					return
				}
				bufStdout := bufio.NewWriter(os.Stdout)
				bw, err = filesys.Write(batchR.Reader, bufStdout)
				bufStdout.Flush()
				if err != nil {
					err = g.Error(err, "Could not write to Stdout")
					return
				} else if err = stream.Context.Err(); err != nil {
					err = g.Error(err, "encountered stream error")
					return
				}
				cnt = cnt + uint64(batchR.Counter)
			}
		}
	} else {
		err = g.Error("target for output is not specified")
		return
	}

	g.DebugLow(
		"wrote %s: %d rows [%s r/s]",
		humanize.Bytes(cast.ToUint64(bw)), cnt, getRate(cnt),
	)

	return
}

// WriteToDb writes to a target DB
// create temp table
// load into temp table
// insert / incremental / replace into target table
func (t *TaskExecution) WriteToDb(cfg *Config, df *iop.Dataflow, tgtConn database.Connection) (cnt uint64, err error) {
	defer t.PBar.Finish()

	// detect empty
	if len(df.Columns) == 0 {
		err = g.Error("no stream columns detected")
		return
	}

	targetTable := cfg.Target.Object
	if cfg.Target.Options.TableTmp == "" {
		tableTmp, err := database.ParseTableName(targetTable, tgtConn.GetType())
		if err != nil {
			return 0, g.Error(err, "no not parse object table name")
		}

		suffix := lo.Ternary(tgtConn.GetType().DBNameUpperCase(), "_TMP", "_tmp")
		if g.In(tgtConn.GetType(), dbio.TypeDbOracle) {
			if len(tableTmp.Name) > 24 {
				tableTmp.Name = tableTmp.Name[:24] // max is 30 chars
			}

			// some weird column / commit error, not picking up latest columns
			suffix2 := g.RandString(g.NumericRunes, 1) + g.RandString(g.AplhanumericRunes, 1)
			suffix2 = lo.Ternary(
				tgtConn.GetType().DBNameUpperCase(),
				strings.ToUpper(suffix2),
				strings.ToLower(suffix2),
			)
			suffix = suffix + suffix2
		}

		tableTmp.Name = tableTmp.Name + suffix
		cfg.Target.Options.TableTmp = tableTmp.FullName()
	}
	if cfg.Mode == "" {
		cfg.Mode = FullRefreshMode
	}

	// Drop & Create the temp table
	err = tgtConn.DropTable(cfg.Target.Options.TableTmp)
	if err != nil {
		err = g.Error(err, "could not drop table "+cfg.Target.Options.TableTmp)
		return
	}

	if paused := df.Pause(); !paused { // to create DDL and set column change functions
		err = g.Error(err, "could not pause streams to infer columns")
		return
	}

	// apply column casing
	applyColumnCasingToDf(df, tgtConn.GetType(), t.Config.Target.Options.ColumnCasing)

	sampleData := iop.NewDataset(df.Columns)
	sampleData.Rows = df.Buffer
	sampleData.Inferred = df.Inferred
	if !sampleData.Inferred {
		sampleData.SafeInference = true
		sampleData.InferColumnTypes()
		df.Columns = sampleData.Columns
	}

	// check table ddl
	if cfg.Target.Options.TableDDL != "" && !strings.Contains(cfg.Target.Options.TableDDL, targetTable) {
		err = g.Error("The Table DDL provided needs to contains the exact object table name: %s", targetTable)
		return
	}

	_, err = createTableIfNotExists(
		tgtConn,
		sampleData,
		cfg.Target.Options.TableTmp,
		// fix tempTableDDL
		strings.Replace(cfg.Target.Options.TableDDL, targetTable, cfg.Target.Options.TableTmp, 1),
	)
	if err != nil {
		err = g.Error(err, "could not create temp table "+cfg.Target.Options.TableTmp)
		return
	}
	cfg.Target.TmpTableCreated = true
	df.Columns = sampleData.Columns

	t.AddCleanupTask(func() {
		conn, err := t.getTgtDBConn(context.Background())
		if err == nil {
			err = conn.DropTable(cfg.Target.Options.TableTmp)
			g.LogError(err)
		}
	})

	err = tgtConn.BeginContext(df.Context.Ctx)
	if err != nil {
		err = g.Error(err, "could not open transaction to write to temp table")
		return
	}

	adjustColumnType := cfg.Target.Options.AdjustColumnType != nil && *cfg.Target.Options.AdjustColumnType

	// set OnColumnChanged
	if adjustColumnType {
		df.OnColumnChanged = func(col iop.Column) error {

			table, err := database.ParseTableName(cfg.Target.Options.TableTmp, tgtConn.GetType())
			if err != nil {
				return g.Error(err, "could not get temp table name for schema change")
			}
			table.Columns, err = tgtConn.GetColumns(cfg.Target.Options.TableTmp)
			if err != nil {
				return g.Error(err, "could not get table columns for schema change")
			}

			df.Columns[col.Position-1].Type = col.Type
			ok, err := tgtConn.OptimizeTable(&table, df.Columns)
			if err != nil {
				return g.Error(err, "could not change table schema")
			} else if ok {
				cfg.Target.columns = table.Columns
				for i := range df.Columns {
					df.Columns[i].Type = table.Columns[i].Type
				}
			}

			return nil
		}
	}

	// set OnColumnAdded
	if *cfg.Target.Options.AddNewColumns {
		df.OnColumnAdded = func(col iop.Column) error {

			// sleep to allow transaction to close
			// time.Sleep(100 * time.Millisecond)

			// df.Context.Lock()
			// defer df.Context.Unlock()

			table, err := database.ParseTableName(cfg.Target.Options.TableTmp, tgtConn.GetType())
			if err != nil {
				return g.Error(err, "could not get temp table name for column add")
			}

			ok, err := database.AddMissingColumns(tgtConn, table, iop.Columns{col})
			if err != nil {
				return g.Error(err, "could not add missing columns")
			} else if ok {
				_, err = pullTargetTempTableColumns(t.Config, tgtConn, true)
				if err != nil {
					return g.Error(err, "could not get table columns")
				}
			}
			return nil
		}
	}

	df.Unpause() // to create DDL and set column change functions
	t.SetProgress("streaming data")
	cnt, err = tgtConn.BulkImportFlow(cfg.Target.Options.TableTmp, df)
	if err != nil {
		tgtConn.Rollback()
		if cast.ToBool(os.Getenv("SLING_CLI")) && cfg.sourceIsFile() {
			err = g.Error(err, "could not insert into %s.", cfg.Target.Options.TableTmp)
		} else {
			err = g.Error(err, "could not insert into "+cfg.Target.Options.TableTmp)
		}
		return
	}

	tgtConn.Commit()
	t.PBar.Finish()

	tCnt, _ := tgtConn.GetCount(cfg.Target.Options.TableTmp)
	if cnt != tCnt {
		err = g.Error("inserted in temp table but table count (%d) != stream count (%d). Records missing. Aborting", tCnt, cnt)
		return
	} else if tCnt == 0 && len(sampleData.Rows) > 0 {
		err = g.Error("Loaded 0 records while sample data has %d records. Exiting.", len(sampleData.Rows))
		return
	}

	// pre SQL
	if preSQL := cfg.Target.Options.PreSQL; preSQL != "" {
		t.SetProgress("executing pre-sql")
		preSQL, err = getSQLText(preSQL)
		if err != nil {
			err = g.Error(err, "could not get pre-sql body")
			return cnt, err
		}

		fMap, err := t.Config.GetFormatMap()
		if err != nil {
			err = g.Error(err, "could not get format map for pre-sql")
			return cnt, err
		}

		_, err = tgtConn.ExecMulti(g.Rm(preSQL, fMap))
		if err != nil {
			err = g.Error(err, "could not execute pre-sql on target")
			return cnt, err
		}
	}

	if cnt == 0 && !cast.ToBool(os.Getenv("SLING_ALLOW_EMPTY_TABLES")) {
		g.Warn("No data or records found in stream. Nothing to do. To allow Sling to create empty tables, set SLING_ALLOW_EMPTY_TABLES=TRUE")
		return
	} else if cnt > 0 {
		// FIXME: find root cause of why columns don't synch while streaming
		df.SyncColumns()

		// aggregate stats from stream processors
		df.Inferred = !cfg.sourceIsFile() // re-infer is source is file
		df.SyncStats()

		// Checksum Comparison, data quality. Limit to 10k, cause sums get too high
		if df.Count() <= 10000 {
			err = tgtConn.CompareChecksums(cfg.Target.Options.TableTmp, df.Columns)
			if err != nil {
				if os.Getenv("ERROR_ON_CHECKSUM_FAILURE") != "" {
					return
				}
				g.DebugLow(g.ErrMsgSimple(err))
			}
		}
	}

	// need to contain the final write in a transcation after data is loaded
	txOptions := sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: false}
	switch tgtConn.GetType() {
	case dbio.TypeDbSnowflake, dbio.TypeDbDuckDb:
		txOptions = sql.TxOptions{}
	case dbio.TypeDbClickhouse:
		txOptions = sql.TxOptions{Isolation: sql.LevelDefault}
	}
	err = tgtConn.BeginContext(df.Context.Ctx, &txOptions)
	if err != nil {
		err = g.Error(err, "could not open transcation to write to final table")
		return
	}

	defer tgtConn.Rollback() // rollback in case of error

	{
		if cfg.Mode == FullRefreshMode {
			// drop, (create if not exists) and insert directly
			err = tgtConn.DropTable(targetTable)
			if err != nil {
				err = g.Error(err, "could not drop table "+targetTable)
				return cnt, err
			}
			t.SetProgress("dropped table " + targetTable)
		}

		// create table if not exists
		sample := iop.NewDataset(df.Columns)
		sample.Rows = df.Buffer
		sample.Inferred = true // already inferred with SyncStats

		created, err := createTableIfNotExists(
			tgtConn,
			sample,
			targetTable,
			cfg.Target.Options.TableDDL,
		)
		if err != nil {
			err = g.Error(err, "could not create table "+targetTable)
			return cnt, err
		} else if created {
			t.SetProgress("created table %s", targetTable)
		}

		table, err := database.ParseTableName(targetTable, tgtConn.GetType())
		if err != nil {
			return cnt, g.Error(err, "could not get table name for optimization")
		}

		if !created && cfg.Mode != FullRefreshMode {
			if *cfg.Target.Options.AddNewColumns {
				ok, err := database.AddMissingColumns(tgtConn, table, sample.Columns)
				if err != nil {
					return cnt, g.Error(err, "could not add missing columns")
				} else if ok {
					_, err = pullTargetTableColumns(t.Config, tgtConn, true)
					if err != nil {
						return cnt, g.Error(err, "could not get table columns")
					}
				}
			}

			if adjustColumnType {

				table.Columns, err = pullTargetTableColumns(t.Config, tgtConn, false)
				if err != nil {
					return cnt, g.Error(err, "could not get table columns for optimization")
				}

				ok, err := tgtConn.OptimizeTable(&table, sample.Columns)
				if err != nil {
					return cnt, g.Error(err, "could not optimize table schema")
				} else if ok {
					cfg.Target.columns = table.Columns
					for i := range df.Columns {
						df.Columns[i].Type = table.Columns[i].Type
						df.Columns[i].DbType = table.Columns[i].DbType
						for _, ds := range df.StreamMap {
							if len(ds.Columns) == len(df.Columns) {
								ds.Columns[i].Type = table.Columns[i].Type
								ds.Columns[i].DbType = table.Columns[i].DbType
							}
						}
					}
				}
			}
		}
	}

	// Put data from tmp to final
	if cnt == 0 {
		t.SetProgress("0 rows inserted. Nothing to do.")
	} else if cfg.Mode == "drop (need to optimize temp table in place)" {
		// use swap
		err = tgtConn.SwapTable(cfg.Target.Options.TableTmp, targetTable)
		if err != nil {
			err = g.Error(err, "could not swap tables %s to %s", cfg.Target.Options.TableTmp, targetTable)
			return 0, err
		}

		err = tgtConn.DropTable(cfg.Target.Options.TableTmp)
		if err != nil {
			err = g.Error(err, "could not drop table "+cfg.Target.Options.TableTmp)
			return
		}
		t.SetProgress("dropped old table of " + targetTable)

	} else if (cfg.Mode == IncrementalMode && len(t.Config.Source.PrimaryKey()) == 0) || cfg.Mode == SnapshotMode || cfg.Mode == FullRefreshMode {
		// create if not exists and insert directly
		err = insertFromTemp(cfg, tgtConn)
		if err != nil {
			err = g.Error(err, "Could not insert from temp")
			return 0, err
		}
	} else if cfg.Mode == TruncateMode {
		// truncate (create if not exists) and insert directly
		truncSQL := g.R(
			tgtConn.GetTemplateValue("core.truncate_table"),
			"table", targetTable,
		)
		_, err = tgtConn.Exec(truncSQL)
		if err != nil {
			err = g.Error(err, "Could not truncate table: "+targetTable)
			return
		}
		t.SetProgress("truncated table " + targetTable)

		// insert
		err = insertFromTemp(cfg, tgtConn)
		if err != nil {
			err = g.Error(err, "Could not insert from temp")
			// data is still in temp table at this point
			// need to decide whether to drop or keep it for future use
			return 0, err
		}
	} else if cfg.Mode == IncrementalMode || cfg.Mode == BackfillMode {
		// insert in temp
		// create final if not exists
		// delete from final and insert
		// or update (such as merge or ON CONFLICT)
		rowAffCnt, err := tgtConn.Upsert(cfg.Target.Options.TableTmp, targetTable, cfg.Source.PrimaryKey())
		if err != nil {
			err = g.Error(err, "Could not incremental from temp")
			// data is still in temp table at this point
			// need to decide whether to drop or keep it for future use
			return 0, err
		}
		if rowAffCnt > 0 {
			g.DebugLow("%d TOTAL INSERTS / UPDATES", rowAffCnt)
		}
	}

	// post SQL
	if postSQL := cfg.Target.Options.PostSQL; postSQL != "" {
		t.SetProgress("executing post-sql")

		postSQL, err = getSQLText(postSQL)
		if err != nil {
			err = g.Error(err, "Error executing Target.PostSQL. Could not get getSQLText for: "+cfg.Target.Options.PostSQL)
			return cnt, err
		}

		fMap, err := t.Config.GetFormatMap()
		if err != nil {
			err = g.Error(err, "could not get format map for post-sql")
			return cnt, err
		}

		_, err = tgtConn.ExecMulti(g.Rm(postSQL, fMap))
		if err != nil {
			err = g.Error(err, "Error executing Target.PostSQL")
			return cnt, err
		}
	}

	err = tgtConn.Commit()
	if err != nil {
		err = g.Error(err, "could not commit")
		return cnt, err
	}

	err = df.Err()
	return
}
