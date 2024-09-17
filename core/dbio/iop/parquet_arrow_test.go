package iop

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/compress"
	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/schema"
	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func TestDecimal(t *testing.T) {

	bigEndian := []parquet.ByteArray{
		// 123456
		[]byte{1, 226, 64},
		// 987654
		[]byte{15, 18, 6},
		// -123456
		[]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 254, 29, 192},
	}

	g.Info("%#v", DecimalByteArrayToString([]byte(bigEndian[0]), 10, 0))
	g.Info("%#v", DecimalByteArrayToString([]byte(bigEndian[1]), 10, 0))
	g.Info("%#v", DecimalByteArrayToString([]byte(bigEndian[2]), 10, 0))

	g.Warn("%#v", []byte(bigEndian[0]))
	g.Info("%#v", StringToDecimalByteArray("123456", MakeDecNumScale(1), parquet.Types.FixedLenByteArray, 3))
	g.Info("%#v", StringToDecimalByteArray("123456", MakeDecNumScale(1), parquet.Types.ByteArray, -1))

	g.Warn("%#v", []byte(bigEndian[1]))
	g.Info("%#v", StringToDecimalByteArray("987654", MakeDecNumScale(1), parquet.Types.ByteArray, -1))

	g.Warn("%#v", []byte(bigEndian[2]))
	g.Info("%#v", StringToDecimalByteArray("-123456", MakeDecNumScale(1), parquet.Types.ByteArray, -1))
	g.Info("%#v", DecimalByteArrayToString(StringToDecimalByteArray("-123456", MakeDecNumScale(1), parquet.Types.ByteArray, -1), 10, 0))

	precision := 10
	scale := 0
	decValStr := "-123456"
	decValBytes := StringToDecimalByteArray(decValStr, MakeDecNumScale(scale), parquet.Types.ByteArray, -1)
	assert.Equal(t, decValStr, DecimalByteArrayToString(decValBytes, precision, scale))

	precision = 15
	scale = 6
	decValStr = "-123456.789000"
	decValBytes = StringToDecimalByteArray(decValStr, MakeDecNumScale(scale), parquet.Types.ByteArray, -1)
	assert.Equal(t, decValStr, DecimalByteArrayToString(decValBytes, precision, scale))

	precision = 40
	scale = 20
	decValStr = "12345612345600000000.12345612345600000000"
	decValBytes = StringToDecimalByteArray(decValStr, MakeDecNumScale(scale), parquet.Types.ByteArray, 60)
	assert.Equal(t, decValStr, DecimalByteArrayToString(decValBytes, precision, scale))
}

func TestNewParquetReader(t *testing.T) {

	// filePath := "/Users/fritz/__/Git/sling-cli/core/dbio/filesys/test/test1/parquet/test1.1.parquet"
	// filePath := "/Users/fritz/Downloads/green_tripdata_2023-01.parquet"
	filePath := "/Users/fritz/Downloads/fhvhv_tripdata_2023-01.parquet"
	f, err := os.Open(filePath)
	g.LogFatal(err)

	reader, err := file.NewParquetReader(f)
	g.LogFatal(err)

	defer reader.Close()

	p := &ParquetArrowReader{Reader: reader}

	columns := p.Columns()
	selectedColIndices := []int{0, 1}
	// selectedColIndices = lo.Map(columns, func(c Column, i int) int { return i })

	count := 0
	for r := 0; r < reader.NumRowGroups(); r++ {
		rowGroup := reader.RowGroup(r)
		rowGroupMeta := rowGroup.MetaData()

		scanners := make([]*Dumper, len(selectedColIndices))
		fields := make([]string, len(selectedColIndices))

		for i, colI := range selectedColIndices {
			_, err := rowGroupMeta.ColumnChunk(colI)
			if err != nil {
				log.Fatal(err)
			}

			col, err := rowGroup.Column(colI)
			if err != nil {
				log.Fatalf("unable to fetch column=%d err=%s", colI, err)
			}
			scanners[i] = createDumper(col)
			fields[i] = col.Descriptor().Path()
		}

		for {
			done := false
			row := make([]any, len(selectedColIndices))
			for i, scanner := range scanners {
				if val, ok := scanner.Next(); ok {
					switch columns[i].Type {
					case DatetimeType, TimestampType, TimestampzType:
						val, _ = convertTimestamp(columns[i], val)
					}

					switch v := val.(type) {
					case parquet.ByteArray:
						val = v.String()
					case parquet.FixedLenByteArray:
						val = v.String()
					}

					row[i] = val
				} else {
					done = true
				}
			}

			if done {
				break
			}

			count++

			if count < 10 {
				g.Warn("%#v", row)
			} else {
				break
			}
		}
	}

	g.Info("%#v", columns.Types())
	g.Info("count %d", count)
}

func TestNewParquetWriter(t *testing.T) {
	filePath := "/tmp/test.parquet"
	f, err := os.Create(filePath)
	g.LogFatal(err)

	columns := NewColumns(
		Columns{
			{Name: "col_string", Type: StringType},
			{Name: "col_bool", Type: BoolType},
			{Name: "col_bigint", Type: BigIntType},
			{Name: "col_decimal", Type: DecimalType},
			{Name: "col_float", Type: FloatType},
			{Name: "col_json", Type: JsonType},
			{Name: "col_timestamp", Type: TimestampType},
		}...,
	)

	rows := [][]any{
		{
			"hello",             // col_string
			true,                // col_bool
			133332,              // col_bigint
			"12.33",             // col_decimal
			12.33,               // col_float
			`{"msg": "Hello!"}`, // col_json
			time.Now(),          // col_timestamp
		},
		{
			"hello2",          // col_string
			false,             // col_bool
			-987123,           // col_bigint
			"-121.33",         // col_decimal
			-121.33,           // col_float
			`{"msg": "Bye!"}`, // col_json
			time.Now(),        // col_timestamp
		},
		{
			nil, // col_string
			nil, // col_bool
			nil, // col_bigint
			0,   // col_decimal
			nil, // col_float
			nil, // col_json
			nil, // col_timestamp
		},
	}

	data := NewDataset(columns)

	for _, row := range rows {
		data.Append(row)
	}

	pw, err := NewParquetArrowWriter(f, columns, compress.Codecs.Snappy)
	g.LogFatal(err)

	for batch := range data.Stream().BatchChan {
		for row := range batch.Rows {
			err := pw.WriteRow(row)
			g.LogFatal(err)
		}
	}

	err = pw.Close()
	g.LogFatal(err)
}

const defaultBatchSize = 128

type Dumper struct {
	reader         file.ColumnChunkReader
	batchSize      int64
	valueOffset    int
	valuesBuffered int

	levelOffset    int64
	levelsBuffered int64
	defLevels      []int16
	repLevels      []int16

	valueBuffer  interface{}
	valueBufferR reflect.Value
}

func createDumper(reader file.ColumnChunkReader) *Dumper {
	batchSize := defaultBatchSize

	var valueBuffer interface{}
	switch reader.(type) {
	case *file.BooleanColumnChunkReader:
		valueBuffer = make([]bool, batchSize)
	case *file.Int32ColumnChunkReader:
		valueBuffer = make([]int32, batchSize)
	case *file.Int64ColumnChunkReader:
		valueBuffer = make([]int64, batchSize)
	case *file.Float32ColumnChunkReader:
		valueBuffer = make([]float32, batchSize)
	case *file.Float64ColumnChunkReader:
		valueBuffer = make([]float64, batchSize)
	case *file.Int96ColumnChunkReader:
		valueBuffer = make([]parquet.Int96, batchSize)
	case *file.ByteArrayColumnChunkReader:
		valueBuffer = make([]parquet.ByteArray, batchSize)
	case *file.FixedLenByteArrayColumnChunkReader:
		valueBuffer = make([]parquet.FixedLenByteArray, batchSize)
	}

	return &Dumper{
		reader:      reader,
		batchSize:   int64(batchSize),
		defLevels:   make([]int16, batchSize),
		repLevels:   make([]int16, batchSize),
		valueBuffer: valueBuffer,
	}
}
func (dump *Dumper) readNextBatch() {
	switch reader := dump.reader.(type) {
	case *file.BooleanColumnChunkReader:
		values := dump.valueBuffer.([]bool)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Int32ColumnChunkReader:
		values := dump.valueBuffer.([]int32)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Int64ColumnChunkReader:
		values := dump.valueBuffer.([]int64)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Float32ColumnChunkReader:
		values := dump.valueBuffer.([]float32)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Float64ColumnChunkReader:
		values := dump.valueBuffer.([]float64)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Int96ColumnChunkReader:
		values := dump.valueBuffer.([]parquet.Int96)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.ByteArrayColumnChunkReader:
		values := dump.valueBuffer.([]parquet.ByteArray)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.FixedLenByteArrayColumnChunkReader:
		values := dump.valueBuffer.([]parquet.FixedLenByteArray)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	}

	dump.valueOffset = 0
	dump.levelOffset = 0
}

func (dump *Dumper) hasNext() bool {
	return dump.levelOffset < dump.levelsBuffered || dump.reader.HasNext()
}

func (dump *Dumper) FormatValue(val interface{}, width int) string {
	fmtstring := fmt.Sprintf("-%d", width)
	switch val := val.(type) {
	case nil:
		return fmt.Sprintf("%"+fmtstring+"s", "NULL")
	case bool:
		return fmt.Sprintf("%"+fmtstring+"t", val)
	case int32:
		return fmt.Sprintf("%"+fmtstring+"d", val)
	case int64:
		return fmt.Sprintf("%"+fmtstring+"d", val)
	case float32:
		return fmt.Sprintf("%"+fmtstring+"f", val)
	case float64:
		return fmt.Sprintf("%"+fmtstring+"f", val)
	case parquet.Int96:
		if parseInt96AsTimestamp {
			usec := int64(binary.LittleEndian.Uint64(val[:8])/1000) +
				(int64(binary.LittleEndian.Uint32(val[8:]))-2440588)*microSecondsPerDay
			t := time.Unix(usec/1e6, (usec%1e6)*1e3).UTC()
			return fmt.Sprintf("%"+fmtstring+"s", t)
		} else {
			return fmt.Sprintf("%"+fmtstring+"s",
				fmt.Sprintf("%d %d %d",
					binary.LittleEndian.Uint32(val[:4]),
					binary.LittleEndian.Uint32(val[4:]),
					binary.LittleEndian.Uint32(val[8:])))
		}
	case parquet.ByteArray:
		if dump.reader.Descriptor().ConvertedType() == schema.ConvertedTypes.UTF8 {
			return fmt.Sprintf("%"+fmtstring+"s", string(val))
		}
		return fmt.Sprintf("% "+fmtstring+"X", val)
	case parquet.FixedLenByteArray:
		return fmt.Sprintf("% "+fmtstring+"X", val)
	default:
		return fmt.Sprintf("%"+fmtstring+"s", fmt.Sprintf("%v", val))
	}
}

func (dump *Dumper) Next() (interface{}, bool) {
	if dump.levelOffset == dump.levelsBuffered {
		if !dump.hasNext() {
			return nil, false
		}
		dump.readNextBatch()
		if dump.levelsBuffered == 0 {
			return nil, false
		}

		dump.valueBufferR = reflect.ValueOf(dump.valueBuffer)
	}

	defLevel := dump.defLevels[int(dump.levelOffset)]
	// repLevel := dump.repLevels[int(dump.levelOffset)]
	dump.levelOffset++

	if defLevel < dump.reader.Descriptor().MaxDefinitionLevel() {
		return nil, true
	}

	v := dump.valueBufferR.Index(dump.valueOffset).Interface()
	dump.valueOffset++

	return v, true
}
