package iop

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDuckDb(t *testing.T) {

	t.Run("ExecMultiContext", func(t *testing.T) {
		duck := NewDuckDb(context.Background())
		result, err := duck.ExecMultiContext(
			context.Background(),
			"create table test (id int, name varchar)",
			"insert into test (id, name) values (1, 'John')",
			"insert into test (id, name) values (2, 'Jane')",
		)

		if assert.NoError(t, err) {
			rows, err := result.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, int64(2), rows)
		}
	})

	t.Run("ExecContext with erroneous query", func(t *testing.T) {
		duck := NewDuckDb(context.Background())
		_, err := duck.Exec("SELECT * FROM non_existent_table")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "non_existent_table")
	})

	t.Run("Stream", func(t *testing.T) {

		duck := NewDuckDb(context.Background(), "path=/tmp/test.duckdb")

		// Create a test table and insert some data
		_, err := duck.ExecMultiContext(
			context.Background(),
			"CREATE or replace TABLE export_test (id INT, name VARCHAR, age INT)",
			"INSERT INTO export_test VALUES (1, 'Alice', 30),(2, 'Bob', 25),(3, 'Charlie', 35)",
		)
		assert.NoError(t, err)

		// Test the Export function
		ds, err := duck.StreamContext(
			context.Background(),
			"SELECT * FROM export_test ORDER BY id",
		)
		assert.NoError(t, err)
		assert.NotNil(t, ds)

		// Verify the exported data
		data, err := ds.Collect(0)
		records := data.Records()
		assert.NoError(t, err)
		assert.Equal(t, 3, len(records))

		// Check the content of the first record
		assert.Equal(t, int64(1), records[0]["id"])
		assert.Equal(t, "Alice", records[0]["name"])
		assert.Equal(t, int64(30), records[0]["age"])

		// Check the content of the last record
		assert.Equal(t, int64(3), records[2]["id"])
		assert.Equal(t, "Charlie", records[2]["name"])
		assert.Equal(t, int64(35), records[2]["age"])

		// Clean up: drop the test table
		_, err = duck.Exec("DROP TABLE export_test")
		assert.NoError(t, err)
	})

	t.Run("Query", func(t *testing.T) {
		duck := NewDuckDb(context.Background(), "path=/tmp/test.duckdb")

		// Create a test table and insert some data
		_, err := duck.ExecMultiContext(
			context.Background(),
			"CREATE or replace TABLE query_test (id INT, name VARCHAR, age INT)",
			"INSERT INTO query_test VALUES (1, 'Alice', 30),(2, 'Bob', 25),(3, 'Charlie', 35)",
		)
		assert.NoError(t, err)

		// Test the Query function
		data, err := duck.Query("SELECT * FROM query_test ORDER BY id")
		assert.NoError(t, err)
		assert.NotNil(t, data)

		// Verify the queried data
		assert.Equal(t, 3, len(data.Rows))

		// Check the content of the first row
		assert.Equal(t, int64(1), data.Rows[0][0])
		assert.Equal(t, "Alice", data.Rows[0][1])
		assert.Equal(t, int64(30), data.Rows[0][2])

		// Check the content of the last row
		assert.Equal(t, int64(3), data.Rows[2][0])
		assert.Equal(t, "Charlie", data.Rows[2][1])
		assert.Equal(t, int64(35), data.Rows[2][2])

		// Verify column names
		expectedColumns := []string{"id", "name", "age"}
		actualColumns := data.GetFields()
		assert.Equal(t, expectedColumns, actualColumns)

		// Clean up: drop the test table
		_, err = duck.Exec("DROP TABLE query_test")
		assert.NoError(t, err)
	})
}
