package db

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var identifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// intervalRegex allows PostgreSQL interval literals like "7 days", "1 month", "2.5 hours", "00:30:00".
var intervalRegex = regexp.MustCompile(`^[a-zA-Z0-9 .:]+$`)

// ValidateIdentifier checks that a string is a safe SQL identifier.
func ValidateIdentifier(name string) error {
	if !identifierRegex.MatchString(name) {
		return fmt.Errorf("invalid identifier: %q", name)
	}
	return nil
}

// ValidateInterval checks that a string is a safe PostgreSQL interval literal.
func ValidateInterval(interval string) error {
	if interval == "" {
		return fmt.Errorf("interval cannot be empty")
	}
	if !intervalRegex.MatchString(interval) {
		return fmt.Errorf("invalid interval: %q", interval)
	}
	return nil
}

// ValidateColumnList checks that a comma-separated column list contains only valid identifiers
// with optional ASC/DESC suffixes (e.g. "col1, col2 DESC, col3 ASC").
func ValidateColumnList(cols string) error {
	if cols == "" {
		return nil
	}
	for _, part := range strings.Split(cols, ",") {
		token := strings.TrimSpace(part)
		// Strip optional ASC/DESC suffix
		upper := strings.ToUpper(token)
		if strings.HasSuffix(upper, " ASC") {
			token = strings.TrimSpace(token[:len(token)-4])
		} else if strings.HasSuffix(upper, " DESC") {
			token = strings.TrimSpace(token[:len(token)-5])
		}
		if err := ValidateIdentifier(token); err != nil {
			return fmt.Errorf("invalid column in list: %w", err)
		}
	}
	return nil
}

// Hypertable represents a TimescaleDB hypertable.
type Hypertable struct {
	Schema            string
	Name              string
	Owner             string
	NumDimensions     int
	ChunkCount        int
	TotalSize         string
	CompressionStatus string
	RetentionPolicy   string
}

// Index represents a table index.
type Index struct {
	Name       string
	Definition string
}

// HypertableDetail holds detailed info for a single hypertable.
type HypertableDetail struct {
	Hypertable
	Columns       []Column
	Chunks        []Chunk
	Indexes       []Index
	TimeDimension string
	ChunkInterval string
	Compressed    bool
	SegmentBy     string
	OrderBy       string
	ReorderIndex  string
	ReorderJobID  *int
}

// Column represents a table column.
type Column struct {
	Name     string
	Type     string
	Nullable bool
	Default  string
}

// Chunk represents a hypertable chunk.
type Chunk struct {
	Schema        string
	Name          string
	RangeStart    time.Time
	RangeEnd      time.Time
	IsCompressed  bool
	TotalBytes    int64
	TotalSize     string
}

// ListHypertables returns all hypertables with summary info.
func ListHypertables(ctx context.Context, pool *pgxpool.Pool) ([]Hypertable, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			h.hypertable_schema,
			h.hypertable_name,
			h.owner,
			h.num_dimensions,
			(SELECT count(*) FROM timescaledb_information.chunks c
			 WHERE c.hypertable_schema = h.hypertable_schema
			   AND c.hypertable_name = h.hypertable_name),
			pg_size_pretty(hypertable_size(format('%I.%I', h.hypertable_schema, h.hypertable_name)::regclass)),
			CASE WHEN EXISTS (
				SELECT 1 FROM timescaledb_information.compression_settings cs
				WHERE cs.hypertable_schema = h.hypertable_schema
				  AND cs.hypertable_name = h.hypertable_name
			) THEN 'Enabled' ELSE 'Disabled' END,
			COALESCE((
				SELECT j.schedule_interval::text FROM timescaledb_information.jobs j
				WHERE j.hypertable_schema = h.hypertable_schema
				  AND j.hypertable_name = h.hypertable_name
				  AND j.proc_name = 'policy_retention'
				LIMIT 1
			), 'None')
		FROM timescaledb_information.hypertables h
		ORDER BY h.hypertable_schema, h.hypertable_name`)
	if err != nil {
		return nil, fmt.Errorf("list hypertables: %w", err)
	}
	defer rows.Close()

	var result []Hypertable
	for rows.Next() {
		var h Hypertable
		if err := rows.Scan(&h.Schema, &h.Name, &h.Owner, &h.NumDimensions,
			&h.ChunkCount, &h.TotalSize, &h.CompressionStatus, &h.RetentionPolicy); err != nil {
			return nil, fmt.Errorf("scan hypertable: %w", err)
		}
		result = append(result, h)
	}
	return result, rows.Err()
}

// GetHypertable returns detailed info about a specific hypertable.
func GetHypertable(ctx context.Context, pool *pgxpool.Pool, schema, table string) (*HypertableDetail, error) {
	if err := ValidateIdentifier(schema); err != nil {
		return nil, err
	}
	if err := ValidateIdentifier(table); err != nil {
		return nil, err
	}

	d := &HypertableDetail{}
	err := pool.QueryRow(ctx, `
		SELECT h.hypertable_schema, h.hypertable_name, h.owner, h.num_dimensions,
			(SELECT count(*) FROM timescaledb_information.chunks c
			 WHERE c.hypertable_schema = h.hypertable_schema AND c.hypertable_name = h.hypertable_name),
			pg_size_pretty(hypertable_size(format('%I.%I', h.hypertable_schema, h.hypertable_name)::regclass))
		FROM timescaledb_information.hypertables h
		WHERE h.hypertable_schema = $1 AND h.hypertable_name = $2`,
		schema, table).Scan(&d.Schema, &d.Name, &d.Owner, &d.NumDimensions, &d.ChunkCount, &d.TotalSize)
	if err != nil {
		return nil, fmt.Errorf("get hypertable: %w", err)
	}

	// Time dimension
	_ = pool.QueryRow(ctx, `
		SELECT column_name, COALESCE(time_interval::text, integer_interval::text, '')
		FROM timescaledb_information.dimensions
		WHERE hypertable_schema = $1 AND hypertable_name = $2 AND dimension_type = 'Time'
		LIMIT 1`, schema, table).Scan(&d.TimeDimension, &d.ChunkInterval)

	// Compression settings
	var segBy, ordBy *string
	err = pool.QueryRow(ctx, `
		SELECT string_agg(CASE WHEN segmentby_column_index > 0 THEN attname END, ', ' ORDER BY segmentby_column_index),
		       string_agg(CASE WHEN orderby_column_index > 0 THEN attname || ' ' || CASE WHEN orderby_asc THEN 'ASC' ELSE 'DESC' END END, ', ' ORDER BY orderby_column_index)
		FROM timescaledb_information.compression_settings
		WHERE hypertable_schema = $1 AND hypertable_name = $2`,
		schema, table).Scan(&segBy, &ordBy)
	if err == nil {
		if segBy != nil {
			d.SegmentBy = *segBy
			d.Compressed = true
		}
		if ordBy != nil {
			d.OrderBy = *ordBy
			d.Compressed = true
		}
	}
	d.CompressionStatus = "Disabled"
	if d.Compressed {
		d.CompressionStatus = "Enabled"
	}

	// Columns
	colRows, err := pool.Query(ctx, `
		SELECT column_name, data_type, is_nullable = 'YES',
			COALESCE(column_default, '')
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`, schema, table)
	if err == nil {
		defer colRows.Close()
		for colRows.Next() {
			var c Column
			if err := colRows.Scan(&c.Name, &c.Type, &c.Nullable, &c.Default); err == nil {
				d.Columns = append(d.Columns, c)
			}
		}
	}

	// Chunks
	chunkRows, err := pool.Query(ctx, `
		SELECT c.chunk_schema, c.chunk_name,
			c.range_start, c.range_end,
			c.is_compressed,
			COALESCE(pg_total_relation_size(format('%I.%I', c.chunk_schema, c.chunk_name)::regclass), 0),
			pg_size_pretty(COALESCE(pg_total_relation_size(format('%I.%I', c.chunk_schema, c.chunk_name)::regclass), 0))
		FROM timescaledb_information.chunks c
		WHERE c.hypertable_schema = $1 AND c.hypertable_name = $2
		ORDER BY c.range_start DESC`, schema, table)
	if err == nil {
		defer chunkRows.Close()
		for chunkRows.Next() {
			var ch Chunk
			if err := chunkRows.Scan(&ch.Schema, &ch.Name, &ch.RangeStart, &ch.RangeEnd,
				&ch.IsCompressed, &ch.TotalBytes, &ch.TotalSize); err == nil {
				d.Chunks = append(d.Chunks, ch)
			}
		}
	}

	// Indexes
	idxRows, err := pool.Query(ctx, `
		SELECT indexname, indexdef
		FROM pg_indexes
		WHERE schemaname = $1 AND tablename = $2
		ORDER BY indexname`, schema, table)
	if err == nil {
		defer idxRows.Close()
		for idxRows.Next() {
			var idx Index
			if err := idxRows.Scan(&idx.Name, &idx.Definition); err == nil {
				d.Indexes = append(d.Indexes, idx)
			}
		}
	}

	// Reorder policy
	var reorderJobID *int
	var reorderIdx *string
	err = pool.QueryRow(ctx, `
		SELECT j.job_id, j.config->>'index_name'
		FROM timescaledb_information.jobs j
		WHERE j.proc_name = 'policy_reorder'
		  AND j.hypertable_schema = $1
		  AND j.hypertable_name = $2
		LIMIT 1`, schema, table).Scan(&reorderJobID, &reorderIdx)
	if err == nil {
		d.ReorderJobID = reorderJobID
		if reorderIdx != nil {
			d.ReorderIndex = *reorderIdx
		}
	}

	return d, nil
}

// SetChunkTimeInterval changes the chunk time interval for a hypertable.
func SetChunkTimeInterval(ctx context.Context, pool *pgxpool.Pool, schema, table, interval string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(table); err != nil {
		return err
	}
	if err := ValidateInterval(interval); err != nil {
		return err
	}

	ht := pgx.Identifier{schema, table}.Sanitize()
	sql := fmt.Sprintf("SELECT set_chunk_time_interval(%s, INTERVAL '%s')", ht, interval)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("set chunk time interval: %w", err)
	}
	return nil
}

// AddReorderPolicy adds a reorder policy on a hypertable.
func AddReorderPolicy(ctx context.Context, pool *pgxpool.Pool, schema, table, indexName string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(table); err != nil {
		return err
	}
	if err := ValidateIdentifier(indexName); err != nil {
		return err
	}

	ht := pgx.Identifier{schema, table}.Sanitize()
	sql := fmt.Sprintf("SELECT add_reorder_policy(%s, '%s', if_not_exists => true)", ht, indexName)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("add reorder policy: %w", err)
	}
	return nil
}

// RemoveReorderPolicy removes a reorder policy from a hypertable.
func RemoveReorderPolicy(ctx context.Context, pool *pgxpool.Pool, schema, table string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(table); err != nil {
		return err
	}

	ht := pgx.Identifier{schema, table}.Sanitize()
	sql := fmt.Sprintf("SELECT remove_reorder_policy(%s, if_not_exists => true)", ht)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("remove reorder policy: %w", err)
	}
	return nil
}

// CreateIndex creates an index on a hypertable.
func CreateIndex(ctx context.Context, pool *pgxpool.Pool, schema, table, indexName, columns string, unique bool) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(table); err != nil {
		return err
	}
	if err := ValidateIdentifier(indexName); err != nil {
		return err
	}

	ht := pgx.Identifier{schema, table}.Sanitize()
	idxName := pgx.Identifier{indexName}.Sanitize()
	uniqueStr := ""
	if unique {
		uniqueStr = "UNIQUE "
	}
	sql := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)", uniqueStr, idxName, ht, columns)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("create index: %w", err)
	}
	return nil
}

// DropIndex drops an index.
func DropIndex(ctx context.Context, pool *pgxpool.Pool, schema, indexName string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(indexName); err != nil {
		return err
	}

	idx := pgx.Identifier{schema, indexName}.Sanitize()
	sql := fmt.Sprintf("DROP INDEX %s", idx)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("drop index: %w", err)
	}
	return nil
}

// CreateHypertable converts an existing table to a hypertable.
func CreateHypertable(ctx context.Context, pool *pgxpool.Pool, schema, table, timeColumn, chunkInterval string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(table); err != nil {
		return err
	}
	if err := ValidateIdentifier(timeColumn); err != nil {
		return err
	}

	ht := pgx.Identifier{schema, table}.Sanitize()
	col := pgx.Identifier{timeColumn}.Sanitize()

	query := fmt.Sprintf(`SELECT create_hypertable(%s, %s`, ht, col)
	if chunkInterval != "" {
		if err := ValidateInterval(chunkInterval); err != nil {
			return err
		}
		query += fmt.Sprintf(`, chunk_time_interval => INTERVAL '%s'`, chunkInterval)
	}
	query += `, if_not_exists => TRUE, migrate_data => TRUE)`

	_, err := pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("create hypertable: %w", err)
	}
	return nil
}

// ListRegularTables returns non-hypertable user tables that can be converted.
func ListRegularTables(ctx context.Context, pool *pgxpool.Pool) ([]struct{ Schema, Name string }, error) {
	rows, err := pool.Query(ctx, `
		SELECT t.table_schema, t.table_name
		FROM information_schema.tables t
		WHERE t.table_type = 'BASE TABLE'
		  AND t.table_schema NOT IN ('pg_catalog', 'information_schema', '_timescaledb_catalog', '_timescaledb_internal', '_timescaledb_config', '_timescaledb_cache')
		  AND NOT EXISTS (
			SELECT 1 FROM timescaledb_information.hypertables h
			WHERE h.hypertable_schema = t.table_schema AND h.hypertable_name = t.table_name
		  )
		ORDER BY t.table_schema, t.table_name`)
	if err != nil {
		return nil, fmt.Errorf("list regular tables: %w", err)
	}
	defer rows.Close()

	var result []struct{ Schema, Name string }
	for rows.Next() {
		var t struct{ Schema, Name string }
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

// ListTableColumns returns columns for a given table.
func ListTableColumns(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]Column, error) {
	if err := ValidateIdentifier(schema); err != nil {
		return nil, err
	}
	if err := ValidateIdentifier(table); err != nil {
		return nil, err
	}

	rows, err := pool.Query(ctx, `
		SELECT column_name, data_type, is_nullable = 'YES', COALESCE(column_default, '')
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []Column
	for rows.Next() {
		var c Column
		if err := rows.Scan(&c.Name, &c.Type, &c.Nullable, &c.Default); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}
