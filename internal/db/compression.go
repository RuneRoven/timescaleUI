package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CompressionStats holds compression info for a hypertable.
type CompressionStats struct {
	Schema                    string
	Table                     string
	Enabled                   bool
	SegmentBy                 string
	OrderBy                   string
	BeforeCompressionBytes    int64
	AfterCompressionBytes     int64
	BeforeCompressionSize     string
	AfterCompressionSize      string
	Ratio                     string
	NumCompressedChunks       int
	NumUncompressedChunks     int
}

// ListCompressionStats returns compression stats for all hypertables.
func ListCompressionStats(ctx context.Context, pool *pgxpool.Pool) ([]CompressionStats, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			h.hypertable_schema,
			h.hypertable_name,
			EXISTS (
				SELECT 1 FROM timescaledb_information.compression_settings cs
				WHERE cs.hypertable_schema = h.hypertable_schema
				  AND cs.hypertable_name = h.hypertable_name
			),
			(SELECT count(*) FROM timescaledb_information.chunks c
			 WHERE c.hypertable_schema = h.hypertable_schema
			   AND c.hypertable_name = h.hypertable_name AND c.is_compressed = true),
			(SELECT count(*) FROM timescaledb_information.chunks c
			 WHERE c.hypertable_schema = h.hypertable_schema
			   AND c.hypertable_name = h.hypertable_name AND c.is_compressed = false)
		FROM timescaledb_information.hypertables h
		ORDER BY h.hypertable_schema, h.hypertable_name`)
	if err != nil {
		return nil, fmt.Errorf("list compression stats: %w", err)
	}
	defer rows.Close()

	var result []CompressionStats
	for rows.Next() {
		var cs CompressionStats
		if err := rows.Scan(&cs.Schema, &cs.Table, &cs.Enabled,
			&cs.NumCompressedChunks, &cs.NumUncompressedChunks); err != nil {
			return nil, err
		}
		result = append(result, cs)
	}
	return result, rows.Err()
}

// EnableCompression enables compression on a hypertable.
func EnableCompression(ctx context.Context, pool *pgxpool.Pool, schema, table, segmentBy, orderBy string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(table); err != nil {
		return err
	}

	if segmentBy != "" {
		if err := ValidateColumnList(segmentBy); err != nil {
			return fmt.Errorf("invalid segmentby: %w", err)
		}
	}
	if orderBy != "" {
		if err := ValidateColumnList(orderBy); err != nil {
			return fmt.Errorf("invalid orderby: %w", err)
		}
	}

	ht := pgx.Identifier{schema, table}.Sanitize()
	sql := fmt.Sprintf("ALTER TABLE %s SET (timescaledb.compress", ht)
	if segmentBy != "" {
		sql += fmt.Sprintf(", timescaledb.compress_segmentby = '%s'", segmentBy)
	}
	if orderBy != "" {
		sql += fmt.Sprintf(", timescaledb.compress_orderby = '%s'", orderBy)
	}
	sql += ")"

	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("enable compression: %w", err)
	}
	return nil
}

// DisableCompression disables compression on a hypertable.
func DisableCompression(ctx context.Context, pool *pgxpool.Pool, schema, table string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(table); err != nil {
		return err
	}

	ht := pgx.Identifier{schema, table}.Sanitize()
	sql := fmt.Sprintf("ALTER TABLE %s SET (timescaledb.compress = false)", ht)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("disable compression: %w", err)
	}
	return nil
}

// CompressChunks compresses all uncompressed chunks older than the given interval.
func CompressChunks(ctx context.Context, pool *pgxpool.Pool, schema, table string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(table); err != nil {
		return err
	}

	ht := pgx.Identifier{schema, table}.Sanitize()
	sql := fmt.Sprintf("SELECT compress_chunk(i) FROM show_chunks(%s) i WHERE NOT is_compressed", ht)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("compress chunks: %w", err)
	}
	return nil
}

// CompressionDetail holds detailed compression info for a hypertable.
type CompressionDetail struct {
	Schema         string
	Table          string
	Enabled        bool
	SegmentBy      string
	OrderBy        string
	PolicyInterval string
	PolicyJobID    *int
	Chunks         []Chunk
}

// GetCompressionDetail returns detailed compression info for a hypertable.
func GetCompressionDetail(ctx context.Context, pool *pgxpool.Pool, schema, table string) (*CompressionDetail, error) {
	if err := ValidateIdentifier(schema); err != nil {
		return nil, err
	}
	if err := ValidateIdentifier(table); err != nil {
		return nil, err
	}

	d := &CompressionDetail{Schema: schema, Table: table}

	// Check if compression is enabled and get settings
	var segBy, ordBy *string
	err := pool.QueryRow(ctx, `
		SELECT
			string_agg(CASE WHEN segmentby_column_index > 0 THEN attname END, ', ' ORDER BY segmentby_column_index),
			string_agg(CASE WHEN orderby_column_index > 0 THEN attname || ' ' || CASE WHEN orderby_asc THEN 'ASC' ELSE 'DESC' END END, ', ' ORDER BY orderby_column_index)
		FROM timescaledb_information.compression_settings
		WHERE hypertable_schema = $1 AND hypertable_name = $2`,
		schema, table).Scan(&segBy, &ordBy)
	if err == nil {
		if segBy != nil {
			d.SegmentBy = *segBy
			d.Enabled = true
		}
		if ordBy != nil {
			d.OrderBy = *ordBy
			d.Enabled = true
		}
	}

	// Compression policy
	var jobID *int
	var interval *string
	err = pool.QueryRow(ctx, `
		SELECT j.job_id, j.config->>'compress_after'
		FROM timescaledb_information.jobs j
		WHERE j.proc_name = 'policy_compression'
		  AND j.hypertable_schema = $1
		  AND j.hypertable_name = $2
		LIMIT 1`, schema, table).Scan(&jobID, &interval)
	if err == nil {
		d.PolicyJobID = jobID
		if interval != nil {
			d.PolicyInterval = *interval
		}
	}

	// Chunks with compression status
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

	return d, nil
}

// UpdateCompressionSettings changes compression settings (decompress all, disable, re-enable).
func UpdateCompressionSettings(ctx context.Context, pool *pgxpool.Pool, schema, table, segmentBy, orderBy string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(table); err != nil {
		return err
	}

	ht := pgx.Identifier{schema, table}.Sanitize()

	// Decompress all chunks first
	decompSQL := fmt.Sprintf("SELECT decompress_chunk(c) FROM show_chunks(%s) c WHERE is_compressed", ht)
	if _, err := pool.Exec(ctx, decompSQL); err != nil {
		return fmt.Errorf("decompress chunks: %w", err)
	}

	// Disable compression
	disableSQL := fmt.Sprintf("ALTER TABLE %s SET (timescaledb.compress = false)", ht)
	if _, err := pool.Exec(ctx, disableSQL); err != nil {
		return fmt.Errorf("disable compression: %w", err)
	}

	// Re-enable with new settings
	return EnableCompression(ctx, pool, schema, table, segmentBy, orderBy)
}

// RemoveCompressionPolicy removes a compression policy.
func RemoveCompressionPolicy(ctx context.Context, pool *pgxpool.Pool, schema, table string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(table); err != nil {
		return err
	}

	ht := pgx.Identifier{schema, table}.Sanitize()
	sql := fmt.Sprintf("SELECT remove_compression_policy(%s, if_not_exists => true)", ht)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("remove compression policy: %w", err)
	}
	return nil
}

// AddCompressionPolicy adds an automatic compression policy.
func AddCompressionPolicy(ctx context.Context, pool *pgxpool.Pool, schema, table, compressAfter string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(table); err != nil {
		return err
	}

	if err := ValidateInterval(compressAfter); err != nil {
		return err
	}

	ht := pgx.Identifier{schema, table}.Sanitize()
	sql := fmt.Sprintf("SELECT add_compression_policy(%s, INTERVAL '%s', if_not_exists => true)", ht, compressAfter)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("add compression policy: %w", err)
	}
	return nil
}
