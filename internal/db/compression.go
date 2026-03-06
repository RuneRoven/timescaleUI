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

// AddCompressionPolicy adds an automatic compression policy.
func AddCompressionPolicy(ctx context.Context, pool *pgxpool.Pool, schema, table, compressAfter string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(table); err != nil {
		return err
	}

	ht := pgx.Identifier{schema, table}.Sanitize()
	sql := fmt.Sprintf("SELECT add_compression_policy(%s, INTERVAL '%s', if_not_exists => true)", ht, compressAfter)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("add compression policy: %w", err)
	}
	return nil
}
