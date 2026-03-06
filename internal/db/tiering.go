package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TieringInfo holds data tiering information.
type TieringInfo struct {
	Available bool
	Tiers     []TierDetail
}

// TierDetail holds info about a specific tier.
type TierDetail struct {
	HypertableSchema string
	HypertableName   string
	TieredChunks     int
	TieredBytes      int64
	TieredSize       string
}

// GetTieringInfo retrieves tiering information, gracefully handling unavailability.
func GetTieringInfo(ctx context.Context, pool *pgxpool.Pool) (*TieringInfo, error) {
	info := &TieringInfo{}

	// Check if tiering is available by looking for the tiered storage function
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_proc p
			JOIN pg_namespace n ON p.pronamespace = n.oid
			WHERE p.proname = 'tier_chunk' AND n.nspname = 'timescaledb_experimental'
		)`).Scan(&exists)
	if err != nil || !exists {
		info.Available = false
		return info, nil
	}

	info.Available = true

	rows, err := pool.Query(ctx, `
		SELECT
			h.hypertable_schema,
			h.hypertable_name,
			count(*) FILTER (WHERE c.is_compressed),
			COALESCE(sum(pg_total_relation_size(format('%I.%I', c.chunk_schema, c.chunk_name)::regclass)) FILTER (WHERE c.is_compressed), 0),
			pg_size_pretty(COALESCE(sum(pg_total_relation_size(format('%I.%I', c.chunk_schema, c.chunk_name)::regclass)) FILTER (WHERE c.is_compressed), 0))
		FROM timescaledb_information.hypertables h
		JOIN timescaledb_information.chunks c
			ON c.hypertable_schema = h.hypertable_schema
			AND c.hypertable_name = h.hypertable_name
		GROUP BY h.hypertable_schema, h.hypertable_name
		ORDER BY h.hypertable_schema, h.hypertable_name`)
	if err != nil {
		return info, fmt.Errorf("list tiering: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var t TierDetail
		if err := rows.Scan(&t.HypertableSchema, &t.HypertableName,
			&t.TieredChunks, &t.TieredBytes, &t.TieredSize); err != nil {
			return info, err
		}
		info.Tiers = append(info.Tiers, t)
	}
	return info, rows.Err()
}
