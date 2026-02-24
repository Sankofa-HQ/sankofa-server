package main

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "password123",
		},
	})
	if err != nil {
		fmt.Println("Connect error:", err)
		return
	}

	rows, err := conn.Query(context.Background(), `
		SELECT 
			arrayJoin(range(1, toUInt32(max_level) + 1)) AS level,
			count(DISTINCT distinct_id) AS users_at_level
		FROM (
			SELECT 'A' AS distinct_id, 3 AS max_level UNION ALL
			SELECT 'B' AS distinct_id, 2 AS max_level UNION ALL
			SELECT 'C' AS distinct_id, 1 AS max_level UNION ALL
			SELECT 'D' AS distinct_id, 3 AS max_level 
		)
		GROUP BY level
		ORDER BY level ASC
	`)
	
	if err != nil {
		fmt.Println("Query error:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var level uint64
		var count uint64
		if err := rows.Scan(&level, &count); err != nil {
			fmt.Println("Scan error:", err)
			continue
		}
		fmt.Printf("Level %d: %d\n", level, count)
	}
}
