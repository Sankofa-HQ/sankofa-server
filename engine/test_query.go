package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "password123",
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	baseQuery := `
                SELECT 
                        p.distinct_id,
                        p.props AS properties,
                        p.latest_seen AS last_seen,
                        groupArray(pa.alias_id) as aliases
                FROM (
                        SELECT 
                                distinct_id,
                                argMax(properties, last_seen) AS props,
                                max(last_seen) AS latest_seen
                        FROM persons
                        WHERE project_id = ? AND environment = ?
                        GROUP BY distinct_id
                ) p
                LEFT JOIN person_aliases pa 
                        ON p.distinct_id = pa.distinct_id
                WHERE 1=1
                AND p.distinct_id IN (SELECT distinct_id FROM cohort_static_members WHERE project_id = ? AND cohort_id = ? GROUP BY distinct_id HAVING sum(sign) > 0)
                GROUP BY p.distinct_id, p.props, p.latest_seen
                ORDER BY p.latest_seen DESC
                LIMIT ? OFFSET ?
	`
	args := []interface{}{
		"proj_IPqmQZEgrYYZfkaqN0Ha7", "test", "proj_IPqmQZEgrYYZfkaqN0Ha7", "coh_3-uKxANia8aEIrDAlxUrI", 200, 0,
	}

	rows, err := conn.Query(context.Background(), baseQuery, args...)
	if err != nil {
		log.Fatal("Query error:", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var distinctID string
		var properties map[string]string
		var lastSeen time.Time
		var aliases []string

		if err := rows.Scan(&distinctID, &properties, &lastSeen, &aliases); err != nil {
			log.Println("Scan error:", err)
			continue
		}
		count++
		fmt.Printf("Row: %s, Props: %d, Aliases: %d\n", distinctID, len(properties), len(aliases))
	}
	fmt.Println("Total Rows Returned:", count)
}
