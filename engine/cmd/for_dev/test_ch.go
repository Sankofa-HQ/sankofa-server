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
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "password123",
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	rows, err := conn.Query(context.Background(), "SELECT toStartOfDay(timestamp) as d, count(*) FROM events GROUP BY d ORDER BY d DESC LIMIT 10")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var d time.Time
		var count uint64
		if err := rows.Scan(&d, &count); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s: %d\n", d.Format("2006-01-02"), count)
	}
}
