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

	test := func(query string) {
		row := conn.QueryRow(context.Background(), query)
		var res uint8
		err := row.Scan(&res)
		if err != nil {
			fmt.Printf("Query: %s => Err: %v\n", query, err)
		} else {
			fmt.Printf("Query: %s => OK: %v\n", query, res)
		}
	}

	test("SELECT '(?1)' == '(?1)'")
	test("SELECT '(??1)' == '(??1)'")
	test("SELECT '(\\?1)' == '(\\?1)'")
	test("SELECT toString('(?1)') == '(?1)'")
}
