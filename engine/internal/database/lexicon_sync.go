package database

import (
	"context"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"gorm.io/gorm"
)

// StartStalenessSyncWorker runs a daily background job to update the last_seen_at
// timestamp for all active Lexicon events by querying ClickHouse.
func StartStalenessSyncWorker(db *gorm.DB, ch driver.Conn) {
	// Start the 24-hour ticker
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	// Quick delay so it doesn't slam the DB exactly at startup
	time.Sleep(10 * time.Second)

	log.Println("🚀 Lexicon Staleness Worker Backgrounded")

	// Fire an initial sync after boot
	syncStaleness(db, ch)

	for range ticker.C {
		syncStaleness(db, ch)
	}
}

func syncStaleness(db *gorm.DB, ch driver.Conn) {
	log.Println("🔄 Running daily Lexicon staleness sync...")

	ctx := context.Background()

	// Ask CH what events fired yesterday, instantly retrieving the tiny aggregated result set.
	query := `
		SELECT tenant_id, event_name, MAX(timestamp) as latest_time 
		FROM events 
		WHERE timestamp >= subtractDays(now(), 1) 
		GROUP BY tenant_id, event_name
	`

	rows, err := ch.Query(ctx, query)
	if err != nil {
		log.Printf("⚠️ Staleness Sync: Failed to query ClickHouse: %v", err)
		return
	}
	defer rows.Close()

	type EventUpdate struct {
		ProjectID string
		EventName string
		Latest    time.Time
	}

	var updates []EventUpdate
	for rows.Next() {
		var u EventUpdate
		if err := rows.Scan(&u.ProjectID, &u.EventName, &u.Latest); err == nil {
			updates = append(updates, u)
		}
	}

	if len(updates) == 0 {
		return // Nothing seen in the last 24h
	}

	// Bulk update SQLite via a transaction.
	tx := db.Begin()
	count := 0

	for _, u := range updates {
		// Update exactly the specified event
		res := tx.Model(&LexiconEvent{}).
			Where("project_id = ? AND name = ?", u.ProjectID, u.EventName).
			Update("last_seen_at", u.Latest)

		if res.Error != nil {
			log.Printf("⚠️ Staleness Sync: Failed to update %s: %v", u.EventName, res.Error)
		} else if res.RowsAffected > 0 {
			count++
		}
	}

	if err := tx.Commit().Error; err != nil {
		log.Printf("⚠️ Staleness Sync: Failed to commit SQLite transaction: %v", err)
	} else {
		log.Printf("✅ Lexicon Staleness Sync Complete. Updated 'last_seen_at' for %d active events.", count)
	}
}
