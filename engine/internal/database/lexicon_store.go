package database

import (
	"fmt"
	"log"

	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// LexiconStore manages the high-performance "Gatekeeper Cache" for Lexicon ingestion.
// It handles both Event Names and Event Properties.
type LexiconStore struct {
	DB               *gorm.DB
	eventNameCache   sync.Map // Key: "projectID:eventName" -> Value: eventID (string)
	propertyCache    sync.Map // Key: "projectID:eventName:propertyName" -> Value: true
	projectSettings  sync.Map // Key: projectID -> Value: bool (autoApproveEvents)
	newLexiconQueue  chan lexiconEntry
	newPropertyQueue chan propertyEntry
	flushInterval    time.Duration
	batchSize        int
}

type lexiconEntry struct {
	ProjectID string
	EventName string
}

type propertyEntry struct {
	ProjectID string
	EventName string
	Name      string
	Type      string
}

// Global instance
var Store *LexiconStore

// InitLexiconStore initializes the store, loads the cache, and starts the background worker.
func InitLexiconStore(db *gorm.DB) *LexiconStore {
	store := &LexiconStore{
		DB:               db,
		newLexiconQueue:  make(chan lexiconEntry, 1000),
		newPropertyQueue: make(chan propertyEntry, 2000), // Larger buffer for properties
		flushInterval:    5 * time.Second,
		batchSize:        100,
	}

	store.loadCache()
	go store.processQueue()

	Store = store
	return store
}

// loadCache pre-warms the cache from SQLite.
func (s *LexiconStore) loadCache() {
	// 1. Load Events
	var events []LexiconEvent
	if err := s.DB.Select("id", "project_id", "name").Find(&events).Error; err != nil {
		log.Println("⚠️ Failed to load Lexicon cache:", err)
	} else {
		for _, e := range events {
			key := fmt.Sprintf("%s:%s", e.ProjectID, e.Name)
			s.eventNameCache.Store(key, e.ID)
		}
		log.Printf("✅ Lexicon Cache Loaded: %d events", len(events))
	}

	// 2. Load Properties
	// We need Event Name to form the cache key, so we join with lexicon_events
	rows, err := s.DB.Table("lexicon_event_properties").
		Select("lexicon_event_properties.project_id, lexicon_events.name as event_name, lexicon_event_properties.name").
		Joins("JOIN lexicon_events ON lexicon_event_properties.event_id = lexicon_events.id").
		Rows()

	if err != nil {
		log.Println("⚠️ Failed to load Lexicon Property cache:", err)
	} else {
		defer rows.Close()
		count := 0
		for rows.Next() {
			var pid string
			var eName, pName string
			if err := rows.Scan(&pid, &eName, &pName); err == nil {
				key := fmt.Sprintf("%s:%s:%s", pid, eName, pName)
				s.propertyCache.Store(key, true)
				count++
			}
		}
		log.Printf("✅ Lexicon Property Cache Loaded: %d properties", count)
	}
}

// TrackEvent handles the "Fast Path". Checks RAM, queues if new.
func (s *LexiconStore) TrackEvent(projectIDStr string, eventName string, properties map[string]interface{}) {
	pID := projectIDStr

	// 1. Handle Event Name
	eventKey := fmt.Sprintf("%s:%s", pID, eventName)
	if _, exists := s.eventNameCache.Load(eventKey); !exists {
		// Mark as seen (ID="" means pending/queued) to prevent dup queueing
		s.eventNameCache.Store(eventKey, "")

		select {
		case s.newLexiconQueue <- lexiconEntry{ProjectID: pID, EventName: eventName}:
		default:
			log.Println("⚠️ Lexicon Queue Full! Dropping new event:", eventName)
		}
	}

	// 2. Handle Properties
	for key, val := range properties {
		propKey := fmt.Sprintf("%s:%s:%s", pID, eventName, key)
		if _, exists := s.propertyCache.Load(propKey); !exists {
			s.propertyCache.Store(propKey, true)

			// Infer Type
			propType := "string"
			switch val.(type) {
			case float64, int, int64:
				propType = "number"
			case bool:
				propType = "boolean"
			case map[string]interface{}:
				propType = "object"
			case []interface{}:
				propType = "list"
			}

			select {
			case s.newPropertyQueue <- propertyEntry{
				ProjectID: pID,
				EventName: eventName,
				Name:      key,
				Type:      propType,
			}:
			default:
				// Drop property if queue full
			}
		}
	}
}

// processQueue is the "Slow Path" running in the background.
func (s *LexiconStore) processQueue() {
	var eventBatch []lexiconEntry
	var propBatch []propertyEntry

	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	flushEvents := func() {
		if len(eventBatch) > 0 {
			s.insertEventBatch(eventBatch)
			eventBatch = nil
		}
	}

	flushProps := func() {
		if len(propBatch) > 0 {
			s.insertPropBatch(propBatch)
			propBatch = nil
		}
	}

	log.Println("🚀 Lexicon Gatekeeper Worker Started")

	for {
		select {
		case entry := <-s.newLexiconQueue:
			eventBatch = append(eventBatch, entry)
			if len(eventBatch) >= s.batchSize {
				flushEvents()
			}
		case entry := <-s.newPropertyQueue:
			propBatch = append(propBatch, entry)
			if len(propBatch) >= s.batchSize {
				// Flush props. Ensure events are flushed first?
				// To maximize chances of Event ID availability, we can flush events first if any.
				flushEvents()
				flushProps()
			}
		case <-ticker.C:
			flushEvents()
			flushProps()
		}
	}
}

func (s *LexiconStore) insertEventBatch(batch []lexiconEntry) {
	uniqueEntries := make(map[string]lexiconEntry)
	for _, entry := range batch {
		key := fmt.Sprintf("%s:%s", entry.ProjectID, entry.EventName)
		uniqueEntries[key] = entry
	}

	var eventsToInsert []LexiconEvent
	now := time.Now()

	for _, entry := range uniqueEntries {
		status := s.resolveStatus(entry.ProjectID)
		eventsToInsert = append(eventsToInsert, LexiconEvent{
			ProjectID:   entry.ProjectID,
			Name:        entry.EventName,
			Status:      status,
			CreatedAt:   now,
			UpdatedAt:   now,
			DisplayName: entry.EventName,
		})
	}

	if len(eventsToInsert) > 0 {
		// Insert Ignore
		if err := s.DB.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "project_id"}, {Name: "name"}},
			DoNothing: true,
		}).Create(&eventsToInsert).Error; err != nil {
			log.Println("❌ Failed to insert Lexicon batch:", err)
			return
		}

		log.Printf("📥 Registered %d new Lexicon events", len(eventsToInsert))

		// Post-process: Resolve and Cache IDs for these events so properties can use them
		// We query back the IDs for the events in this batch.

		// Group by ProjectID for querying IDs.
		eventsByProject := make(map[string][]string)
		for _, e := range uniqueEntries {
			eventsByProject[e.ProjectID] = append(eventsByProject[e.ProjectID], e.EventName)
		}

		for pid, names := range eventsByProject {
			var foundEvents []LexiconEvent
			if err := s.DB.Select("id", "name").
				Where("project_id = ? AND name IN ?", pid, names).
				Find(&foundEvents).Error; err == nil {

				for _, fe := range foundEvents {
					key := fmt.Sprintf("%s:%s", pid, fe.Name)
					s.eventNameCache.Store(key, fe.ID)
				}
			}
		}
	}
}

func (s *LexiconStore) insertPropBatch(batch []propertyEntry) {
	uniqueProps := make(map[string]propertyEntry)
	for _, entry := range batch {
		key := fmt.Sprintf("%s:%s:%s", entry.ProjectID, entry.EventName, entry.Name)
		uniqueProps[key] = entry
	}

	var propsToInsert []LexiconEventProperty
	now := time.Now()

	// Need to resolve Event IDs
	for _, entry := range uniqueProps {
		eventKey := fmt.Sprintf("%s:%s", entry.ProjectID, entry.EventName)

		val, ok := s.eventNameCache.Load(eventKey)
		if !ok || val == nil {
			// Should not happen if events flushed first, unless insert failed
			continue
		}

		eventID, _ := val.(string)
		if eventID == "" {
			// Still ""? Means it wasn't resolved in insertEventBatch.
			// Try to resolve one last time (maybe it existed in DB but cache had "")
			// Or just skip to avoid constraint error
			continue
		}

		eID := eventID
		status := s.resolveStatus(entry.ProjectID)
		propsToInsert = append(propsToInsert, LexiconEventProperty{
			ProjectID:   entry.ProjectID,
			EventID:     &eID,
			Name:        entry.Name,
			Status:      status,
			Type:        entry.Type,
			CreatedAt:   now,
			UpdatedAt:   now,
			DisplayName: entry.Name,
		})
	}

	if len(propsToInsert) > 0 {
		err := s.DB.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "project_id"}, {Name: "event_id"}, {Name: "name"}},
			DoNothing: true,
		}).Create(&propsToInsert).Error

		if err != nil {
			log.Println("❌ Failed to insert Lexicon Property batch:", err)
		} else {
			log.Printf("📥 Registered %d new Lexicon properties", len(propsToInsert))
		}
	}
}

// resolveStatus determines the initial status for new Lexicon items based on project settings.
// Returns "approved" if auto-approve is on (default), "pending" if manual review is required.
func (s *LexiconStore) resolveStatus(projectID string) string {
	// Check cache first
	if val, ok := s.projectSettings.Load(projectID); ok {
		if autoApprove, _ := val.(bool); !autoApprove {
			return "pending"
		}
		return "approved"
	}

	// Query DB and cache
	var proj Project
	autoApprove := true // default
	if err := s.DB.Select("auto_approve_events").Where("id = ?", projectID).First(&proj).Error; err == nil {
		autoApprove = proj.AutoApproveEvents
	}
	s.projectSettings.Store(projectID, autoApprove)

	if !autoApprove {
		return "pending"
	}
	return "approved"
}
