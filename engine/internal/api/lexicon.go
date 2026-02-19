package api

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"sankofa/engine/internal/database"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type LexiconHandler struct {
	DB *gorm.DB
	CH driver.Conn
}

func NewLexiconHandler(db *gorm.DB, ch driver.Conn) *LexiconHandler {
	return &LexiconHandler{DB: db, CH: ch}
}

func (h *LexiconHandler) RegisterRoutes(router fiber.Router, authMiddleware fiber.Handler) {
	lexicon := router.Group("/lexicon", authMiddleware)

	// Events
	lexicon.Get("/events", h.ListEvents)
	lexicon.Put("/events/:id", h.UpdateEvent)

	// Event Properties
	lexicon.Get("/event-properties", h.ListEventProperties)
	lexicon.Put("/event-properties/:id", h.UpdateEventProperty)

	// Profile Properties
	lexicon.Get("/profile-properties", h.ListProfileProperties)
	lexicon.Put("/profile-properties/:id", h.UpdateProfileProperty)
}

// --- EVENTS ---

func (h *LexiconHandler) ListEvents(c *fiber.Ctx) error {
	project, err := h.getProjectFromContext(c)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	// 1. Get known events from Postgres
	var dbEvents []database.LexiconEvent
	if err := h.DB.Where("project_id = ?", project.ID).Find(&dbEvents).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch events from DB"})
	}

	// 2. Get raw events from ClickHouse (Sync)
	// We only sync if requested or periodically, but for now let's do a lightweight check
	// actually, for a responsive UI, we should upsert missing ones.

	environment := c.Query("environment", "live")

	// ClickHouse Query for unique event names
	query := `
		SELECT DISTINCT event_name
		FROM events
		WHERE tenant_id = ? AND environment = ?
	`
	rows, err := h.CH.Query(context.Background(), query, fmt.Sprint(project.ID), environment)
	if err != nil {
		log.Println("CH Query Error:", err)
		// Fallback to just returning what we have in DB
	} else {
		defer rows.Close()

		knownMap := make(map[string]bool)
		for _, e := range dbEvents {
			knownMap[e.Name] = true
		}

		var newEvents []database.LexiconEvent

		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err == nil {
				if !knownMap[name] && name != "" {
					newEvents = append(newEvents, database.LexiconEvent{
						ProjectID:   project.ID,
						Name:        name,
						DisplayName: getNameFromKey(name), // Auto-generate display name
						CreatedAt:   time.Now(),
						UpdatedAt:   time.Now(),
					})
				}
			}
		}

		// Bulk Insert new events
		if len(newEvents) > 0 {
			if err := h.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&newEvents).Error; err != nil {
				log.Println("Failed to auto-create events:", err)
			} else {
				// Re-fetch to get IDs
				h.DB.Where("project_id = ?", project.ID).Find(&dbEvents)
			}
		}
	}

	// Sort by name
	sort.Slice(dbEvents, func(i, j int) bool {
		return dbEvents[i].Name < dbEvents[j].Name
	})

	return c.JSON(dbEvents)
}

func (h *LexiconHandler) UpdateEvent(c *fiber.Ctx) error {
	id := c.Params("id")

	// Parse raw JSON to only update fields that were actually sent
	var body map[string]interface{}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}

	updates := map[string]interface{}{"updated_at": time.Now()}

	if v, ok := body["display_name"]; ok {
		updates["display_name"] = v
	}
	if v, ok := body["description"]; ok {
		updates["description"] = v
	}
	if v, ok := body["hidden"]; ok {
		updates["hidden"] = v
	}
	if v, ok := body["dropped"]; ok {
		updates["dropped"] = v
	}
	if v, ok := body["tags"]; ok {
		tagsVal, _ := toStringArray(v)
		serialised, _ := tagsVal.Value()
		updates["tags"] = serialised
	}

	result := h.DB.Model(&database.LexiconEvent{}).Where("id = ?", id).Updates(updates)
	if result.Error != nil {
		log.Println("❌ Update Event Error:", result.Error)
		return c.Status(500).JSON(fiber.Map{"error": "Update failed"})
	}

	return c.JSON(fiber.Map{"success": true})
}

// --- EVENT PROPERTIES ---

func (h *LexiconHandler) ListEventProperties(c *fiber.Ctx) error {
	project, err := h.getProjectFromContext(c)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	var allProps []database.LexiconEventProperty
	if err := h.DB.Where("project_id = ?", project.ID).Find(&allProps).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "DB Error"})
	}

	// Deduplicate properties: Prioritize Global (EventID == nil)
	propMap := make(map[string]database.LexiconEventProperty)
	for _, p := range allProps {
		existing, found := propMap[p.Name]
		if !found {
			propMap[p.Name] = p
		} else {
			// If current p is Global (EventID is nil), it overwrites existing
			// If existing is NOT global but p is, p wins.
			// (Note: *uint is nil for global)
			if p.EventID == nil {
				propMap[p.Name] = p
			} else if existing.EventID != nil {
				// Both are per-event. Prefer one with a display name?
				// For now, keep existing (first found).
				// Or merge?
				if p.DisplayName != "" && existing.DisplayName == "" {
					propMap[p.Name] = p
				}
			}
		}
	}

	// Convert map back to slice
	var props []database.LexiconEventProperty
	for _, p := range propMap {
		props = append(props, p)
	}

	// Sync with ClickHouse
	environment := c.Query("environment", "live")

	// Query to get keys from recent events
	// Using mapKeys to get property names
	query := `
		SELECT DISTINCT arrayJoin(mapKeys(properties)) as key
		FROM events
		WHERE tenant_id = ? AND environment = ?
		LIMIT 1000
	`
	rows, err := h.CH.Query(context.Background(), query, fmt.Sprint(project.ID), environment)
	if err != nil {
		log.Println("⚠️ ClickHouse Property Sync Error:", err)
		// Return what we have in DB even if sync fails
		return c.JSON(props)
	}
	defer rows.Close()

	knownMap := make(map[string]bool)
	for _, p := range props {
		knownMap[p.Name] = true
	}

	var newProps []database.LexiconEventProperty
	seenInBatch := make(map[string]bool)

	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err == nil {
			if !knownMap[key] && !seenInBatch[key] && key != "" {
				seenInBatch[key] = true

				// Infer Type (Default to string for now, expensive to query values)
				pType := "string"

				newProps = append(newProps, database.LexiconEventProperty{
					ProjectID:   project.ID,
					Name:        key,
					DisplayName: getNameFromKey(key),
					Type:        pType,
					CreatedAt:   time.Now(),
					UpdatedAt:   time.Now(),
				})
			}
		}
	}

	if len(newProps) > 0 {
		log.Printf("Found %d new event properties", len(newProps))
		if err := h.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&newProps).Error; err != nil {
			log.Println("Failed to save new properties:", err)
		} else {
			// Refresh list
			h.DB.Where("project_id = ?", project.ID).Find(&props)
		}
	}

	return c.JSON(props)
}

func (h *LexiconHandler) UpdateEventProperty(c *fiber.Ctx) error {
	id := c.Params("id")

	var body map[string]interface{}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}

	updates := map[string]interface{}{"updated_at": time.Now()}

	if v, ok := body["display_name"]; ok {
		updates["display_name"] = v
	}
	if v, ok := body["description"]; ok {
		updates["description"] = v
	}
	if v, ok := body["example_value"]; ok {
		updates["example_value"] = v
	}
	if v, ok := body["hidden"]; ok {
		updates["hidden"] = v
	}
	if v, ok := body["dropped"]; ok {
		updates["dropped"] = v
	}
	if v, ok := body["type"]; ok {
		updates["type"] = v
	}
	if v, ok := body["tags"]; ok {
		tagsVal, _ := toStringArray(v)
		serialised, _ := tagsVal.Value()
		updates["tags"] = serialised
	}

	result := h.DB.Model(&database.LexiconEventProperty{}).Where("id = ?", id).Updates(updates)
	if result.Error != nil {
		log.Println("❌ Update Event Property Error:", result.Error)
		return c.Status(500).JSON(fiber.Map{"error": "Update failed"})
	}
	return c.JSON(fiber.Map{"success": true})
}

// --- PROFILE PROPERTIES ---

func (h *LexiconHandler) ListProfileProperties(c *fiber.Ctx) error {
	project, err := h.getProjectFromContext(c)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	entityType := c.Query("type", "User") // User or Company

	var props []database.LexiconProfileProperty
	if err := h.DB.Where("project_id = ? AND entity_type = ?", project.ID, entityType).Find(&props).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "DB Error"})
	}

	// Sync from 'persons' table
	environment := c.Query("environment", "live")

	// Only sync User properties for now, Company requires different logic/table
	if entityType == "User" {
		query := `
			SELECT arrayJoin(mapKeys(properties)) as key
			FROM persons
			WHERE tenant_id = ? AND environment = ?
			LIMIT 1000
		`
		rows, err := h.CH.Query(context.Background(), query, fmt.Sprint(project.ID), environment)
		if err == nil {
			defer rows.Close()
			knownMap := make(map[string]bool)
			for _, p := range props {
				knownMap[p.Name] = true
			}

			var newProps []database.LexiconProfileProperty
			seenInBatch := make(map[string]bool)

			for rows.Next() {
				var key string
				if err := rows.Scan(&key); err == nil {
					if !knownMap[key] && !seenInBatch[key] && key != "" {
						seenInBatch[key] = true
						newProps = append(newProps, database.LexiconProfileProperty{
							ProjectID:   project.ID,
							EntityType:  "User",
							Name:        key,
							DisplayName: getNameFromKey(key),
							Type:        "string",
							CreatedAt:   time.Now(),
							UpdatedAt:   time.Now(),
						})
					}
				}
			}

			if len(newProps) > 0 {
				h.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&newProps)
				h.DB.Where("project_id = ? AND entity_type = ?", project.ID, entityType).Find(&props)
			}
		}
	}

	return c.JSON(props)
}

func (h *LexiconHandler) UpdateProfileProperty(c *fiber.Ctx) error {
	id := c.Params("id")

	var body map[string]interface{}
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}

	updates := map[string]interface{}{"updated_at": time.Now()}

	if v, ok := body["display_name"]; ok {
		updates["display_name"] = v
	}
	if v, ok := body["description"]; ok {
		updates["description"] = v
	}
	if v, ok := body["example_value"]; ok {
		updates["example_value"] = v
	}
	if v, ok := body["hidden"]; ok {
		updates["hidden"] = v
	}
	if v, ok := body["dropped"]; ok {
		updates["dropped"] = v
	}
	if v, ok := body["type"]; ok {
		updates["type"] = v
	}
	if v, ok := body["tags"]; ok {
		tagsVal, _ := toStringArray(v)
		serialised, _ := tagsVal.Value()
		updates["tags"] = serialised
	}

	result := h.DB.Model(&database.LexiconProfileProperty{}).Where("id = ?", id).Updates(updates)
	if result.Error != nil {
		log.Println("❌ Update Profile Property Error:", result.Error)
		return c.Status(500).JSON(fiber.Map{"error": "Update failed"})
	}
	return c.JSON(fiber.Map{"success": true})
}

// --- HELPERS ---

func (h *LexiconHandler) getProjectFromContext(c *fiber.Ctx) (*database.Project, error) {
	// This assumes AuthMiddleware has run and set user_id
	// And we are either passed a project_id or we use the user's current project

	// Re-using logic from other handlers or making a helper
	// For now, duplicate safe logic:

	userID, ok := c.Locals("user_id").(uint)
	if !ok {
		return nil, fmt.Errorf("Unauthorized")
	}

	var project database.Project
	queryProjectID := c.Query("project_id", "")

	if queryProjectID != "" {
		if err := h.DB.First(&project, queryProjectID).Error; err != nil {
			return nil, fmt.Errorf("Project not found")
		}
	} else {
		var user database.User
		if err := h.DB.First(&user, userID).Error; err != nil {
			return nil, fmt.Errorf("User not found")
		}
		if user.CurrentProjectID == nil {
			return nil, fmt.Errorf("No project selected")
		}
		if err := h.DB.First(&project, *user.CurrentProjectID).Error; err != nil {
			return nil, fmt.Errorf("Project not found")
		}
	}
	return &project, nil
}

// toStringArray converts a JSON-parsed interface{} (typically []interface{}) to our StringArray.
func toStringArray(v interface{}) (database.StringArray, error) {
	result := database.StringArray{}
	if v == nil {
		return result, nil
	}
	switch arr := v.(type) {
	case []interface{}:
		for _, item := range arr {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
	case []string:
		result = database.StringArray(arr)
	}
	return result, nil
}

// getNameFromKey "signup_completed" -> "Signup Completed"
func getNameFromKey(key string) string {
	parts := strings.Split(key, "_")
	for i, p := range parts {
		if len(p) > 0 {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, " ")
}
