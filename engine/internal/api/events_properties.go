package api

import (
	"context"
	"fmt"
	"log"
	"strings"

	"sankofa/engine/internal/database"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

// GetEventProperties - GET /api/v1/events/properties
func (h *EventsHandler) GetEventProperties(c *fiber.Ctx) error {
	projectIDStr := c.Query("project_id")
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	var project database.Project
	if projectIDStr != "" {
		if err := h.DB.First(&project, "id = ?", projectIDStr).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
		}
		if !checkProjectAccess(h.DB, &project, userID) {
			return c.Status(403).JSON(fiber.Map{"error": "Access denied to project events"})
		}
	} else {
		var user database.User
		if err := h.DB.First(&user, "id = ?", userID).Error; err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to get user"})
		}
		if user.CurrentProjectID == nil {
			return c.Status(400).JSON(fiber.Map{"error": "No project selected"})
		}
		if err := h.DB.First(&project, "id = ?", *user.CurrentProjectID).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
		}

		if !checkProjectAccess(h.DB, &project, userID) {
			return c.Status(403).JSON(fiber.Map{"error": "Access denied to project events"})
		}
	}

	environment := c.Query("environment", "live")
	eventName := c.Query("event_name")
	if eventName == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing event_name parameter"})
	}

	// Expand virtual/merged event names into their constituent event names
	expandedNames := ExpandVirtualEventNames(h.DB, project.ID, []string{eventName})
	if len(expandedNames) == 0 {
		expandedNames = []string{eventName}
	}

	// Build placeholders for the IN clause
	placeholders := make([]string, len(expandedNames))
	queryArgs := make([]interface{}, 0, 2+len(expandedNames)*2)
	queryArgs = append(queryArgs, project.ID, environment)
	for i, name := range expandedNames {
		placeholders[i] = "?"
		queryArgs = append(queryArgs, name)
	}
	inClause := strings.Join(placeholders, ",")

	// Duplicate args for the UNION ALL second half
	queryArgs = append(queryArgs, project.ID, environment)
	for _, name := range expandedNames {
		queryArgs = append(queryArgs, name)
	}

	// query event property keys from all constituent events
	query := fmt.Sprintf(`
		SELECT DISTINCT key 
		FROM (
			SELECT arrayJoin(mapKeys(properties)) AS key
			FROM events
			WHERE project_id = ? AND environment = ? AND event_name IN (%s)
			
			UNION ALL
			
			SELECT arrayJoin(mapKeys(default_properties)) AS key
			FROM events
			WHERE project_id = ? AND environment = ? AND event_name IN (%s)
		) AS combined
		ORDER BY key
		LIMIT 1000
	`, inClause, inClause)

	rows, err := h.CH.Query(context.Background(), query, queryArgs...)
	if err != nil {
		log.Println("ClickHouse Query Error:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to query event properties"})
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			continue
		}
		keys = append(keys, key)
	}

	keys = applyLexiconToEventProperties(h.DB, project.ID, keys)

	return c.JSON(fiber.Map{
		"keys": keys,
	})
}

// applyLexiconToEventProperties takes a list of raw property keys from ClickHouse,
// maps them against the LexiconEventProperty table in SQLite, hides any properties that are merged
// into a virtual property, and adds the virtual properties themselves if they aren't already included.
func applyLexiconToEventProperties(db *gorm.DB, projectID string, rawKeys []string) []string {
	if len(rawKeys) == 0 {
		return rawKeys
	}

	// Identify unique keys, stripping out the 'prop_' prefix if it exists to query Lexicon
	rawSet := make(map[string]bool)
	for _, k := range rawKeys {
		rawSet[k] = true
	}

	var allLexiconProps []database.LexiconEventProperty
	if err := db.Where("project_id = ?", projectID).Find(&allLexiconProps).Error; err != nil {
		return rawKeys // Fallback if DB errs
	}

	hiddenMap := make(map[string]bool)
	virtualMap := make(map[string]bool)
	mergedTargetsMap := make(map[string]bool) // virtual IDs we need to add

	for _, p := range allLexiconProps {
		if p.Hidden {
			// Also include 'prop_' prefixed version
			hiddenMap[p.Name] = true
			hiddenMap["prop_"+p.Name] = true
		}
		if p.IsVirtual {
			virtualMap[p.Name] = true
			virtualMap["prop_"+p.Name] = true
		}
		if p.MergedIntoID != nil && *p.MergedIntoID != "" {
			// If a raw property was in our original list, and it's merged into a target, we need to ensure the target is in the list
			if rawSet[p.Name] || rawSet["prop_"+p.Name] {
				mergedTargetsMap[*p.MergedIntoID] = true
			}
		}
	}

	// Resolve the virtual IDs back to their names to prepend them
	if len(mergedTargetsMap) > 0 {
		for _, p := range allLexiconProps {
			if mergedTargetsMap[p.ID] {
				rawSet["prop_"+p.Name] = true // Add the virtual one
			}
		}
	}

	var finalKeys []string
	for k := range rawSet {
		// Only include it if it's not hidden (which includes being merged as a child)
		if !hiddenMap[k] {
			finalKeys = append(finalKeys, k)
		}
	}

	return finalKeys
}
