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
	eventNamesStr := c.Query("event_names")

	var targetEvents []string
	if eventNamesStr != "" {
		targetEvents = strings.Split(eventNamesStr, ",")
	} else if eventName != "" {
		targetEvents = []string{eventName}
	}

	var query string
	var queryArgs []interface{}

	if len(targetEvents) == 0 {
		// "All Events" — query properties across ALL events for this project
		// Tag default properties with 'default:' prefix so frontend can distinguish them
		query = `
			SELECT DISTINCT key 
			FROM (
				SELECT arrayJoin(mapKeys(properties)) AS key
				FROM events
				WHERE project_id = ? AND environment = ?
				
				UNION ALL
				
				SELECT concat('default:', arrayJoin(mapKeys(default_properties))) AS key
				FROM events
				WHERE project_id = ? AND environment = ?
			) AS combined
			ORDER BY key
			LIMIT 1000
		`
		queryArgs = []interface{}{project.ID, environment, project.ID, environment}
	} else {
		// Specific event(s) — expand virtual/merged event names into constituent events
		expandedNames := ExpandVirtualEventNames(h.DB, project.ID, targetEvents)
		if len(expandedNames) == 0 {
			expandedNames = targetEvents
		}

		// Build placeholders for the IN clause
		placeholders := make([]string, len(expandedNames))
		queryArgs = make([]interface{}, 0, 2+len(expandedNames)*2)
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

		query = fmt.Sprintf(`
			SELECT DISTINCT key 
			FROM (
				SELECT arrayJoin(mapKeys(properties)) AS key
				FROM events
				WHERE project_id = ? AND environment = ? AND event_name IN (%s)
				
				UNION ALL
				
				SELECT concat('default:', arrayJoin(mapKeys(default_properties))) AS key
				FROM events
				WHERE project_id = ? AND environment = ? AND event_name IN (%s)
			) AS combined
			ORDER BY key
			LIMIT 1000
		`, inClause, inClause)
	}

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
// Keys may have a "default:" prefix to indicate they come from default_properties.
func applyLexiconToEventProperties(db *gorm.DB, projectID string, rawKeys []string) []string {
	if len(rawKeys) == 0 {
		return rawKeys
	}

	rawSet := make(map[string]bool)
	// Build a reverse map: clean name → prefixed key (e.g. "os" → "default:os")
	cleanToRaw := make(map[string]string)
	for _, k := range rawKeys {
		rawSet[k] = true
		clean := k
		if strings.HasPrefix(k, "default:") {
			clean = k[8:]
		}
		cleanToRaw[clean] = k
	}

	var allLexiconProps []database.LexiconEventProperty
	if err := db.Where("project_id = ?", projectID).Find(&allLexiconProps).Error; err != nil {
		return rawKeys // Fallback if DB errs
	}

	hiddenMap := make(map[string]bool)
	mergedTargetsMap := make(map[string]bool) // virtual IDs we need to add

	for _, p := range allLexiconProps {
		// Check if this lexicon property exists in rawSet (with or without prefix)
		prefixedKey, existsInRaw := cleanToRaw[p.Name]

		if p.Hidden && existsInRaw {
			hiddenMap[prefixedKey] = true
		}
		if p.MergedIntoID != nil && *p.MergedIntoID != "" {
			// If a raw property was in our original list, and it's merged into a target,
			// hide the child and ensure the virtual parent is in the list
			if existsInRaw {
				mergedTargetsMap[*p.MergedIntoID] = true
				hiddenMap[prefixedKey] = true // Hide the child that's merged into a virtual
			}
		}
	}

	// Resolve the virtual IDs back to their display names
	if len(mergedTargetsMap) > 0 {
		for _, p := range allLexiconProps {
			if mergedTargetsMap[p.ID] {
				// Virtual/merged properties are event properties (not default), so no prefix
				rawSet[p.Name] = true
			}
		}
	}

	var finalKeys []string
	for k := range rawSet {
		if !hiddenMap[k] {
			finalKeys = append(finalKeys, k)
		}
	}

	return finalKeys
}

// ExpandVirtualPropertyNames takes a property name and checks if it is a virtual/merged property in SQLite.
// If it is, it returns all the raw property names that are merged into it.
// If it is not, it returns a slice containing just the original property name.
func ExpandVirtualPropertyNames(db *gorm.DB, projectID string, propertyName string) []string {
	var parent database.LexiconEventProperty
	if err := db.Where("project_id = ? AND name = ?", projectID, propertyName).First(&parent).Error; err != nil {
		return []string{propertyName} // Not found or not a virtual property
	}

	var children []database.LexiconEventProperty
	if err := db.Where("project_id = ? AND merged_into_id = ?", projectID, parent.ID).Find(&children).Error; err != nil {
		return []string{propertyName} // No children
	}

	if len(children) == 0 {
		return []string{propertyName}
	}

	result := make([]string, 0, len(children)+1)
	result = append(result, propertyName) // Include the parent too, just in case
	for _, child := range children {
		result = append(result, child.Name)
	}

	return result
}
