package api

import (
	"context"
	"log"
	"strconv"

	"sankofa/engine/internal/database"

	"github.com/gofiber/fiber/v2"
)

// GetEventProperties - GET /api/v1/events/properties
func (h *EventsHandler) GetEventProperties(c *fiber.Ctx) error {
	projectIDStr := c.Query("project_id")
	userID, ok := c.Locals("user_id").(float64)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	var project database.Project
	if projectIDStr != "" {
		id, _ := strconv.Atoi(projectIDStr)
		if err := h.DB.First(&project, id).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
		}
		// TODO: Check if user has access to project
	} else {
		var user database.User
		if err := h.DB.First(&user, userID).Error; err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to get user"})
		}
		if user.CurrentProjectID == nil {
			return c.Status(400).JSON(fiber.Map{"error": "No project selected"})
		}
		if err := h.DB.First(&project, *user.CurrentProjectID).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
		}
	}

	environment := c.Query("environment", "live")
	eventName := c.Query("event_name")
	if eventName == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing event_name parameter"})
	}

	// query event property keys
	query := `
		SELECT DISTINCT arrayJoin(mapKeys(properties)) AS key
		FROM events
		WHERE project_id = ? AND environment = ? AND event_name = ?
		ORDER BY key
		LIMIT 1000
	`

	rows, err := h.CH.Query(context.Background(), query,
		strconv.Itoa(int(project.ID)),
		environment,
		eventName,
	)
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

	return c.JSON(fiber.Map{
		"keys": keys,
	})
}
