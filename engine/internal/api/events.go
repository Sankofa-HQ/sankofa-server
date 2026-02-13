package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"sankofa/engine/internal/database"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type EventsHandler struct {
	DB *gorm.DB
	CH driver.Conn
}

func NewEventsHandler(db *gorm.DB, ch driver.Conn) *EventsHandler {
	return &EventsHandler{DB: db, CH: ch}
}

func (h *EventsHandler) RegisterRoutes(router fiber.Router, authMiddleware fiber.Handler) {
	events := router.Group("/events", authMiddleware)
	events.Get("/", h.ListEvents)
	events.Get("/names", h.GetEventNames)
	events.Get("/counts", h.GetEventCounts)
	events.Get("/:id", h.GetEventDetail)
}

// ListEvents - GET /api/v1/events
func (h *EventsHandler) ListEvents(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(uint)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	// Resolve project: prefer query param, fallback to user's current project
	var project database.Project
	queryProjectID := c.Query("project_id", "")

	if queryProjectID != "" {
		// Use project ID from frontend
		if err := h.DB.First(&project, queryProjectID).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
		}
	} else {
		// Fallback to user's current project in DB
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

	// Parse query parameters
	limit := c.QueryInt("limit", 100)
	if limit > 1000 {
		limit = 1000
	}

	offset := c.QueryInt("offset", 0)
	eventName := c.Query("event_name", "")
	distinctID := c.Query("distinct_id", "")
	startTime := c.Query("start_time", "")
	endTime := c.Query("end_time", "")
	environment := c.Query("environment", "live")

	orgID := strconv.Itoa(int(project.OrganizationID))
	projID := strconv.Itoa(int(project.ID))

	// Build ClickHouse query
	query := `
		SELECT 
			timestamp,
			event_name,
			distinct_id,
			properties,
			default_properties,
			lib_version,
			tenant_id,
			project_id,
			organization_id,
			environment
		FROM events
		WHERE tenant_id = ?
			AND environment = ?
	`
	args := []interface{}{projID, environment}

	if eventName != "" {
		query += " AND event_name = ?"
		args = append(args, eventName)
	}
	if distinctID != "" {
		query += " AND distinct_id = ?"
		args = append(args, distinctID)
	}
	if startTime != "" {
		query += " AND timestamp >= parseDateTimeBestEffort(?)"
		args = append(args, startTime)
	}
	if endTime != "" {
		query += " AND timestamp <= parseDateTimeBestEffort(?)"
		args = append(args, endTime)
	}

	// Dynamic Property Filters
	filtersJSON := c.Query("filters", "")
	if filtersJSON != "" {
		var filters []struct {
			Property string `json:"property"`
			Value    string `json:"value"`
			Operator string `json:"operator"`
		}
		if err := json.Unmarshal([]byte(filtersJSON), &filters); err == nil {
			for _, f := range filters {
				if f.Property != "" && f.Value != "" {
					key := f.Property
					if strings.HasPrefix(key, "prop_") {
						query += " AND properties[?] = ?"
						args = append(args, key[5:], f.Value)
					} else {
						switch key {
						case "event_name", "distinct_id", "lib_version":
							query += fmt.Sprintf(" AND %s = ?", key)
							args = append(args, f.Value)
						default:
							query += " AND default_properties[?] = ?"
							args = append(args, key, f.Value)
						}
					}
				}
			}
		}
	}

	// Inline LIMIT/OFFSET (ClickHouse driver doesn't parameterize these well)
	query += fmt.Sprintf(" ORDER BY timestamp DESC LIMIT %d OFFSET %d", limit, offset)

	log.Printf("Events Query: org=%s proj=%s env=%s limit=%d offset=%d", orgID, projID, environment, limit, offset)

	rows, err := h.CH.Query(context.Background(), query, args...)
	if err != nil {
		log.Println("ClickHouse Query Error:", err)
		// Return empty list instead of 500 to keep UI functional
		return c.JSON(fiber.Map{
			"events": []interface{}{},
			"total":  0,
			"limit":  limit,
			"offset": offset,
		})
	}
	defer rows.Close()

	type Event struct {
		Timestamp         time.Time         `json:"timestamp"`
		EventName         string            `json:"event_name"`
		DistinctID        string            `json:"distinct_id"`
		Properties        map[string]string `json:"properties"`
		DefaultProperties map[string]string `json:"default_properties"`
		LibVersion        string            `json:"lib_version"`
		TenantID          string            `json:"tenant_id"`
		ProjectID         string            `json:"project_id"`
		OrganizationID    string            `json:"organization_id"`
		Environment       string            `json:"environment"`
	}

	var events []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(
			&e.Timestamp,
			&e.EventName,
			&e.DistinctID,
			&e.Properties,
			&e.DefaultProperties,
			&e.LibVersion,
			&e.TenantID,
			&e.ProjectID,
			&e.OrganizationID,
			&e.Environment,
		); err != nil {
			log.Println("Row scan error:", err)
			continue
		}
		events = append(events, e)
	}

	// Get total count
	var totalCount uint64
	countQuery := `
		SELECT count() FROM events
		WHERE tenant_id = ? AND environment = ?
	`
	countArgs := []interface{}{projID, environment}
	if eventName != "" {
		countQuery += " AND event_name = ?"
		countArgs = append(countArgs, eventName)
	}
	if distinctID != "" {
		countQuery += " AND distinct_id = ?"
		countArgs = append(countArgs, distinctID)
	}
	if startTime != "" {
		countQuery += " AND timestamp >= parseDateTimeBestEffort(?)"
		countArgs = append(countArgs, startTime)
	}
	if endTime != "" {
		countQuery += " AND timestamp <= parseDateTimeBestEffort(?)"
		countArgs = append(countArgs, endTime)
	}

	if filtersJSON != "" {
		var filters []struct {
			Property string `json:"property"`
			Value    string `json:"value"`
			Operator string `json:"operator"`
		}
		if err := json.Unmarshal([]byte(filtersJSON), &filters); err == nil {
			for _, f := range filters {
				if f.Property != "" && f.Value != "" {
					key := f.Property
					if strings.HasPrefix(key, "prop_") {
						countQuery += " AND properties[?] = ?"
						countArgs = append(countArgs, key[5:], f.Value)
					} else {
						switch key {
						case "event_name", "distinct_id", "lib_version":
							countQuery += fmt.Sprintf(" AND %s = ?", key)
							countArgs = append(countArgs, f.Value)
						default:
							countQuery += " AND default_properties[?] = ?"
							countArgs = append(countArgs, key, f.Value)
						}
					}
				}
			}
		}
	}

	if err := h.CH.QueryRow(context.Background(), countQuery, countArgs...).Scan(&totalCount); err != nil {
		log.Println("Count query error:", err)
	}

	return c.JSON(fiber.Map{
		"events": events,
		"total":  totalCount,
		"limit":  limit,
		"offset": offset,
	})
}

// GetEventNames - GET /api/v1/events/names
func (h *EventsHandler) GetEventNames(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(uint)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	// Resolve project: prefer query param, fallback to user's current project
	var project database.Project
	queryProjectID := c.Query("project_id", "")

	if queryProjectID != "" {
		if err := h.DB.First(&project, queryProjectID).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
		}
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

	// Get unique event names
	query := `
		SELECT DISTINCT event_name
		FROM events
		WHERE tenant_id = ? AND environment = ?
		ORDER BY event_name
	`

	rows, err := h.CH.Query(context.Background(), query,
		strconv.Itoa(int(project.ID)),
		environment,
	)
	if err != nil {
		log.Println("ClickHouse Query Error:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to query event names"})
	}
	defer rows.Close()

	var eventNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		eventNames = append(eventNames, name)
	}

	return c.JSON(fiber.Map{
		"event_names": eventNames,
	})
}

// GetEventDetail - GET /api/v1/events/:id (stub for now)
func (h *EventsHandler) GetEventDetail(c *fiber.Ctx) error {
	// For now, events don't have unique IDs in our schema
	// This would require adding a UUID field to events table
	return c.Status(501).JSON(fiber.Map{"error": "Not implemented yet"})
}

// GetEventCounts - GET /api/v1/events/counts
// Returns event counts grouped by event_name for the last 30 days
func (h *EventsHandler) GetEventCounts(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(uint)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	var project database.Project
	queryProjectID := c.Query("project_id", "")

	if queryProjectID != "" {
		if err := h.DB.First(&project, queryProjectID).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
		}
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
	days := c.QueryInt("days", 30)
	projID := strconv.Itoa(int(project.ID))

	query := `
		SELECT event_name, count() as cnt
		FROM events
		WHERE tenant_id = ?
			AND environment = ?
			AND timestamp >= now() - INTERVAL ? DAY
		GROUP BY event_name
		ORDER BY cnt DESC
	`

	rows, err := h.CH.Query(context.Background(), query, projID, environment, days)
	if err != nil {
		log.Println("ClickHouse counts query error:", err)
		return c.JSON(fiber.Map{"counts": map[string]uint64{}})
	}
	defer rows.Close()

	counts := make(map[string]uint64)
	for rows.Next() {
		var name string
		var cnt uint64
		if err := rows.Scan(&name, &cnt); err != nil {
			continue
		}
		counts[name] = cnt
	}

	return c.JSON(fiber.Map{
		"counts": counts,
		"days":   days,
	})
}
