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

func (h *EventsHandler) RegisterRoutes(router fiber.Router, authMiddleware fiber.Handler) {
	events := router.Group("/events", authMiddleware)
	events.Get("/", h.ListEvents)
	events.Get("/names", h.GetEventNames)
	events.Get("/counts", h.GetEventCounts)
	events.Get("/values", h.GetEventValues)
	events.Get("/properties", h.GetEventProperties) // New route
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
	distinctID := c.Query("distinct_id", "")
	startTime := c.Query("start_time", "")
	endTime := c.Query("end_time", "")
	environment := c.Query("environment", "live")

	orgID := strconv.Itoa(int(project.OrganizationID))
	projID := strconv.Itoa(int(project.ID))

	// 1. Build Base WHERE Clause
	whereClause := "WHERE tenant_id = ? AND environment = ?"
	queryArgs := []interface{}{projID, environment}

	if distinctID != "" {
		whereClause += " AND distinct_id = ?"
		queryArgs = append(queryArgs, distinctID)
	}
	if startTime != "" {
		whereClause += " AND timestamp >= parseDateTimeBestEffort(?)"
		queryArgs = append(queryArgs, startTime)
	}
	if endTime != "" {
		whereClause += " AND timestamp <= parseDateTimeBestEffort(?)"
		queryArgs = append(queryArgs, endTime)
	}

	// 2. Parse Event Queries (or Fallback)
	queriesJSON := c.Query("queries", "")
	var eventQueries []struct {
		EventName   string `json:"eventName"`
		IsFirstTime bool   `json:"isFirstTime"`
		Filters     []struct {
			Property    string `json:"property"`
			Value       string `json:"value"`
			Operator    string `json:"operator"`
			Type        string `json:"type"`
			ListType    string `json:"listType"`
			Subkey      string `json:"subkey"`
			Aggregation string `json:"aggregation"`
		} `json:"filters"`
	}

	if queriesJSON != "" {
		if err := json.Unmarshal([]byte(queriesJSON), &eventQueries); err != nil {
			log.Println("Failed to parse queries:", err)
		}
	}

	// Fallback to legacy single query params if no queries provided
	filtersJSON := c.Query("filters", "") // used for fallback parsing
	if len(eventQueries) == 0 {
		eventName := c.Query("event_name", "")

		// Only add if there is at least an event name or filters, or if we want "All Events" (empty eventName is valid for "All")
		var filters []struct {
			Property    string `json:"property"`
			Value       string `json:"value"`
			Operator    string `json:"operator"`
			Type        string `json:"type"`
			ListType    string `json:"listType"`
			Subkey      string `json:"subkey"`
			Aggregation string `json:"aggregation"`
		}
		if filtersJSON != "" {
			json.Unmarshal([]byte(filtersJSON), &filters)
		}

		eventQueries = append(eventQueries, struct {
			EventName   string `json:"eventName"`
			IsFirstTime bool   `json:"isFirstTime"`
			Filters     []struct {
				Property    string `json:"property"`
				Value       string `json:"value"`
				Operator    string `json:"operator"`
				Type        string `json:"type"`
				ListType    string `json:"listType"`
				Subkey      string `json:"subkey"`
				Aggregation string `json:"aggregation"`
			} `json:"filters"`
		}{EventName: eventName, Filters: filters})
	}

	// 3. Append OR Logic for Event Queries
	if len(eventQueries) > 0 {
		var orClauses []string

		for _, q := range eventQueries {
			var andClauses []string

			// Event Name filter
			if q.EventName != "" {
				andClauses = append(andClauses, "event_name = ?")
				queryArgs = append(queryArgs, q.EventName)
			}

			// First Time Filter
			if q.IsFirstTime {
				// Keep only events that created the user-event pairing
				// Using a subquery to find min timestamp for each (distinct_id, event_name)
				andClauses = append(andClauses, "(distinct_id, event_name, timestamp) IN (SELECT distinct_id, event_name, min(timestamp) FROM events GROUP BY distinct_id, event_name)")
			}

			// Property Filters
			for _, f := range q.Filters {
				if f.Property != "" && f.Value != "" {
					key := f.Property
					var col string

					// Determine column and path
					if strings.HasPrefix(key, "prop_") {
						col = fmt.Sprintf("properties['%s']", key[5:])
					} else if key == "event_name" || key == "distinct_id" || key == "lib_version" || key == "timestamp" {
						col = key
					} else {
						col = fmt.Sprintf("default_properties['%s']", key)
					}

					// Handle Object Subkey
					if f.Type == "object" && f.Subkey != "" {
						col = fmt.Sprintf("JSONExtractString(%s, '%s')", col, f.Subkey)
					}

					// Cast value
					castWrapper := func(c string, t string) string {
						switch t {
						case "number":
							return fmt.Sprintf("toFloat64OrZero(%s)", c)
						case "boolean":
							return fmt.Sprintf("(%s = 'true')", c)
						case "date":
							return fmt.Sprintf("parseDateTimeBestEffort(%s)", c)
						default:
							return c
						}
					}
					targetCol := castWrapper(col, f.Type)

					// Aggregation for lists
					if f.Type == "list" && f.Aggregation != "" {
						switch f.Aggregation {
						case "sum":
							targetCol = fmt.Sprintf("arraySum(JSONExtract(%s, 'Array(Float64)'))", col)
						case "avg":
							targetCol = fmt.Sprintf("arrayAvg(JSONExtract(%s, 'Array(Float64)'))", col)
						case "min":
							targetCol = fmt.Sprintf("arrayMin(JSONExtract(%s, 'Array(Float64)'))", col)
						case "max":
							targetCol = fmt.Sprintf("arrayMax(JSONExtract(%s, 'Array(Float64)'))", col)
						case "count":
							targetCol = fmt.Sprintf("length(JSONExtractArrayRaw(%s))", col)
						case "distinct_count":
							targetCol = fmt.Sprintf("length(arrayDistinct(JSONExtractArrayRaw(%s)))", col)
						}
					}

					// Generate SQL based on Operator
					switch f.Operator {
					case ">":
						andClauses = append(andClauses, fmt.Sprintf("%s > ?", targetCol))
						queryArgs = append(queryArgs, f.Value)
					case "<":
						andClauses = append(andClauses, fmt.Sprintf("%s < ?", targetCol))
						queryArgs = append(queryArgs, f.Value)
					case ">=":
						andClauses = append(andClauses, fmt.Sprintf("%s >= ?", targetCol))
						queryArgs = append(queryArgs, f.Value)
					case "<=":
						andClauses = append(andClauses, fmt.Sprintf("%s <= ?", targetCol))
						queryArgs = append(queryArgs, f.Value)
					case "is", "=":
						andClauses = append(andClauses, fmt.Sprintf("%s = ?", targetCol))
						queryArgs = append(queryArgs, f.Value)
					case "is_not", "!=":
						andClauses = append(andClauses, fmt.Sprintf("%s != ?", targetCol))
						queryArgs = append(queryArgs, f.Value)
					case "contains":
						andClauses = append(andClauses, fmt.Sprintf("%s ILIKE ?", targetCol))
						queryArgs = append(queryArgs, "%"+f.Value+"%")
					case "does_not_contain":
						andClauses = append(andClauses, fmt.Sprintf("%s NOT ILIKE ?", targetCol))
						queryArgs = append(queryArgs, "%"+f.Value+"%")
					case "is_set":
						if strings.HasPrefix(key, "prop_") || (!strings.HasPrefix(key, "event_") && !strings.HasPrefix(key, "distinct_") && !strings.HasPrefix(key, "lib_") && !strings.HasPrefix(key, "time")) {
							if f.Type == "object" && f.Subkey != "" {
								andClauses = append(andClauses, fmt.Sprintf("JSONHas(%s, '%s') = 1", strings.TrimSuffix(strings.TrimPrefix(col, "JSONExtractString("), ", '"+f.Subkey+"')"), f.Subkey))
							} else {
								if strings.HasPrefix(key, "prop_") {
									andClauses = append(andClauses, fmt.Sprintf("mapContains(properties, '%s')", key[5:]))
								} else {
									andClauses = append(andClauses, fmt.Sprintf("mapContains(default_properties, '%s')", key))
								}
							}
						} else {
							andClauses = append(andClauses, fmt.Sprintf("%s IS NOT NULL", col))
						}
					case "is_not_set":
						if strings.HasPrefix(key, "prop_") {
							andClauses = append(andClauses, fmt.Sprintf("NOT mapContains(properties, '%s')", key[5:]))
						} else {
							andClauses = append(andClauses, fmt.Sprintf("NOT mapContains(default_properties, '%s')", key))
						}
					case "any_in_list":
						var vals []string
						if err := json.Unmarshal([]byte(f.Value), &vals); err == nil && len(vals) > 0 {
							placeholders := strings.Repeat("?,", len(vals)-1) + "?"
							andClauses = append(andClauses, fmt.Sprintf("%s IN (%s)", targetCol, placeholders))
							for _, v := range vals {
								queryArgs = append(queryArgs, v)
							}
						}
					}
				}
			}

			// Group this query's conditions
			if len(andClauses) > 0 {
				orClauses = append(orClauses, "("+strings.Join(andClauses, " AND ")+")")
			} else if q.EventName == "" {
				// Empty query matches "All Events".
				orClauses = append(orClauses, "(1=1)")
			}
		}

		if len(orClauses) > 0 {
			whereClause += " AND (" + strings.Join(orClauses, " OR ") + ")"
		}
	}

	// 4. Construct Full Queries
	eventsQuery := `
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
	` + whereClause + fmt.Sprintf(" ORDER BY timestamp DESC LIMIT %d OFFSET %d", limit, offset)

	log.Printf("Events Query: org=%s proj=%s env=%s limit=%d offset=%d", orgID, projID, environment, limit, offset)

	// Execute Events Query
	rows, err := h.CH.Query(context.Background(), eventsQuery, queryArgs...)
	if err != nil {
		log.Println("ClickHouse Query Error:", err)
		return c.JSON(fiber.Map{
			"events": []interface{}{},
			"total":  0,
			"limit":  limit,
			"offset": offset,
		})
	}
	defer rows.Close()

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

	// Execute Count Query
	var totalCount uint64
	countQuery := "SELECT count() FROM events " + whereClause

	if err := h.CH.QueryRow(context.Background(), countQuery, queryArgs...).Scan(&totalCount); err != nil {
		log.Println("Count query error:", err)
	}

	// Default empty slice if nil
	if events == nil {
		events = []Event{}
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

	// Fetch from Lexicon (SQLite) - The "Gatekeeper" source of truth
	var lexiconEvents []database.LexiconEvent
	if err := h.DB.Model(&database.LexiconEvent{}).
		Where("project_id = ? AND hidden = ?", project.ID, false).
		Order("name ASC").
		Find(&lexiconEvents).Error; err != nil {
		log.Println("Lexicon Query Error:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to query event names"})
	}

	var eventNames []string
	for _, e := range lexiconEvents {
		eventNames = append(eventNames, e.Name)
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

// GetEventValues - GET /api/v1/events/values
func (h *EventsHandler) GetEventValues(c *fiber.Ctx) error {
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
	property := c.Query("property", "")
	search := c.Query("search", "")
	eventName := c.Query("event_name", "")

	if property == "" {
		return c.JSON(fiber.Map{"values": []string{}})
	}

	// Determine column based on property name
	col := ""
	if strings.HasPrefix(property, "prop_") {
		col = fmt.Sprintf("properties['%s']", property[5:])
	} else if property == "event_name" || property == "distinct_id" || property == "lib_version" || property == "timestamp" {
		col = property
	} else {
		col = fmt.Sprintf("default_properties['%s']", property)
	}

	// Build Query
	query := fmt.Sprintf(`
		SELECT DISTINCT %s as val
		FROM events
		WHERE tenant_id = ? AND environment = ?
	`, col)
	args := []interface{}{strconv.Itoa(int(project.ID)), environment}

	if eventName != "" {
		query += " AND event_name = ?"
		args = append(args, eventName)
	}

	if search != "" {
		query += fmt.Sprintf(" AND %s LIKE ?", col)
		args = append(args, "%%"+search+"%%")
	}

	query += " ORDER BY val LIMIT 100"

	rows, err := h.CH.Query(context.Background(), query, args...)
	if err != nil {
		log.Println("ClickHouse Values Query Error:", err)
		return c.JSON(fiber.Map{"values": []string{}})
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err == nil {
			values = append(values, val)
		}
	}

	return c.JSON(fiber.Map{"values": values})
}
