package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"sankofa/engine/internal/database"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type PeopleHandler struct {
	DB *gorm.DB
	CH driver.Conn
}

func NewPeopleHandler(db *gorm.DB, ch driver.Conn) *PeopleHandler {
	return &PeopleHandler{DB: db, CH: ch}
}

type PersonProfile struct {
	DistinctID string            `json:"distinct_id"`
	Properties map[string]string `json:"properties"`
	LastSeen   time.Time         `json:"last_seen"`
	Aliases    []string          `json:"aliases"`
}

// Filter represents a unified filter from the frontend (user property or event)
type Filter struct {
	Id         string `json:"id"`
	FilterType string `json:"filterType"` // "user_property" or "event" or "cohort"

	// User Property Fields
	Property string      `json:"property"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"` // Can be string or number

	// Event Filter Fields
	BehaviorType string `json:"behaviorType"` // "did" or "did_not"
	EventName    string `json:"eventName"`
	Metric       string `json:"metric"`    // "total_events", "aggregate_sum", etc.
	TimeRange    string `json:"timeRange"` // "30d", "24h", etc.
}

// sanitizeKey removes any characters that could cause SQL injection in property key names
func sanitizeKey(key string) string {
	// Only allow alphanumeric, underscore, dollar sign, and dot
	result := make([]byte, 0, len(key))
	for _, b := range []byte(key) {
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b == '$' || b == '.' {
			result = append(result, b)
		}
	}
	return string(result)
}

// ListPeople - GET /api/v1/people
// Params: project_id (opt), limit, offset, search
func (h *PeopleHandler) ListPeople(c *fiber.Ctx) error {
	// 1. Auth & Context
	userID, ok := c.Locals("user_id").(uint)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	// Resolve Project
	var project database.Project
	queryProjectID := c.Query("project_id", "")

	if queryProjectID != "" {
		if err := h.DB.First(&project, queryProjectID).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
		}
	} else {
		// Fallback to user's current project
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

	limit := c.QueryInt("limit", 50)
	if limit > 200 {
		limit = 200
	}
	offset := c.QueryInt("offset", 0)
	search := c.Query("search", "")
	environment := c.Query("environment", "live")

	projID := strconv.Itoa(int(project.ID))

	// 2. Build ClickHouse Query
	baseQuery := `
		SELECT 
			p.distinct_id,
			p.properties,
			p.last_seen,
			groupArray(pa.alias_id) as aliases
		FROM persons p
		LEFT JOIN person_aliases pa 
			ON p.distinct_id = pa.distinct_id 
			AND p.project_id = pa.project_id 
			AND p.environment = pa.environment
		WHERE p.project_id = ? AND p.environment = ?
	`
	args := []interface{}{projID, environment}

	if search != "" {
		baseQuery += " AND (positionCaseInsensitive(p.distinct_id, ?) > 0)"
		args = append(args, search)
	}

	// DEBUG: log initial query
	log.Printf("DEBUG: Initial BaseQuery: %s | Args: %v", baseQuery, args)

	// Parse and apply filters
	filtersJSON := c.Query("filters", "")
	if filtersJSON != "" {
		var filters []Filter
		if err := json.Unmarshal([]byte(filtersJSON), &filters); err == nil {
			for _, f := range filters {

				// --- User Property Filters ---
				if f.FilterType == "user_property" {
					if f.Property == "" {
						continue
					}

					valStr := fmt.Sprintf("%v", f.Value)

					switch f.Operator {
					case "is":
						baseQuery += fmt.Sprintf(" AND p.properties['%s'] = ?", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "is_not":
						baseQuery += fmt.Sprintf(" AND p.properties['%s'] != ?", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "contains":
						baseQuery += fmt.Sprintf(" AND positionCaseInsensitive(p.properties['%s'], ?) > 0", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "does_not_contain":
						baseQuery += fmt.Sprintf(" AND positionCaseInsensitive(p.properties['%s'], ?) = 0", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "is_set":
						baseQuery += fmt.Sprintf(" AND mapContains(p.properties, '%s')", sanitizeKey(f.Property))
					case "is_not_set":
						baseQuery += fmt.Sprintf(" AND NOT mapContains(p.properties, '%s')", sanitizeKey(f.Property))
					case "gt":
						baseQuery += fmt.Sprintf(" AND toFloat64OrZero(p.properties['%s']) > toFloat64OrZero(?)", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "lt":
						baseQuery += fmt.Sprintf(" AND toFloat64OrZero(p.properties['%s']) < toFloat64OrZero(?)", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "gte":
						baseQuery += fmt.Sprintf(" AND toFloat64OrZero(p.properties['%s']) >= toFloat64OrZero(?)", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "lte":
						baseQuery += fmt.Sprintf(" AND toFloat64OrZero(p.properties['%s']) <= toFloat64OrZero(?)", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "eq":
						baseQuery += fmt.Sprintf(" AND p.properties['%s'] = ?", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "neq":
						baseQuery += fmt.Sprintf(" AND p.properties['%s'] != ?", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "in_last":
						// Handle Date "in_last" logic: e.g. "7d"
						// Assuming value is string like "7d", "24h"
						// Check if property is a date string in ISO format?
						// For simplicity, we might interpret Last Seen logic or just timestamp comparison
						// But properties are strings. We need parseDateTimeBestEffort
						durStr := valStr
						var seconds int64
						if len(durStr) > 0 {
							val, _ := strconv.Atoi(durStr[:len(durStr)-1])
							unit := durStr[len(durStr)-1]
							if unit == 'h' {
								seconds = int64(val) * 3600
							}
							if unit == 'd' {
								seconds = int64(val) * 86400
							}
							if unit == 'm' {
								seconds = int64(val) * 60
							} // minutes or months? Assuming minutes for simple parse, but UI usually sends 'd' or 'h'
						}
						if seconds > 0 {
							baseQuery += fmt.Sprintf(" AND parseDateTimeBestEffortOrNull(p.properties['%s']) >= now() - INTERVAL ? SECOND", sanitizeKey(f.Property))
							args = append(args, seconds)
						}
					}
				} else if f.FilterType == "event" {
					// --- Event Filters ---
					// Users who DID / DID NOT do [Event] [Metric Operator Value] in [TimeRange]

					// 1. Calculate time range seconds
					var timeSeconds int64 = 30 * 86400 // Default 30d
					if f.TimeRange == "all" {
						timeSeconds = 0
					} else if len(f.TimeRange) > 0 {
						val, _ := strconv.Atoi(f.TimeRange[:len(f.TimeRange)-1])
						unit := f.TimeRange[len(f.TimeRange)-1]
						switch unit {
						case 'h':
							timeSeconds = int64(val) * 3600
						case 'd':
							timeSeconds = int64(val) * 86400
						case 'm':
							timeSeconds = int64(val) * 86400 * 30 // Approx month
						}
					}

					// 2. Build Subquery for Events
					// SELECT distinct_id FROM events WHERE ... GROUP BY distinct_id HAVING ...

					subQuery := `SELECT distinct_id FROM events WHERE project_id = ? AND environment = ? AND event_name = ?`
					subArgs := []interface{}{projID, environment, f.EventName}

					if timeSeconds > 0 {
						subQuery += ` AND timestamp >= now() - INTERVAL ? SECOND`
						subArgs = append(subArgs, timeSeconds)
					}

					// 3. Aggregation / Having Clause
					havingClause := ""
					metricOp := ""

					// Map operator string to SQL
					switch f.Operator {
					case "gt":
						metricOp = ">"
					case "lt":
						metricOp = "<"
					case "gte":
						metricOp = ">="
					case "lte":
						metricOp = "<="
					case "eq":
						metricOp = "="
					case "neq":
						metricOp = "!="
					default:
						metricOp = ">="
					}

					valFloat := 0.0
					if v, ok := f.Value.(float64); ok {
						valFloat = v
					} else if v, ok := f.Value.(string); ok {
						valFloat, _ = strconv.ParseFloat(v, 64)
					}

					// Determine Metric Expression
					// We use mapUpdate to check both default_properties and properties. Custom properties override defaults.
					propExpr := fmt.Sprintf("toFloat64OrZero(mapUpdate(default_properties, properties)['%s'])", sanitizeKey(f.Property))

					switch f.Metric {
					case "total_events":
						havingClause = fmt.Sprintf("count() %s ?", metricOp)
						subArgs = append(subArgs, valFloat)
					case "aggregate_sum":
						if f.Property != "" {
							havingClause = fmt.Sprintf("sum(%s) %s ?", propExpr, metricOp)
							subArgs = append(subArgs, valFloat)
						}
					case "aggregate_avg":
						if f.Property != "" {
							havingClause = fmt.Sprintf("avg(%s) %s ?", propExpr, metricOp)
							subArgs = append(subArgs, valFloat)
						}
					case "aggregate_min":
						if f.Property != "" {
							havingClause = fmt.Sprintf("min(%s) %s ?", propExpr, metricOp)
							subArgs = append(subArgs, valFloat)
						}
					case "aggregate_max":
						if f.Property != "" {
							havingClause = fmt.Sprintf("max(%s) %s ?", propExpr, metricOp)
							subArgs = append(subArgs, valFloat)
						}
					case "aggregate_distinct_count":
						if f.Property != "" {
							havingClause = fmt.Sprintf("uniqExact(mapUpdate(default_properties, properties)['%s']) %s ?", sanitizeKey(f.Property), metricOp)
							subArgs = append(subArgs, valFloat)
						}
					default:
						// Default to total key count if metric unknown? or just count()
						havingClause = fmt.Sprintf("count() %s ?", metricOp)
						subArgs = append(subArgs, valFloat)
					}

					if havingClause != "" {
						subQuery += " GROUP BY distinct_id HAVING " + havingClause
					} else {
						// If no metric specified (rare), just existence
						subQuery += " GROUP BY distinct_id"
					}

					// 4. Apply to Main Query
					if f.BehaviorType == "did" {
						baseQuery += fmt.Sprintf(" AND p.distinct_id IN (%s)", subQuery)
						args = append(args, subArgs...)
					} else { // did_not
						baseQuery += fmt.Sprintf(" AND p.distinct_id NOT IN (%s)", subQuery)
						args = append(args, subArgs...)
					}
				}
			}
		} else {
			log.Println("Failed to parse filters:", err)
		}
	}

	baseQuery += `
		GROUP BY p.distinct_id, p.properties, p.last_seen
		ORDER BY p.last_seen DESC 
		LIMIT ? OFFSET ?
	`
	args = append(args, limit, offset)

	// DEBUG: log final query
	log.Printf("DEBUG: Final BaseQuery: %s | Args: %v", baseQuery, args)

	rows, err := h.CH.Query(context.Background(), baseQuery, args...)
	if err != nil {
		log.Println("Details: ClickHouse ListPeople Error:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch people"})
	}
	defer rows.Close()

	var people []PersonProfile
	for rows.Next() {
		var p PersonProfile
		if err := rows.Scan(&p.DistinctID, &p.Properties, &p.LastSeen, &p.Aliases); err != nil {
			continue
		}
		// Filter empty aliases
		var cleanAliases []string
		for _, a := range p.Aliases {
			if a != "" {
				cleanAliases = append(cleanAliases, a)
			}
		}
		p.Aliases = cleanAliases
		if p.Aliases == nil {
			p.Aliases = []string{}
		}
		people = append(people, p)
	}

	// Return empty list instead of null
	if people == nil {
		people = []PersonProfile{}
	}

	return c.JSON(fiber.Map{
		"data":   people,
		"limit":  limit,
		"offset": offset,
	})
}

// GetPerson - GET /api/v1/people/:id
func (h *PeopleHandler) GetPerson(c *fiber.Ctx) error {
	distinctID := c.Params("id")
	if distinctID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing distinct_id"})
	}

	// 1. Auth & Context
	userID, ok := c.Locals("user_id").(uint)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	// Resolve Project
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
	projID := strconv.Itoa(int(project.ID))

	// 2. Fetch Person from ClickHouse
	query := `
		SELECT 
			p.distinct_id,
			p.properties,
			p.last_seen,
			groupArray(pa.alias_id) as aliases
		FROM persons p
		LEFT JOIN person_aliases pa 
			ON p.distinct_id = pa.distinct_id 
			AND p.project_id = pa.project_id 
			AND p.environment = pa.environment
		WHERE p.project_id = ? AND p.environment = ? AND p.distinct_id = ?
		GROUP BY p.distinct_id, p.properties, p.last_seen
		LIMIT 1
	`

	var p PersonProfile
	if err := h.CH.QueryRow(context.Background(), query, projID, environment, distinctID).Scan(
		&p.DistinctID, &p.Properties, &p.LastSeen, &p.Aliases,
	); err != nil {
		if err.Error() == "sql: no rows in result set" { // Check standard sql error or driver specific
			return c.Status(404).JSON(fiber.Map{"error": "Person not found"})
		}
		log.Println("Details: ClickHouse GetPerson Error:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch person"})
	}

	// Clean aliases
	var cleanAliases []string
	for _, a := range p.Aliases {
		if a != "" {
			cleanAliases = append(cleanAliases, a)
		}
	}
	p.Aliases = cleanAliases
	if p.Aliases == nil {
		p.Aliases = []string{}
	}

	return c.JSON(fiber.Map{"data": p})
}

// GetPropertyKeys - GET /api/v1/people/properties/keys
// Returns all distinct property keys from the persons table
func (h *PeopleHandler) GetPropertyKeys(c *fiber.Ctx) error {
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
	projID := strconv.Itoa(int(project.ID))

	query := `
		SELECT DISTINCT arrayJoin(mapKeys(properties)) AS key
		FROM persons
		WHERE project_id = ? AND environment = ?
		ORDER BY key
		LIMIT 200
	`

	rows, err := h.CH.Query(context.Background(), query, projID, environment)
	if err != nil {
		log.Println("ClickHouse GetPropertyKeys Error:", err)
		return c.JSON(fiber.Map{"keys": []string{}})
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err == nil && key != "" {
			keys = append(keys, key)
		}
	}

	if keys == nil {
		keys = []string{}
	}

	return c.JSON(fiber.Map{"keys": keys})
}

// GetPropertyValues - GET /api/v1/people/properties/values
// Returns distinct values for a given property key from the persons table
// Params: property (required), search (optional), limit (optional)
func (h *PeopleHandler) GetPropertyValues(c *fiber.Ctx) error {
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
	limit := c.QueryInt("limit", 100)

	if property == "" {
		return c.JSON(fiber.Map{"values": []string{}})
	}

	projID := strconv.Itoa(int(project.ID))

	query := `
		SELECT DISTINCT properties[?] AS val
		FROM persons
		WHERE project_id = ? AND environment = ? AND val != ''
	`
	args := []interface{}{property, projID, environment}

	if search != "" {
		query += " AND positionCaseInsensitive(val, ?) > 0"
		args = append(args, search)
	}

	query += " ORDER BY val LIMIT ?"
	args = append(args, limit)

	rows, err := h.CH.Query(context.Background(), query, args...)
	if err != nil {
		log.Println("ClickHouse GetPropertyValues Error:", err)
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

	if values == nil {
		values = []string{}
	}

	return c.JSON(fiber.Map{
		"values":   values,
		"property": property,
	})
}
