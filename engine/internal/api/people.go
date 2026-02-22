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
	DisplayID  string            `json:"display_id"` // The latest user-facing identity
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
	Value    interface{} `json:"value"`    // Can be string or number
	CohortID interface{} `json:"cohortId"` // Used by modern frontend for cohort filtering

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

// resolveDisplayID follows the alias chain FORWARD to find the latest user-facing identity.
// The alias table stores: alias_id = previousID, distinct_id = newUserID
// So starting from the anonymous UUID, we follow alias_id → distinct_id to get the latest ID.
func (h *PeopleHandler) resolveDisplayID(projID, environment, canonicalID string) string {
	currentID := canonicalID
	visited := map[string]bool{currentID: true}

	for i := 0; i < 10; i++ {
		var nextID string
		err := h.CH.QueryRow(context.Background(),
			"SELECT distinct_id FROM person_aliases WHERE project_id = ? AND environment = ? AND alias_id = ? LIMIT 1",
			projID, environment, currentID,
		).Scan(&nextID)

		if err != nil || nextID == "" || visited[nextID] {
			break
		}
		visited[nextID] = true
		currentID = nextID
	}
	return currentID
}

// ListPeople - GET /api/v1/people
// Params: project_id (opt), limit, offset, search
func (h *PeopleHandler) ListPeople(c *fiber.Ctx) error {
	// 1. Auth & Context
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	// Resolve Project
	var project database.Project
	queryProjectID := c.Query("project_id", "")

	if queryProjectID != "" {
		if err := h.DB.First(&project, "id = ?", queryProjectID).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
		}
	} else {
		// Fallback to user's current project
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
	}

	limit := c.QueryInt("limit", 50)
	if limit > 200 {
		limit = 200
	}
	offset := c.QueryInt("offset", 0)
	search := c.Query("search", "")
	environment := c.Query("environment", "live")

	projID := project.ID

	// 2. Build ClickHouse Query
	// Use a subquery with argMax to deduplicate ReplacingMergeTree rows
	// This ensures we get only the latest profile per distinct_id
	baseQuery := `
		SELECT 
			p.distinct_id,
			p.props AS properties,
			p.latest_seen AS last_seen,
			groupArray(pa.alias_id) as aliases
		FROM (
			SELECT 
				distinct_id,
				argMax(properties, last_seen) AS props,
				max(last_seen) AS latest_seen
			FROM persons
			WHERE project_id = ? AND environment = ?
			GROUP BY distinct_id
		) p
		LEFT JOIN person_aliases pa 
			ON p.distinct_id = pa.distinct_id
		WHERE 1=1
	`
	args := []interface{}{projID, environment}

	if search != "" {
		// Search both distinct_id and aliases (resolve alias -> distinct_id)
		baseQuery += ` AND (
			positionCaseInsensitive(p.distinct_id, ?) > 0 
			OR p.distinct_id IN (
				SELECT alias_id FROM person_aliases 
				WHERE positionCaseInsensitive(distinct_id, ?) > 0
			)
		)`
		args = append(args, search, search)
	}

	// DEBUG: log initial query
	log.Printf("DEBUG: Initial BaseQuery: %s | Args: %v", baseQuery, args)

	// Parse and apply filters
	filtersJSON := c.Query("filters", "")
	if filtersJSON != "" {
		log.Printf("DEBUG ListPeople: Raw filtersJSON: %s", filtersJSON)
		var filters []Filter
		if err := json.Unmarshal([]byte(filtersJSON), &filters); err == nil {
			log.Printf("DEBUG ListPeople: Parsed filters: %+v", filters)
			for _, f := range filters {

				// --- User Property Filters ---
				if f.FilterType == "user_property" {
					if f.Property == "" {
						continue
					}

					valStr := fmt.Sprintf("%v", f.Value)

					switch f.Operator {
					case "is":
						baseQuery += fmt.Sprintf(" AND p.props['%s'] = ?", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "is_not":
						baseQuery += fmt.Sprintf(" AND p.props['%s'] != ?", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "contains":
						baseQuery += fmt.Sprintf(" AND positionCaseInsensitive(p.props['%s'], ?) > 0", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "does_not_contain":
						baseQuery += fmt.Sprintf(" AND positionCaseInsensitive(p.props['%s'], ?) = 0", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "is_set":
						baseQuery += fmt.Sprintf(" AND mapContains(p.props, '%s')", sanitizeKey(f.Property))
					case "is_not_set":
						baseQuery += fmt.Sprintf(" AND NOT mapContains(p.props, '%s')", sanitizeKey(f.Property))
					case "gt":
						baseQuery += fmt.Sprintf(" AND toFloat64OrZero(p.props['%s']) > toFloat64OrZero(?)", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "lt":
						baseQuery += fmt.Sprintf(" AND toFloat64OrZero(p.props['%s']) < toFloat64OrZero(?)", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "gte":
						baseQuery += fmt.Sprintf(" AND toFloat64OrZero(p.props['%s']) >= toFloat64OrZero(?)", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "lte":
						baseQuery += fmt.Sprintf(" AND toFloat64OrZero(p.props['%s']) <= toFloat64OrZero(?)", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "eq":
						baseQuery += fmt.Sprintf(" AND p.props['%s'] = ?", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "neq":
						baseQuery += fmt.Sprintf(" AND p.props['%s'] != ?", sanitizeKey(f.Property))
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
							baseQuery += fmt.Sprintf(" AND parseDateTimeBestEffortOrNull(p.props['%s']) >= now() - INTERVAL ? SECOND", sanitizeKey(f.Property))
							args = append(args, seconds)
						}
					}
				} else if f.FilterType == "event" {
					// --- Event Filters ---
					// Users who DID / DID NOT do [Event] [Metric Operator Value] in [TimeRange]

					// 1. Calculate time range sql
					timeSql, timeArgs := ParseTimeRangeSql("timestamp", f.TimeRange)
					if timeSql == "" && f.TimeRange != "all" {
						timeSql, timeArgs = ParseTimeRangeSql("timestamp", "30d") // Default
					}

					// 2. Build Subquery for Events
					// SELECT distinct_id FROM events WHERE ... GROUP BY distinct_id HAVING ...

					subQuery := `SELECT distinct_id FROM events WHERE project_id = ? AND environment = ? AND event_name = ?`
					subArgs := []interface{}{projID, environment, f.EventName}

					if timeSql != "" {
						subQuery += " AND " + timeSql
						subArgs = append(subArgs, timeArgs...)
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
				} else if f.FilterType == "cohort" {
					// --- Cohort Filters ---
					cohortIDStr := fmt.Sprintf("%v", f.Value)
					if f.CohortID != nil {
						cohortIDStr = fmt.Sprintf("%v", f.CohortID)
					}
					var cohort database.Cohort
					if err := h.DB.First(&cohort, "id = ?", cohortIDStr).Error; err != nil {
						log.Println("Cohort not found:", cohortIDStr)
						continue
					}

					var cohortSQL string
					var cohortArgs []interface{}

					if cohort.Type == "static" {
						// Static Cohort: Use ClickHouse table with CollapsingMergeTree logic
						cohortSQL = "SELECT distinct_id FROM cohort_static_members WHERE project_id = ? AND cohort_id = ? GROUP BY distinct_id HAVING sum(sign) > 0"
						cohortArgs = []interface{}{projID, cohort.ID}
					} else {
						// Dynamic Cohort: Parse AST and Build SQL
						var ast CohortAST
						if err := json.Unmarshal(cohort.Rules, &ast); err == nil {
							cohortSQL, cohortArgs = BuildCohortSQL(h.DB, projID, environment, ast)
						} else {
							log.Println("Failed to parse cohort rules:", err)
						}
					}

					if cohortSQL != "" {
						if f.BehaviorType == "did_not" { // exclude cohort (rare but possible)
							baseQuery += fmt.Sprintf(" AND p.distinct_id NOT IN (%s)", cohortSQL)
						} else {
							baseQuery += fmt.Sprintf(" AND p.distinct_id IN (%s)", cohortSQL)
						}
						args = append(args, cohortArgs...)
					}
				}
			}
		} else {
			log.Printf("Failed to parse filters: %v. Raw json: %s", err, filtersJSON)
		}
	}

	baseQuery += `
		GROUP BY p.distinct_id, p.props, p.latest_seen
		ORDER BY p.latest_seen DESC
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

		// Resolve display_id for list view
		p.DisplayID = h.resolveDisplayID(projID, environment, p.DistinctID)

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
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	// Resolve Project
	var project database.Project
	queryProjectID := c.Query("project_id", "")

	if queryProjectID != "" {
		if err := h.DB.First(&project, "id = ?", queryProjectID).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
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
	}

	environment := c.Query("environment", "live")
	projID := project.ID

	// 2. Resolve alias chain: follow alias_id -> distinct_id links recursively
	// The SDK creates aliases like: alias_id = previousID, distinct_id = newUserID
	// So the chain is: user_88 -> user_304 -> user_532 -> anonymous_uuid (person row)
	// We need to follow the chain: given distinct_id in aliases, get alias_id, repeat
	canonicalID := distinctID

	// First, check if the person exists directly with this distinct_id
	var directCount uint64
	h.CH.QueryRow(context.Background(),
		"SELECT count() FROM persons WHERE project_id = ? AND environment = ? AND distinct_id = ?",
		projID, environment, distinctID,
	).Scan(&directCount)

	if directCount == 0 {
		// Person doesn't exist directly — resolve alias chain (max 10 hops to prevent loops)
		currentID := distinctID
		visited := map[string]bool{currentID: true}

		for i := 0; i < 10; i++ {
			var resolvedID string
			err := h.CH.QueryRow(context.Background(),
				"SELECT alias_id FROM person_aliases WHERE project_id = ? AND environment = ? AND distinct_id = ? LIMIT 1",
				projID, environment, currentID,
			).Scan(&resolvedID)

			if err != nil || resolvedID == "" {
				break // No more aliases to follow
			}
			if visited[resolvedID] {
				break // Prevent infinite loop
			}

			visited[resolvedID] = true
			currentID = resolvedID
			log.Printf("Alias chain: %s -> %s", canonicalID, currentID)
		}
		canonicalID = currentID
		if canonicalID != distinctID {
			log.Printf("Alias fully resolved: %s -> %s", distinctID, canonicalID)
		}
	}

	// 3. Fetch Person from ClickHouse (deduplicated via argMax)
	query := `
		SELECT 
			p.distinct_id,
			p.props AS properties,
			p.latest_seen AS last_seen,
			groupArray(pa.alias_id) as aliases
		FROM (
			SELECT 
				distinct_id,
				argMax(properties, last_seen) AS props,
				max(last_seen) AS latest_seen
			FROM persons
			WHERE project_id = ? AND environment = ? AND distinct_id = ?
			GROUP BY distinct_id
		) p
		LEFT JOIN person_aliases pa 
			ON p.distinct_id = pa.distinct_id
		GROUP BY p.distinct_id, p.props, p.latest_seen
		LIMIT 1
	`

	var p PersonProfile
	if err := h.CH.QueryRow(context.Background(), query, projID, environment, canonicalID).Scan(
		&p.DistinctID, &p.Properties, &p.LastSeen, &p.Aliases,
	); err != nil {
		// Person not found in persons table — try to create a stub from events
		log.Printf("Person %s (canonical: %s) not in persons table, checking events...", distinctID, canonicalID)

		// Check if this distinct_id (or any ID in the alias chain) has events
		idsToCheck := []string{distinctID, canonicalID}
		var lastSeen time.Time
		var foundDistinctID string

		for _, checkID := range idsToCheck {
			err2 := h.CH.QueryRow(context.Background(),
				"SELECT distinct_id, max(timestamp) FROM events WHERE project_id = ? AND environment = ? AND distinct_id = ? GROUP BY distinct_id",
				projID, environment, checkID,
			).Scan(&foundDistinctID, &lastSeen)
			if err2 == nil {
				break
			}
		}

		if foundDistinctID != "" {
			// Found events for this user, create stub profile
			p = PersonProfile{
				DistinctID: distinctID, // Use the originally requested ID
				Properties: map[string]string{},
				LastSeen:   lastSeen,
				Aliases:    []string{},
			}

			// Collect all known aliases for this user
			aliasRows, err := h.CH.Query(context.Background(),
				"SELECT alias_id, distinct_id FROM person_aliases WHERE project_id = ? AND environment = ? AND (alias_id = ? OR distinct_id = ?)",
				projID, environment, canonicalID, distinctID,
			)
			if err == nil {
				defer aliasRows.Close()
				for aliasRows.Next() {
					var aid, did string
					if aliasRows.Scan(&aid, &did) == nil {
						if aid != distinctID {
							p.Aliases = append(p.Aliases, aid)
						}
						if did != distinctID {
							p.Aliases = append(p.Aliases, did)
						}
					}
				}
			}

			return c.JSON(fiber.Map{"data": p})
		}

		return c.Status(404).JSON(fiber.Map{"error": "Person not found"})
	}

	// 4. Resolve display_id: follow alias chain forward to find the latest identity
	p.DisplayID = h.resolveDisplayID(projID, environment, p.DistinctID)

	// 5. Resolve ALL aliases for this person (recursive chain) to populate p.Aliases
	// The initial query only gets aliases linked to the *canonical* ID, which might miss some hops.
	allIDs := h.resolveAllAliasedIDs(projID, environment, canonicalID)

	// Filter out the main distinct_id and display_id from the aliases list
	seenAliases := make(map[string]bool)
	var finalAliases []string

	for _, id := range allIDs {
		// Don't include the main ID or display ID in the aliases list
		if id != p.DistinctID && id != p.DisplayID {
			if !seenAliases[id] {
				seenAliases[id] = true
				finalAliases = append(finalAliases, id)
			}
		}
	}
	p.Aliases = finalAliases

	return c.JSON(fiber.Map{"data": p})
}

// resolveAllAliasedIDs collects all distinct_ids that belong to the same person
// by following the alias chain in both directions.
// Mirrors the logic in EventsHandler.
func (h *PeopleHandler) resolveAllAliasedIDs(projID, environment, startID string) []string {
	allIDs := map[string]bool{startID: true}
	queue := []string{startID}

	for len(queue) > 0 {
		currentID := queue[0]
		queue = queue[1:]

		// Forward: currentID is alias_id -> find distinct_id
		rows, err := h.CH.Query(context.Background(),
			"SELECT distinct_id FROM person_aliases WHERE project_id = ? AND environment = ? AND alias_id = ?",
			projID, environment, currentID,
		)
		if err == nil {
			for rows.Next() {
				var did string
				if rows.Scan(&did) == nil && !allIDs[did] {
					allIDs[did] = true
					queue = append(queue, did)
				}
			}
			rows.Close()
		}

		// Backward: currentID is distinct_id -> find alias_id
		rows2, err := h.CH.Query(context.Background(),
			"SELECT alias_id FROM person_aliases WHERE project_id = ? AND environment = ? AND distinct_id = ?",
			projID, environment, currentID,
		)
		if err == nil {
			for rows2.Next() {
				var aid string
				if rows2.Scan(&aid) == nil && !allIDs[aid] {
					allIDs[aid] = true
					queue = append(queue, aid)
				}
			}
			rows2.Close()
		}
	}

	result := make([]string, 0, len(allIDs))
	for id := range allIDs {
		result = append(result, id)
	}
	return result
}

// GetPropertyKeys - GET /api/v1/people/properties/keys
// Returns all distinct property keys from the persons table
func (h *PeopleHandler) GetPropertyKeys(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	var project database.Project
	queryProjectID := c.Query("project_id", "")

	if queryProjectID != "" {
		if err := h.DB.First(&project, "id = ?", queryProjectID).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
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
	}

	environment := c.Query("environment", "live")
	projID := project.ID

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

	keys = applyLexiconToProfileProperties(h.DB, project.ID, keys)

	return c.JSON(fiber.Map{"keys": keys})
}

// applyLexiconToProfileProperties takes a list of raw profile property keys from ClickHouse,
// maps them against the LexiconProfileProperty table in SQLite, hides any properties that are merged
// into a virtual property, and adds the virtual properties themselves if they aren't already included.
func applyLexiconToProfileProperties(db *gorm.DB, projectID string, rawKeys []string) []string {
	if len(rawKeys) == 0 {
		return rawKeys
	}

	rawSet := make(map[string]bool)
	for _, k := range rawKeys {
		rawSet[k] = true
	}

	var allLexiconProps []database.LexiconProfileProperty
	if err := db.Where("project_id = ?", projectID).Find(&allLexiconProps).Error; err != nil {
		return rawKeys
	}

	hiddenMap := make(map[string]bool)
	virtualMap := make(map[string]bool)
	mergedTargetsMap := make(map[string]bool)

	for _, p := range allLexiconProps {
		if p.Hidden {
			hiddenMap[p.Name] = true
		}
		if p.IsVirtual {
			virtualMap[p.Name] = true
		}
		if p.MergedIntoID != nil && *p.MergedIntoID != "" {
			if rawSet[p.Name] {
				mergedTargetsMap[*p.MergedIntoID] = true
			}
		}
	}

	if len(mergedTargetsMap) > 0 {
		for _, p := range allLexiconProps {
			if mergedTargetsMap[p.ID] {
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

// GetPropertyValues - GET /api/v1/people/properties/values
// Returns distinct values for a given property key from the persons table
// Params: property (required), search (optional), limit (optional)
func (h *PeopleHandler) GetPropertyValues(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	var project database.Project
	queryProjectID := c.Query("project_id", "")

	if queryProjectID != "" {
		if err := h.DB.First(&project, "id = ?", queryProjectID).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
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
	}

	environment := c.Query("environment", "live")
	property := c.Query("property", "")
	search := c.Query("search", "")
	limit := c.QueryInt("limit", 100)

	if property == "" {
		return c.JSON(fiber.Map{"values": []string{}})
	}

	projID := project.ID

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
