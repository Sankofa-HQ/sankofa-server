package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
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
	Sparkline  []uint64          `json:"sparkline"`
}

// EventWhereClause is a sub-filter on event properties (e.g. "where App Version is 1.0.0")
type EventWhereClause struct {
	ID               string `json:"id"`
	Property         string `json:"property"`      // e.g. "prop_app_version"
	PropertyLabel    string `json:"propertyLabel"` // Display label
	PropertyType     string `json:"propertyType"`  // "string", "number", etc.
	TrackedAs        string `json:"trackedAs"`     // Raw backend key e.g. "app_version"
	Operator         string `json:"operator"`      // "is", "is_not", "contains", etc.
	Value            string `json:"value"`
	PropertyCategory string `json:"propertyCategory"` // "event", "default", "user", "system"
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
	BehaviorType string             `json:"behaviorType"` // "did" or "did_not"
	EventName    string             `json:"eventName"`
	Metric       string             `json:"metric"`       // "total_events", "aggregate_sum", etc.
	TimeRange    string             `json:"timeRange"`    // "30d", "24h", etc.
	WhereFilters []EventWhereClause `json:"whereFilters"` // Sub-filters on event properties
}

// buildWhereClauseSQL converts where clauses into SQL conditions for the events table.
// Event/default properties use mapUpdate(default_properties, properties) on the events table.
// User properties use a subquery against the persons table.
// It also expands virtual/merged properties into their child properties.
func buildWhereClauseSQL(db *gorm.DB, clauses []EventWhereClause, projectID, environment string) (string, []interface{}) {
	var conditions []string
	var args []interface{}

	for _, wc := range clauses {
		if wc.Property == "" {
			continue
		}

		// Determine the raw property key for ClickHouse
		rawKey := wc.TrackedAs
		if rawKey == "" {
			rawKey = wc.Property
		}
		// Check if this is a user property before stripping prefixes
		isUserProp := wc.PropertyCategory == "user" || strings.HasPrefix(rawKey, "user_")

		// Strip frontend prefixes to get the actual property key
		if strings.HasPrefix(rawKey, "prop_") {
			rawKey = rawKey[5:]
		} else if strings.HasPrefix(rawKey, "default_") {
			rawKey = rawKey[8:]
		} else if strings.HasPrefix(rawKey, "user_") {
			rawKey = rawKey[5:]
		}
		rawKey = sanitizeKey(rawKey)

		// Expand virtual/merged properties into child property names
		childKeys := []string{rawKey}
		if db != nil && !isUserProp {
			var vp database.LexiconEventProperty
			if err := db.Select("id").Where("project_id = ? AND is_virtual = ? AND name = ?", projectID, true, rawKey).First(&vp).Error; err == nil {
				// It's a virtual property — get its children
				var children []database.LexiconEventProperty
				db.Select("name").Where("project_id = ? AND merged_into_id = ?", projectID, vp.ID).Find(&children)
				if len(children) > 0 {
					childKeys = make([]string, len(children))
					for i, c := range children {
						childKeys[i] = c.Name
					}
				}
			}
		}

		if isUserProp {
			// User properties: check against the persons table via subquery
			personPropExpr := fmt.Sprintf("sub.props['%s']", rawKey)
			personSubBase := "SELECT distinct_id FROM (SELECT distinct_id, argMax(properties, last_seen) AS props FROM persons WHERE project_id = ? AND environment = ? GROUP BY distinct_id) sub WHERE "

			switch wc.Operator {
			case "is":
				conditions = append(conditions, fmt.Sprintf("distinct_id IN (%s%s = ?)", personSubBase, personPropExpr))
				args = append(args, projectID, environment, wc.Value)
			case "is_not":
				conditions = append(conditions, fmt.Sprintf("distinct_id IN (%s%s != ?)", personSubBase, personPropExpr))
				args = append(args, projectID, environment, wc.Value)
			case "contains":
				conditions = append(conditions, fmt.Sprintf("distinct_id IN (%spositionCaseInsensitive(%s, ?) > 0)", personSubBase, personPropExpr))
				args = append(args, projectID, environment, wc.Value)
			case "does_not_contain":
				conditions = append(conditions, fmt.Sprintf("distinct_id IN (%spositionCaseInsensitive(%s, ?) = 0)", personSubBase, personPropExpr))
				args = append(args, projectID, environment, wc.Value)
			case "is_set":
				conditions = append(conditions, fmt.Sprintf("distinct_id IN (%smapContains(sub.props, '%s'))", personSubBase, rawKey))
				args = append(args, projectID, environment)
			case "is_not_set":
				conditions = append(conditions, fmt.Sprintf("distinct_id IN (%sNOT mapContains(sub.props, '%s'))", personSubBase, rawKey))
				args = append(args, projectID, environment)
			case "gt":
				conditions = append(conditions, fmt.Sprintf("distinct_id IN (%stoFloat64OrZero(%s) > toFloat64OrZero(?))", personSubBase, personPropExpr))
				args = append(args, projectID, environment, wc.Value)
			case "lt":
				conditions = append(conditions, fmt.Sprintf("distinct_id IN (%stoFloat64OrZero(%s) < toFloat64OrZero(?))", personSubBase, personPropExpr))
				args = append(args, projectID, environment, wc.Value)
			case "gte":
				conditions = append(conditions, fmt.Sprintf("distinct_id IN (%stoFloat64OrZero(%s) >= toFloat64OrZero(?))", personSubBase, personPropExpr))
				args = append(args, projectID, environment, wc.Value)
			case "lte":
				conditions = append(conditions, fmt.Sprintf("distinct_id IN (%stoFloat64OrZero(%s) <= toFloat64OrZero(?))", personSubBase, personPropExpr))
				args = append(args, projectID, environment, wc.Value)
			}
		} else {
			// Event/default properties: check on the events table directly
			// For merged/virtual properties, childKeys has multiple entries (e.g. os, os_version)
			var propClauses []string
			var propArgs []interface{}

			for _, ck := range childKeys {
				propExpr := fmt.Sprintf("mapUpdate(default_properties, properties)['%s']", ck)

				switch wc.Operator {
				case "is":
					propClauses = append(propClauses, fmt.Sprintf("%s = ?", propExpr))
					propArgs = append(propArgs, wc.Value)
				case "is_not":
					propClauses = append(propClauses, fmt.Sprintf("%s != ?", propExpr))
					propArgs = append(propArgs, wc.Value)
				case "contains":
					propClauses = append(propClauses, fmt.Sprintf("positionCaseInsensitive(%s, ?) > 0", propExpr))
					propArgs = append(propArgs, wc.Value)
				case "does_not_contain":
					propClauses = append(propClauses, fmt.Sprintf("positionCaseInsensitive(%s, ?) = 0", propExpr))
					propArgs = append(propArgs, wc.Value)
				case "is_set":
					propClauses = append(propClauses, fmt.Sprintf("mapContains(mapUpdate(default_properties, properties), '%s')", ck))
				case "is_not_set":
					propClauses = append(propClauses, fmt.Sprintf("NOT mapContains(mapUpdate(default_properties, properties), '%s')", ck))
				case "gt":
					propClauses = append(propClauses, fmt.Sprintf("toFloat64OrZero(%s) > toFloat64OrZero(?)", propExpr))
					propArgs = append(propArgs, wc.Value)
				case "lt":
					propClauses = append(propClauses, fmt.Sprintf("toFloat64OrZero(%s) < toFloat64OrZero(?)", propExpr))
					propArgs = append(propArgs, wc.Value)
				case "gte":
					propClauses = append(propClauses, fmt.Sprintf("toFloat64OrZero(%s) >= toFloat64OrZero(?)", propExpr))
					propArgs = append(propArgs, wc.Value)
				case "lte":
					propClauses = append(propClauses, fmt.Sprintf("toFloat64OrZero(%s) <= toFloat64OrZero(?)", propExpr))
					propArgs = append(propArgs, wc.Value)
				}
			}

			if len(propClauses) > 0 {
				// For merged properties: positive ops use OR, negative ops use AND
				isNegative := wc.Operator == "is_not" || wc.Operator == "does_not_contain" || wc.Operator == "is_not_set"
				joinOp := " OR "
				if isNegative {
					joinOp = " AND "
				}
				conditions = append(conditions, "("+strings.Join(propClauses, joinOp)+")")
				args = append(args, propArgs...)
			}
		}
	}

	if len(conditions) == 0 {
		return "", nil
	}

	return strings.Join(conditions, " AND "), args
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
	// Use a 3-level subquery structure:
	// 1. Inner: Deduplicate profiles per distinct_id.
	// 2. Mid: Map each distinct_id to its canonical identity using unique aliases (mid_props, mid_seen) 
	//    to avoid "nested aggregate function" errors in ClickHouse.
	// 3. Outer: Group by canonical_id to merge all aliased identities into a single person record.
	baseQuery := `
		SELECT 
			canonical_id as distinct_id,
			argMax(mid_props, mid_seen) as properties,
			max(mid_seen) as last_seen,
			groupUniqArray(alias_id) as aliases
		FROM (
			SELECT 
				p.distinct_id,
				p.props AS mid_props,
				p.latest_seen AS mid_seen,
				if(pa.distinct_id != '', pa.distinct_id, p.distinct_id) as canonical_id,
				pa.alias_id
			FROM (
				SELECT 
					distinct_id,
					argMax(properties, last_seen) AS props,
					max(last_seen) AS latest_seen
				FROM persons
				WHERE project_id = ? AND environment = ?
				GROUP BY distinct_id
			) p
			LEFT JOIN (
				SELECT alias_id, distinct_id 
				FROM person_aliases 
				WHERE project_id = ? AND environment = ?
			) pa ON p.distinct_id = pa.alias_id
		)
		WHERE 1=1
	`
	args := []interface{}{projID, environment, projID, environment}

	if search != "" {
		// Search both distinct_id and aliases (resolve alias -> distinct_id)
		baseQuery += ` AND (
			positionCaseInsensitive(distinct_id, ?) > 0 
			OR distinct_id IN (
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
						baseQuery += fmt.Sprintf(" AND properties['%s'] = ?", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "is_not":
						baseQuery += fmt.Sprintf(" AND properties['%s'] != ?", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "contains":
						baseQuery += fmt.Sprintf(" AND positionCaseInsensitive(properties['%s'], ?) > 0", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "does_not_contain":
						baseQuery += fmt.Sprintf(" AND positionCaseInsensitive(properties['%s'], ?) = 0", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "is_set":
						baseQuery += fmt.Sprintf(" AND mapContains(properties, '%s')", sanitizeKey(f.Property))
					case "is_not_set":
						baseQuery += fmt.Sprintf(" AND NOT mapContains(properties, '%s')", sanitizeKey(f.Property))
					case "gt":
						baseQuery += fmt.Sprintf(" AND toFloat64OrZero(properties['%s']) > toFloat64OrZero(?)", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "lt":
						baseQuery += fmt.Sprintf(" AND toFloat64OrZero(properties['%s']) < toFloat64OrZero(?)", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "gte":
						baseQuery += fmt.Sprintf(" AND toFloat64OrZero(properties['%s']) >= toFloat64OrZero(?)", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "lte":
						baseQuery += fmt.Sprintf(" AND toFloat64OrZero(properties['%s']) <= toFloat64OrZero(?)", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "eq":
						baseQuery += fmt.Sprintf(" AND properties['%s'] = ?", sanitizeKey(f.Property))
						args = append(args, valStr)
					case "neq":
						baseQuery += fmt.Sprintf(" AND properties['%s'] != ?", sanitizeKey(f.Property))
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
							baseQuery += fmt.Sprintf(" AND parseDateTimeBestEffortOrNull(properties['%s']) >= now() - INTERVAL ? SECOND", sanitizeKey(f.Property))
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

					expandedNames := ExpandVirtualEventNames(h.DB, projID, environment, []string{f.EventName})
					placeholders := make([]string, len(expandedNames))
					subArgs := []interface{}{projID, environment}
					for i, name := range expandedNames {
						placeholders[i] = "?"
						subArgs = append(subArgs, name)
					}

					subQuery := fmt.Sprintf(`SELECT distinct_id FROM events WHERE project_id = ? AND environment = ? AND event_name IN (%s)`, strings.Join(placeholders, ","))

					if timeSql != "" {
						subQuery += " AND " + timeSql
						subArgs = append(subArgs, timeArgs...)
					}

					// 2b. Apply where clause filters on event properties
					if len(f.WhereFilters) > 0 {
						wcSQL, wcArgs := buildWhereClauseSQL(h.DB, f.WhereFilters, projID, environment)
						if wcSQL != "" {
							subQuery += " AND " + wcSQL
							subArgs = append(subArgs, wcArgs...)
						}
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
						baseQuery += fmt.Sprintf(" AND distinct_id IN (%s)", subQuery)
						args = append(args, subArgs...)
					} else { // did_not
						baseQuery += fmt.Sprintf(" AND distinct_id NOT IN (%s)", subQuery)
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
							baseQuery += fmt.Sprintf(" AND distinct_id NOT IN (%s)", cohortSQL)
						} else {
							baseQuery += fmt.Sprintf(" AND distinct_id IN (%s)", cohortSQL)
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
		GROUP BY distinct_id
		ORDER BY last_seen DESC
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
	} else if len(people) > 0 {
		// --- Fetch 7-Day Sparkline Data ---
		var allIDs []string
		// Pre-populate sparkline arrays with 7 zeros
		for i := range people {
			people[i].Sparkline = make([]uint64, 7)
			allIDs = append(allIDs, people[i].DistinctID)
			allIDs = append(allIDs, people[i].Aliases...)
		}

		sparkQuery := `
			SELECT 
				distinct_id,
				dateDiff('day', toDate(timestamp), today()) as days_ago,
				count() as c
			FROM events
			WHERE project_id = ? 
			  AND environment = ? 
			  AND distinct_id IN (?)
			  AND timestamp >= now() - INTERVAL 6 DAY
			GROUP BY distinct_id, days_ago
		`
		sparkRows, err := h.CH.Query(context.Background(), sparkQuery, projID, environment, allIDs)
		if err == nil {
			defer sparkRows.Close()

			// Map alias to parent index for fast lookup
			idToIndex := make(map[string]int)
			for i, p := range people {
				idToIndex[p.DistinctID] = i
				for _, a := range p.Aliases {
					idToIndex[a] = i
				}
			}

			for sparkRows.Next() {
				var dID string
				var daysAgo int32
				var count uint64
				if err := sparkRows.Scan(&dID, &daysAgo, &count); err == nil {
					// Map days_ago (0=today, 6=6 days ago) to array index (0=oldest, 6=today)
					idx := 6 - int(daysAgo)
					if idx >= 0 && idx < 7 {
						if pIndex, exists := idToIndex[dID]; exists {
							people[pIndex].Sparkline[idx] += count
						}
					}
				}
			}
		} else {
			log.Println("Details: ClickHouse Sparkline Error:", err)
		}
	}

	return c.JSON(fiber.Map{
		"data":   people,
		"limit":  limit,
		"offset": offset,
	})
}

// GetPerson - GET /api/v1/people/:id
func (h *PeopleHandler) GetPerson(c *fiber.Ctx) error {
	distinctID := strings.TrimSpace(c.Params("id"))
	if decoded, err := url.PathUnescape(distinctID); err == nil {
		distinctID = decoded
	}

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

	// 2. Resolve ALL IDs in the alias chain
	allIDs := h.resolveAllAliasedIDs(projID, environment, distinctID)

	// 3. Query ClickHouse for all IDs in the chain
	var p PersonProfile
	placeholders := make([]string, len(allIDs))
	queryArgs := []interface{}{projID, environment}
	for i, id := range allIDs {
		placeholders[i] = "?"
		queryArgs = append(queryArgs, id)
	}

	query := fmt.Sprintf(`
		SELECT 
			distinct_id,
			properties,
			last_seen
		FROM persons
		WHERE project_id = ? AND environment = ? AND distinct_id IN (%s)
		ORDER BY last_seen DESC
	`, strings.Join(placeholders, ","))

	rows, err := h.CH.Query(context.Background(), query, queryArgs...)
	if err != nil {
		log.Printf("GetPerson Error: %v", err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var did string
			var props map[string]string
			var ls time.Time
			if err := rows.Scan(&did, &props, &ls); err == nil {
				if p.DistinctID == "" {
					p.DistinctID = did
					p.Properties = props
					p.LastSeen = ls
				} else {
					// Merge properties, newer values win?
					// For now, just keep the first (latest) one's properties as primary,
					// but we could merge them here.
					for k, v := range props {
						if _, ok := p.Properties[k]; !ok {
							p.Properties[k] = v
						}
					}
					if ls.After(p.LastSeen) {
						p.LastSeen = ls
					}
				}
			}
		}
	}

// Fallback if not in persons table: check events table for activity
	if p.DistinctID == "" {
		placeholdersEvents := make([]string, len(allIDs))
		queryArgsEvents := []interface{}{projID, environment}
		for i, id := range allIDs {
			placeholdersEvents[i] = "?"
			queryArgsEvents = append(queryArgsEvents, id)
		}

		var foundDistinctID string
		var lastSeen time.Time

		err2 := h.CH.QueryRow(context.Background(),
			fmt.Sprintf("SELECT distinct_id, max(timestamp) FROM events WHERE project_id = ? AND environment = ? AND distinct_id IN (%s) GROUP BY distinct_id ORDER BY max(timestamp) DESC LIMIT 1", strings.Join(placeholdersEvents, ",")),
			queryArgsEvents...,
		).Scan(&foundDistinctID, &lastSeen)

		if err2 == nil && foundDistinctID != "" {
			p.DistinctID = foundDistinctID
			p.LastSeen = lastSeen
			p.Properties = make(map[string]string)
		} else {
			return c.Status(404).JSON(fiber.Map{
				"error":    "Person not found",
				"id":       distinctID,
				"resolved": allIDs,
				"proj":     projID,
				"env":      environment,
			})
		}
	}

	// 4. Resolve display_id: find the latest identity
	p.DisplayID = h.resolveDisplayID(projID, environment, p.DistinctID)

	// 5. Populate Aliases list from the allIDs we already found
	seenAliases := make(map[string]bool)
	var finalAliases []string

	for _, id := range allIDs {
		if id != p.DistinctID && id != p.DisplayID {
			if !seenAliases[id] {
				seenAliases[id] = true
				finalAliases = append(finalAliases, id)
			}
		}
	}
	p.Aliases = finalAliases
	if p.Aliases == nil {
		p.Aliases = []string{}
	}

	return c.JSON(fiber.Map{"data": p})
}

// resolveAllAliasedIDs collects all distinct_ids that belong to the same person
// by following the alias chain in both directions.
// Mirrors the logic in EventsHandler.
func (h *PeopleHandler) resolveAllAliasedIDs(projID, environment, startID string) []string {
	startID = strings.TrimSpace(startID)
	if startID == "" {
		return []string{}
	}

	allIDs := map[string]bool{startID: true}
	queue := []string{startID}

	// BFS to find all connected IDs
	for len(queue) > 0 {
		currentID := queue[0]
		queue = queue[1:]

		// Forward: alias_id -> distinct_id
		rows, err := h.CH.Query(context.Background(),
			"SELECT DISTINCT distinct_id FROM person_aliases WHERE project_id = ? AND environment = ? AND alias_id = ?",
			projID, environment, currentID,
		)
		if err == nil {
			for rows.Next() {
				var did string
				if err := rows.Scan(&did); err == nil && did != "" {
					if !allIDs[did] {
						allIDs[did] = true
						queue = append(queue, did)
					}
				}
			}
			rows.Close()
		} else {
			log.Printf("DEBUG: resolveAllAliasedIDs Forward Error: %v", err)
		}

		// Backward: distinct_id -> alias_id
		rows2, err := h.CH.Query(context.Background(),
			"SELECT DISTINCT alias_id FROM person_aliases WHERE project_id = ? AND environment = ? AND distinct_id = ?",
			projID, environment, currentID,
		)
		if err == nil {
			for rows2.Next() {
				var aid string
				if err := rows2.Scan(&aid); err == nil && aid != "" {
					if !allIDs[aid] {
						allIDs[aid] = true
						queue = append(queue, aid)
					}
				}
			}
			rows2.Close()
		} else {
			log.Printf("DEBUG: resolveAllAliasedIDs Backward Error: %v", err)
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

	keys = applyLexiconToProfileProperties(h.DB, project.ID, environment, keys)

	return c.JSON(fiber.Map{"keys": keys})
}

// applyLexiconToProfileProperties takes a list of raw profile property keys from ClickHouse,
// maps them against the LexiconProfileProperty table in SQLite, hides any properties that are merged
// into a virtual property, and adds the virtual properties themselves if they aren't already included.
func applyLexiconToProfileProperties(db *gorm.DB, projectID string, environment string, rawKeys []string) []string {
	if len(rawKeys) == 0 {
		return rawKeys
	}

	rawSet := make(map[string]bool)
	for _, k := range rawKeys {
		rawSet[k] = true
	}

	var allLexiconProps []database.LexiconProfileProperty
	if err := db.Where("project_id = ? AND environment = ?", projectID, environment).Find(&allLexiconProps).Error; err != nil {
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

// GetPersonHeatmap - GET /api/v1/people/:id/heatmap
// Returns daily event counts over the last 365 days for the user
func (h *PeopleHandler) GetPersonHeatmap(c *fiber.Ctx) error {
	distinctID := strings.TrimSpace(c.Params("id"))
	if decoded, err := url.PathUnescape(distinctID); err == nil {
		distinctID = decoded
	}

	if distinctID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing distinct_id"})
	}

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
	hideSystem := c.Query("hide_system", "false") == "true"
	projID := project.ID

	// Resolve alias chain to ensure we get all events for identities belonging to this person
	canonicalID := distinctID
	allIDs := h.resolveAllAliasedIDs(projID, environment, canonicalID)
	if len(allIDs) == 0 {
		allIDs = []string{canonicalID}
	}

	placeholders := make([]string, len(allIDs))
	queryArgs := []interface{}{projID, environment}
	for i, id := range allIDs {
		placeholders[i] = "?"
		queryArgs = append(queryArgs, id)
	}

	query := fmt.Sprintf(`
		SELECT 
			toString(toDate(timestamp)) as day,
			count() as value
		FROM (
			SELECT distinct_id, event_name, timestamp
			FROM events 
			WHERE project_id = ? AND environment = ? AND distinct_id IN (%s) %s
			LIMIT 1 BY distinct_id, event_name, timestamp
		)
		GROUP BY day
		ORDER BY day ASC
	`, strings.Join(placeholders, ","), func() string {
		if hideSystem {
			return "AND event_name NOT LIKE '$%'"
		}
		return ""
	}())

	rows, err := h.CH.Query(context.Background(), query, queryArgs...)
	if err != nil {
		log.Println("ClickHouse GetPersonHeatmap Error:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to generate heatmap"})
	}
	defer rows.Close()

	type HeatmapNode struct {
		Day   string `json:"day"`
		Value uint64 `json:"value"`
	}

	var results []HeatmapNode
	for rows.Next() {
		var node HeatmapNode
		if err := rows.Scan(&node.Day, &node.Value); err == nil {
			results = append(results, node)
		}
	}

	if results == nil {
		results = []HeatmapNode{}
	}

	return c.JSON(results)
}
