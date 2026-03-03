package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"sankofa/engine/internal/database"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type EventsHandler struct {
	DB               *gorm.DB
	CH               driver.Conn
	defaultPropsMap  sync.Map
	defaultPropsTime sync.Map
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

	// Promoted fields for reading from DB
	SessionID   string `json:"-"`
	City        string `json:"-"`
	Region      string `json:"-"`
	Country     string `json:"-"`
	OS          string `json:"-"`
	DeviceModel string `json:"-"`
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

// resolveAllAliasedIDs collects all distinct_ids that belong to the same person
// by following the alias chain in both directions.
// Given any ID in the chain, it returns [anonymous_uuid, user_532, user_304, user_80]
func (h *EventsHandler) resolveAllAliasedIDs(projID, environment, startID string) []string {
	allIDs := map[string]bool{startID: true}
	queue := []string{startID}

	for len(queue) > 0 {
		currentID := queue[0]
		queue = queue[1:]

		// Forward: currentID is alias_id → find distinct_id
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

		// Backward: currentID is distinct_id → find alias_id
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

// checkProjectAccess verifies if a user has RBAC clearance for a specific project.
// Returns true if access is allowed, false otherwise.
func checkProjectAccess(db *gorm.DB, project *database.Project, userID string) bool {
	var membership database.OrganizationMember
	if err := db.Where("organization_id = ? AND user_id = ?", project.OrganizationID, userID).First(&membership).Error; err != nil {
		return false
	}

	if membership.Role == "Owner" || membership.Role == "Admin" {
		return true // Org owners/admins have implicit access to all projects
	}

	// Strictly verify standard member access via direct linking or team linking
	var count int64
	db.Raw(`
		SELECT COUNT(DISTINCT p.id) 
		FROM projects p 
		LEFT JOIN project_members pm ON p.id = pm.project_id AND pm.user_id = ?
		LEFT JOIN team_projects tp ON p.id = tp.project_id
		LEFT JOIN team_members tm ON tp.team_id = tm.team_id AND tm.user_id = ?
		WHERE p.id = ? AND (pm.user_id IS NOT NULL OR tm.user_id IS NOT NULL)
	`, userID, userID, project.ID).Scan(&count)

	return count > 0
}

// TrackEvent handles the "Fast Path". Checks RAM, queues if new.
func (h *EventsHandler) ListEvents(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	// Resolve project: prefer query param, fallback to user's current project
	var project database.Project
	queryProjectID := c.Query("project_id", "")

	if queryProjectID != "" {
		// Use project ID from frontend
		if err := h.DB.First(&project, "id = ?", queryProjectID).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
		}
	} else {
		// Fallback to user's current project in DB
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

	// RBAC Check: Does the user have access to this project?
	if !checkProjectAccess(h.DB, &project, userID) {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied to project events"})
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
	hideSystem := c.Query("hide_system", "false") == "true"

	orgID := project.OrganizationID
	projID := project.ID

	// 1. Build Base WHERE Clause
	whereClause := "WHERE tenant_id = ? AND environment = ?"
	queryArgs := []interface{}{projID, environment}

	if hideSystem {
		whereClause += " AND event_name NOT LIKE '$%'"
	}

	if distinctID != "" {
		// Resolve all related IDs through the alias chain to find ALL events for this user
		allIDs := h.resolveAllAliasedIDs(projID, environment, distinctID)
		if len(allIDs) == 1 {
			whereClause += " AND distinct_id = ?"
			queryArgs = append(queryArgs, allIDs[0])
		} else {
			placeholders := make([]string, len(allIDs))
			for i := range allIDs {
				placeholders[i] = "?"
				queryArgs = append(queryArgs, allIDs[i])
			}
			whereClause += " AND distinct_id IN (" + strings.Join(placeholders, ",") + ")"
		}
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
				expanded := ExpandVirtualEventNames(h.DB, projID, []string{q.EventName})
				if len(expanded) == 1 {
					andClauses = append(andClauses, "event_name = ?")
					queryArgs = append(queryArgs, expanded[0])
				} else if len(expanded) > 1 {
					placeholders := make([]string, len(expanded))
					for i, ex := range expanded {
						placeholders[i] = "?"
						queryArgs = append(queryArgs, ex)
					}
					andClauses = append(andClauses, "event_name IN ("+strings.Join(placeholders, ",")+")")
				} else {
					// Fallback (shouldn't realistically happen if expanded is empty, but just in case)
					andClauses = append(andClauses, "event_name = ?")
					queryArgs = append(queryArgs, q.EventName)
				}
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
					keys := []string{f.Property}
					if f.Property != "event_name" && f.Property != "distinct_id" && f.Property != "lib_version" && f.Property != "timestamp" {
						keys = h.expandVirtualPropertyNames(projID, f.Property)
					}

					var propClauses []string
					var propArgs []interface{}

					for _, key := range keys {
						var col string
						isUserProp := strings.HasPrefix(key, "user_")

						// Determine column and path
						if strings.HasPrefix(key, "prop_") {
							col = fmt.Sprintf("properties['%s']", key[5:])
						} else if key == "event_name" || key == "distinct_id" || key == "lib_version" || key == "timestamp" {
							col = key
						} else if isUserProp {
							col = fmt.Sprintf("properties['%s']", key[5:])
						} else if strings.HasPrefix(key, "default_") {
							col = fmt.Sprintf("default_properties['%s']", key[8:])
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
							propClauses = append(propClauses, fmt.Sprintf("%s > ?", targetCol))
							propArgs = append(propArgs, f.Value)
						case "<":
							propClauses = append(propClauses, fmt.Sprintf("%s < ?", targetCol))
							propArgs = append(propArgs, f.Value)
						case ">=":
							propClauses = append(propClauses, fmt.Sprintf("%s >= ?", targetCol))
							propArgs = append(propArgs, f.Value)
						case "<=":
							propClauses = append(propClauses, fmt.Sprintf("%s <= ?", targetCol))
							propArgs = append(propArgs, f.Value)
						case "is", "=":
							if col == "event_name" {
								expanded := ExpandVirtualEventNames(h.DB, projID, []string{f.Value})
								if len(expanded) == 1 {
									propClauses = append(propClauses, fmt.Sprintf("%s = ?", targetCol))
									propArgs = append(propArgs, expanded[0])
								} else {
									placeholders := make([]string, len(expanded))
									for i, ex := range expanded {
										placeholders[i] = "?"
										propArgs = append(propArgs, ex)
									}
									propClauses = append(propClauses, fmt.Sprintf("%s IN (%s)", targetCol, strings.Join(placeholders, ",")))
								}
							} else {
								propClauses = append(propClauses, fmt.Sprintf("%s = ?", targetCol))
								propArgs = append(propArgs, f.Value)
							}
						case "is_not", "!=":
							if col == "event_name" {
								expanded := ExpandVirtualEventNames(h.DB, projID, []string{f.Value})
								if len(expanded) == 1 {
									propClauses = append(propClauses, fmt.Sprintf("%s != ?", targetCol))
									propArgs = append(propArgs, expanded[0])
								} else {
									placeholders := make([]string, len(expanded))
									for i, ex := range expanded {
										placeholders[i] = "?"
										propArgs = append(propArgs, ex)
									}
									propClauses = append(propClauses, fmt.Sprintf("%s NOT IN (%s)", targetCol, strings.Join(placeholders, ",")))
								}
							} else {
								propClauses = append(propClauses, fmt.Sprintf("%s != ?", targetCol))
								propArgs = append(propArgs, f.Value)
							}
						case "contains":
							propClauses = append(propClauses, fmt.Sprintf("%s ILIKE ?", targetCol))
							propArgs = append(propArgs, "%"+f.Value+"%")
						case "does_not_contain":
							propClauses = append(propClauses, fmt.Sprintf("%s NOT ILIKE ?", targetCol))
							propArgs = append(propArgs, "%"+f.Value+"%")
						case "is_set":
							if strings.HasPrefix(key, "prop_") || (!strings.HasPrefix(key, "event_") && !strings.HasPrefix(key, "distinct_") && !strings.HasPrefix(key, "lib_") && !strings.HasPrefix(key, "time")) {
								if f.Type == "object" && f.Subkey != "" {
									propClauses = append(propClauses, fmt.Sprintf("JSONHas(%s, '%s') = 1", strings.TrimSuffix(strings.TrimPrefix(col, "JSONExtractString("), ", '"+f.Subkey+"')"), f.Subkey))
								} else {
									if strings.HasPrefix(key, "prop_") {
										propClauses = append(propClauses, fmt.Sprintf("mapContains(properties, '%s')", key[5:]))
									} else if isUserProp {
										propClauses = append(propClauses, fmt.Sprintf("mapContains(properties, '%s')", key[5:]))
									} else if strings.HasPrefix(key, "default_") {
										propClauses = append(propClauses, fmt.Sprintf("mapContains(default_properties, '%s')", key[8:]))
									} else {
										propClauses = append(propClauses, fmt.Sprintf("mapContains(default_properties, '%s')", key))
									}
								}
							} else {
								propClauses = append(propClauses, fmt.Sprintf("%s IS NOT NULL", col))
							}
						case "is_not_set":
							if strings.HasPrefix(key, "prop_") {
								propClauses = append(propClauses, fmt.Sprintf("NOT mapContains(properties, '%s')", key[5:]))
							} else if isUserProp {
								propClauses = append(propClauses, fmt.Sprintf("NOT mapContains(properties, '%s')", key[5:]))
							} else if strings.HasPrefix(key, "default_") {
								propClauses = append(propClauses, fmt.Sprintf("NOT mapContains(default_properties, '%s')", key[8:]))
							} else {
								propClauses = append(propClauses, fmt.Sprintf("NOT mapContains(default_properties, '%s')", key))
							}
						case "any_in_list":
							var vals []string
							if err := json.Unmarshal([]byte(f.Value), &vals); err == nil && len(vals) > 0 {
								if col == "event_name" {
									vals = ExpandVirtualEventNames(h.DB, projID, vals)
								}
								placeholders := strings.Repeat("?,", len(vals)-1) + "?"
								propClauses = append(propClauses, fmt.Sprintf("%s IN (%s)", targetCol, placeholders))
								for _, v := range vals {
									propArgs = append(propArgs, v)
								}
							}
						}

						// Special Cohort Handling (Type == "cohort")
						if f.Type == "cohort" {
							cohortIDStr := f.Value
							var cohort database.Cohort
							if err := h.DB.First(&cohort, "id = ?", cohortIDStr).Error; err == nil {
								var cohortSQL string
								var cohortArgs []interface{}

								if cohort.Type == "static" {
									cohortSQL = "SELECT distinct_id FROM cohort_static_members WHERE project_id = ? AND cohort_id = ? GROUP BY distinct_id HAVING sum(sign) > 0"
									cohortID, _ := strconv.Atoi(cohortIDStr)
									cohortArgs = []interface{}{projID, cohortID}
								} else {
									var ast CohortAST
									if err := json.Unmarshal(cohort.Rules, &ast); err == nil {
										cohortSQL, cohortArgs = BuildCohortSQL(h.DB, projID, environment, ast)
									}
								}

								if cohortSQL != "" {
									if f.Operator == "is_not" || f.Operator == "!=" {
										propClauses = append(propClauses, fmt.Sprintf("distinct_id NOT IN (%s)", cohortSQL))
									} else {
										propClauses = append(propClauses, fmt.Sprintf("distinct_id IN (%s)", cohortSQL))
									}
									propArgs = append(propArgs, cohortArgs...)
								}
							}
						}
					}

					if len(propClauses) > 0 {
						isNegative := f.Operator == "is_not" || f.Operator == "!=" || f.Operator == "does_not_contain" || f.Operator == "is_not_set"
						joinOp := " OR "
						if isNegative {
							joinOp = " AND "
						}

						subCond := strings.Join(propClauses, joinOp)

						// Wrap User property checks in a subquery
						if strings.HasPrefix(f.Property, "user_") {
							subCond = fmt.Sprintf("distinct_id IN (SELECT distinct_id FROM persons WHERE project_id = ? AND environment = ? AND (%s))", subCond)
							// Prepend projID and environment to propArgs
							newArgs := []interface{}{projID, environment}
							newArgs = append(newArgs, propArgs...)
							propArgs = newArgs
						} else {
							subCond = "(" + subCond + ")"
						}

						andClauses = append(andClauses, subCond)
						queryArgs = append(queryArgs, propArgs...)
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
			session_id,
			city,
			region,
			country,
			os,
			device_model,
			properties,
			default_properties,
			lib_version,
			tenant_id,
			project_id,
			organization_id,
			environment
		FROM events
	` + whereClause + fmt.Sprintf(" ORDER BY timestamp DESC LIMIT 1 BY distinct_id, event_name, timestamp LIMIT %d OFFSET %d", limit, offset)

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
			&e.SessionID,
			&e.City,
			&e.Region,
			&e.Country,
			&e.OS,
			&e.DeviceModel,
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

		// Remerge promoted fields into Maps for JSON output identical to old schema
		if e.Properties == nil {
			e.Properties = make(map[string]string)
		}
		if e.DefaultProperties == nil {
			e.DefaultProperties = make(map[string]string)
		}

		if e.SessionID != "" {
			e.Properties["$session_id"] = e.SessionID
		}
		if e.City != "" {
			e.DefaultProperties["$city"] = e.City
		}
		if e.Region != "" {
			e.DefaultProperties["$region"] = e.Region
		}
		if e.Country != "" {
			e.DefaultProperties["$country"] = e.Country
		}
		if e.OS != "" {
			e.DefaultProperties["$os"] = e.OS
		}
		if e.DeviceModel != "" {
			e.DefaultProperties["$device_model"] = e.DeviceModel
		}

		events = append(events, e)
	}

	// Execute Count Query
	var totalCount uint64
	countQuery := "SELECT count() FROM (SELECT distinct_id, event_name, timestamp FROM events " + whereClause + " LIMIT 1 BY distinct_id, event_name, timestamp)"

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
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	// Resolve project: prefer query param, fallback to user's current project
	var project database.Project
	queryProjectID := c.Query("project_id", "")
	hideSystem := c.Query("hide_system", "true") == "true"

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

	// RBAC Check
	if !checkProjectAccess(h.DB, &project, userID) {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied to project events"})
	}

	// Fetch from Lexicon (SQLite) - The "Gatekeeper" source of truth
	var totalLexiconEvents int64

	lexiconQuery := h.DB.Model(&database.LexiconEvent{}).Where("project_id = ?", project.ID)
	if hideSystem {
		lexiconQuery = lexiconQuery.Where("name NOT LIKE '$%'")
	}
	lexiconQuery.Count(&totalLexiconEvents)

	var eventNames []string

	if totalLexiconEvents > 0 {
		var lexiconEvents []database.LexiconEvent

		query := h.DB.Model(&database.LexiconEvent{}).
			Where("project_id = ? AND status = ? AND hidden = ?", project.ID, "approved", false)

		if hideSystem {
			query = query.Where("name NOT LIKE '$%'")
		}

		if err := query.Order("name ASC").Find(&lexiconEvents).Error; err != nil {
			log.Println("Lexicon Query Error:", err)
		}
		for _, e := range lexiconEvents {
			eventNames = append(eventNames, e.Name)
		}
	} else {
		// Fallback: if Lexicon is empty, query ClickHouse directly
		projID := project.ID
		environment := c.Query("environment", "live")

		query := "SELECT DISTINCT event_name FROM events WHERE tenant_id = ? AND environment = ?"
		if hideSystem {
			query += " AND event_name NOT LIKE '$%'"
		}
		query += " ORDER BY event_name"

		rows, err := h.CH.Query(context.Background(), query, projID, environment)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var name string
				if rows.Scan(&name) == nil && name != "" {
					eventNames = append(eventNames, name)
				}
			}
		} else {
			log.Println("ClickHouse fallback event names error:", err)
		}
	}

	if eventNames == nil {
		eventNames = []string{}
	}

	return c.JSON(fiber.Map{
		"event_names": eventNames,
	})
}

// GetEventDetail - GET /api/v1/events/:id
func (h *EventsHandler) GetEventDetail(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	eventID := c.Params("id")
	if eventID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing event ID"})
	}

	// 1. Resolve Project
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

	if !checkProjectAccess(h.DB, &project, userID) {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied to project events"})
	}

	environment := c.Query("environment", "live")
	projID := project.ID

	// 2. Fetch Event from ClickHouse
	query := `
		SELECT 
			id, timestamp, event_name, distinct_id, session_id, city, region, country, os, device_model, properties, default_properties, lib_version
		FROM events
		WHERE project_id = ? AND environment = ? AND id = ?
		LIMIT 1
	`

	var e struct {
		ID                string            `json:"id"`
		Timestamp         time.Time         `json:"timestamp"`
		EventName         string            `json:"event_name"`
		DistinctID        string            `json:"distinct_id"`
		SessionID         string            `json:"-"`
		City              string            `json:"-"`
		Region            string            `json:"-"`
		Country           string            `json:"-"`
		OS                string            `json:"-"`
		DeviceModel       string            `json:"-"`
		Properties        map[string]string `json:"properties"`
		DefaultProperties map[string]string `json:"default_properties"`
		LibVersion        string            `json:"lib_version"`
	}

	if err := h.CH.QueryRow(context.Background(), query, projID, environment, eventID).Scan(
		&e.ID, &e.Timestamp, &e.EventName, &e.DistinctID, &e.SessionID, &e.City, &e.Region, &e.Country, &e.OS, &e.DeviceModel, &e.Properties, &e.DefaultProperties, &e.LibVersion,
	); err != nil {
		log.Printf("GetEventDetail error: %v, event_id: %s", err, eventID)
		return c.Status(404).JSON(fiber.Map{"error": "Event not found"})
	}

	// Remerge promoted fields into Maps for JSON output
	if e.Properties == nil {
		e.Properties = make(map[string]string)
	}
	if e.DefaultProperties == nil {
		e.DefaultProperties = make(map[string]string)
	}

	if e.SessionID != "" {
		e.Properties["$session_id"] = e.SessionID
	}
	if e.City != "" {
		e.DefaultProperties["$city"] = e.City
	}
	if e.Region != "" {
		e.DefaultProperties["$region"] = e.Region
	}
	if e.Country != "" {
		e.DefaultProperties["$country"] = e.Country
	}
	if e.OS != "" {
		e.DefaultProperties["$os"] = e.OS
	}
	if e.DeviceModel != "" {
		e.DefaultProperties["$device_model"] = e.DeviceModel
	}

	return c.JSON(fiber.Map{"data": e})
}

// GetEventCounts - GET /api/v1/events/counts
// Returns event counts grouped by event_name for the last 30 days
func (h *EventsHandler) GetEventCounts(c *fiber.Ctx) error {
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

	if !checkProjectAccess(h.DB, &project, userID) {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied to project events"})
	}

	environment := c.Query("environment", "live")
	days := c.QueryInt("days", 30)
	projID := project.ID
	hideSystem := c.Query("hide_system", "true") == "true"

	query := `
		SELECT event_name, count() as cnt
		FROM events
		WHERE tenant_id = ?
			AND environment = ?
			AND timestamp >= now() - INTERVAL ? DAY
	`

	if hideSystem {
		query += " AND event_name NOT LIKE '$%'\n"
	}

	query += `
		GROUP BY event_name
		ORDER BY cnt DESC
	`

	rows, err := h.CH.Query(context.Background(), query, projID, environment, days)
	if err != nil {
		log.Println("ClickHouse counts query error:", err)
		return c.JSON(fiber.Map{"counts": map[string]uint64{}})
	}
	defer rows.Close()

	// Fetch mapping from child event to virtual parent event
	virtualMap := h.getVirtualEventMapping(projID)
	counts := make(map[string]uint64)

	// Fetch Lexicon visibility rules
	var lexEvents []database.LexiconEvent
	h.DB.Select("name, status, hidden").Where("project_id = ?", projID).Find(&lexEvents)

	lexiconActive := len(lexEvents) > 0
	allowMap := make(map[string]bool)
	for _, le := range lexEvents {
		if le.Status == "approved" && !le.Hidden {
			allowMap[le.Name] = true
		}
	}

	for rows.Next() {
		var name string
		var cnt uint64
		if err := rows.Scan(&name, &cnt); err != nil {
			continue
		}

		// Enforce Lexicon gatekeeping if Lexicon is active for this project
		if lexiconActive {
			if !allowMap[name] {
				continue
			}
		}

		// If this event is merged into a virtual event, aggregate under the virtual event's name
		if parentName, isMerged := virtualMap[name]; isMerged {
			counts[parentName] += cnt
		} else {
			counts[name] += cnt
		}
	}

	return c.JSON(fiber.Map{
		"counts": counts,
		"days":   days,
	})
}

// GetEventValues - GET /api/v1/events/values
func (h *EventsHandler) GetEventValues(c *fiber.Ctx) error {
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

	if !checkProjectAccess(h.DB, &project, userID) {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied to project events"})
	}

	environment := c.Query("environment", "live")
	property := c.Query("property", "")
	search := c.Query("search", "")
	eventName := c.Query("event_name", "")

	// Expand merged/virtual event names
	var expandedEventNames []string
	if eventName != "" {
		expandedEventNames = ExpandVirtualEventNames(h.DB, project.ID, []string{eventName})
		if len(expandedEventNames) == 0 {
			expandedEventNames = []string{eventName}
		}
	}

	if property == "" {
		return c.JSON(fiber.Map{"values": []string{}})
	}

	keys := []string{property}
	if property != "event_name" && property != "distinct_id" && property != "lib_version" && property != "timestamp" {
		keys = h.expandVirtualPropertyNames(project.ID, property)
	}

	var unionQueries []string
	var args []interface{}

	for _, key := range keys {
		// Determine column based on property name
		col := ""
		if strings.HasPrefix(key, "prop_") {
			col = fmt.Sprintf("properties['%s']", key[5:])
		} else if key == "event_name" || key == "distinct_id" || key == "lib_version" || key == "timestamp" {
			col = key
		} else if strings.HasPrefix(key, "default_") {
			rawKey := key[8:]
			col = fmt.Sprintf("if(mapContains(default_properties, '$%s'), default_properties['$%s'], default_properties['%s'])", rawKey, rawKey, rawKey)
		} else {
			col = fmt.Sprintf("if(mapContains(default_properties, '$%s'), default_properties['$%s'], default_properties['%s'])", key, key, key)
		}

		// Build Query
		subQuery := fmt.Sprintf(`
			SELECT DISTINCT %s as val
			FROM events
			WHERE tenant_id = ? AND environment = ? AND %s != '' AND %s IS NOT NULL
		`, col, col, col)
		args = append(args, project.ID, environment)

		if len(expandedEventNames) > 0 {
			ph := make([]string, len(expandedEventNames))
			for i := range expandedEventNames {
				ph[i] = "?"
			}
			subQuery += " AND event_name IN (" + strings.Join(ph, ",") + ")"
			for _, en := range expandedEventNames {
				args = append(args, en)
			}
		}

		if search != "" {
			subQuery += fmt.Sprintf(" AND %s ILIKE ?", col)
			args = append(args, "%"+search+"%")
		}

		unionQueries = append(unionQueries, subQuery)
	}

	query := "SELECT DISTINCT val FROM (" + strings.Join(unionQueries, " UNION ALL ") + ") ORDER BY val LIMIT 100"

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

// getVirtualEventMapping returns a map of child event names to their parent virtual event's name.
func (h *EventsHandler) getVirtualEventMapping(projectID string) map[string]string {
	mapping := make(map[string]string)

	var virtualEvents []database.LexiconEvent
	if err := h.DB.Where("project_id = ? AND is_virtual = ?", projectID, true).Find(&virtualEvents).Error; err != nil {
		return mapping
	}

	if len(virtualEvents) == 0 {
		return mapping
	}

	virtualIDToName := make(map[string]string)
	var virtualIDs []string
	for _, ve := range virtualEvents {
		virtualIDToName[ve.ID] = ve.Name
		virtualIDs = append(virtualIDs, ve.ID)
	}

	var children []database.LexiconEvent
	if err := h.DB.Where("project_id = ? AND merged_into_id IN ?", projectID, virtualIDs).Find(&children).Error; err != nil {
		return mapping
	}

	for _, child := range children {
		if child.MergedIntoID != nil {
			if parentName, ok := virtualIDToName[*child.MergedIntoID]; ok {
				mapping[child.Name] = parentName
			}
		}
	}

	return mapping
}

// getKnownDefaultProperties fetches unique keys from the default_properties map for a project.
// It caches the result for 15 minutes to avoid hitting ClickHouse constantly.
func (h *EventsHandler) getKnownDefaultProperties(projectID string) map[string]bool {
	if val, ok := h.defaultPropsTime.Load(projectID); ok {
		if time.Since(val.(time.Time)) < 15*time.Minute {
			if propsMap, ok := h.defaultPropsMap.Load(projectID); ok {
				return propsMap.(map[string]bool)
			}
		}
	}

	query := `
		SELECT DISTINCT arrayJoin(mapKeys(default_properties)) as key
		FROM (
			SELECT default_properties
			FROM events
			WHERE project_id = ?
			ORDER BY timestamp DESC
			LIMIT 10000
		)
	`
	rows, err := h.CH.Query(context.Background(), query, projectID)

	defaultProps := make(map[string]bool)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err == nil {
				defaultProps[key] = true
			}
		}
	}

	// Always fallback to some critical defaults just in case the query failed or table is completely empty
	criticalDefaults := []string{
		"browser", "browser_version", "os", "os_version",
		"device_type", "device_model", "device_manufacturer",
		"screen_resolution", "screen_width", "screen_height",
		"lib", "lib_version", "referrer", "referring_domain",
		"current_url", "host", "pathname", "search",
		"city", "region", "country", "continent",
		"timezone", "locale", "app_version", "app_build",
		"build_number", "ip", "user_agent",
	}
	for _, k := range criticalDefaults {
		defaultProps[k] = true
	}

	h.defaultPropsMap.Store(projectID, defaultProps)
	h.defaultPropsTime.Store(projectID, time.Now())

	return defaultProps
}

// expandVirtualPropertyNames checks if the given property name (which may be prefixed with "prop_")
// is a virtual property in Lexicon, and if so, returns all its underlying child property names.
func (h *EventsHandler) expandVirtualPropertyNames(projectID string, propName string) []string {
	if propName == "" {
		return []string{}
	}

	// Default properties (default_xxx) are never virtual/merged — return as-is
	if strings.HasPrefix(propName, "default_") {
		return []string{propName}
	}

	isEventProp := strings.HasPrefix(propName, "prop_")
	rawName := propName
	if isEventProp {
		rawName = propName[len("prop_"):]
	}

	var virtualID string
	var found bool
	var foundInEventProps bool // track which table we found it in

	if isEventProp {
		// Explicitly prefixed prop_ — search event properties only
		var vp database.LexiconEventProperty
		if err := h.DB.Select("id").Where("project_id = ? AND is_virtual = ? AND name = ?", projectID, true, rawName).First(&vp).Error; err == nil {
			virtualID = vp.ID
			found = true
			foundInEventProps = true
		}
	} else {
		// No prefix (e.g. merged_prop_..., or a raw property name).
		// Try LexiconEventProperty first — merged event properties like merged_prop_... live here.
		var vep database.LexiconEventProperty
		if err := h.DB.Select("id").Where("project_id = ? AND is_virtual = ? AND name = ?", projectID, true, rawName).First(&vep).Error; err == nil {
			virtualID = vep.ID
			found = true
			foundInEventProps = true
		}

		// Fall back to LexiconProfileProperty (user/profile merged properties)
		if !found {
			var vpp database.LexiconProfileProperty
			if err := h.DB.Select("id").Where("project_id = ? AND is_virtual = ? AND name = ?", projectID, true, rawName).First(&vpp).Error; err == nil {
				virtualID = vpp.ID
				found = true
				foundInEventProps = false
			}
		}
	}

	if !found {
		return []string{propName}
	}

	// Known default properties fetched dynamically from ClickHouse
	knownDefaults := h.getKnownDefaultProperties(projectID)

	var expanded []string
	if isEventProp || foundInEventProps {
		// Resolve children from LexiconEventProperty
		var children []database.LexiconEventProperty
		h.DB.Select("name").Where("project_id = ? AND merged_into_id = ?", projectID, virtualID).Find(&children)
		for _, child := range children {
			if knownDefaults[child.Name] {
				expanded = append(expanded, "default_"+child.Name)
			} else {
				expanded = append(expanded, "prop_"+child.Name)
			}
		}
	} else {
		// Resolve children from LexiconProfileProperty
		var children []database.LexiconProfileProperty
		h.DB.Select("name").Where("project_id = ? AND merged_into_id = ?", projectID, virtualID).Find(&children)
		for _, child := range children {
			expanded = append(expanded, child.Name)
		}
	}

	if len(expanded) == 0 {
		return []string{propName}
	}
	return expanded
}
