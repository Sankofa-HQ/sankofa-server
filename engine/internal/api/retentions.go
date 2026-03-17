package api

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
	"sankofa/engine/internal/database"
	"sankofa/engine/internal/models"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type RetentionsHandler struct {
	db            *gorm.DB
	chConn        driver.Conn
	eventsHandler *EventsHandler
}

func NewRetentionsHandler(db *gorm.DB, chConn driver.Conn, eventsHandler *EventsHandler) *RetentionsHandler {
	return &RetentionsHandler{db: db, chConn: chConn, eventsHandler: eventsHandler}
}

func (h *RetentionsHandler) RegisterRoutes(router fiber.Router, authMiddleware fiber.Handler) {
	retentions := router.Group("/projects/:project_id/retentions")
	retentions.Use(authMiddleware)
	retentions.Post("/", h.CalculateRetention)
	retentions.Post("/users", h.RetentionUsers)
	retentions.Post("/saved", h.CreateSavedRetention)
	retentions.Get("/saved", h.ListSavedRetentions)
	retentions.Get("/saved/:id", h.GetSavedRetention)
	retentions.Put("/saved/:id", h.UpdateSavedRetention)
	retentions.Delete("/saved/:id", h.DeleteSavedRetention)
}

func (h *RetentionsHandler) CalculateRetention(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	if projectID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Project ID is required"})
	}

	var req models.RetentionRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	req.ProjectID = projectID
	if req.Environment == "" {
		req.Environment = c.Query("environment", "live")
	}

	// Expand Events
	expandedStart := ExpandVirtualEventNames(h.db, req.ProjectID, req.Environment, []string{req.StartEvent})
	if len(expandedStart) > 0 {
		req.ExpandedStartEvent = expandedStart
	} else {
		req.ExpandedStartEvent = []string{req.StartEvent}
	}

	expandedReturn := ExpandVirtualEventNames(h.db, req.ProjectID, req.Environment, []string{req.ReturnEvent})
	if len(expandedReturn) > 0 {
		req.ExpandedReturnEvent = expandedReturn
	} else {
		req.ExpandedReturnEvent = []string{req.ReturnEvent}
	}

	// Filter Expansions
	for i, f := range req.GlobalFilters {
		if f.Property == "event_name" {
			expanded := ExpandVirtualEventNames(h.db, projectID, req.Environment, f.Values)
			req.GlobalFilters[i].Values = expanded
			if len(expanded) > 1 {
				if f.Operator == "eq" || f.Operator == "is" {
					req.GlobalFilters[i].Operator = "in"
				} else if f.Operator == "neq" || f.Operator == "is_not" {
					req.GlobalFilters[i].Operator = "not_in"
				}
			}
		} else {
			expanded := ExpandVirtualPropertyNames(h.db, projectID, req.Environment, f.Property)
			if len(expanded) > 1 || (len(expanded) == 1 && expanded[0] != f.Property) {
				req.GlobalFilters[i].ExpandedProperties = expanded
			}
		}
	}

	req.ExpandedBreakdowns = make([][]string, len(req.Breakdowns))
	for i, bd := range req.Breakdowns {
		expanded := ExpandVirtualPropertyNames(h.db, projectID, req.Environment, bd)
		if len(expanded) == 0 {
			expanded = []string{bd}
		}
		req.ExpandedBreakdowns[i] = expanded
	}

	query, args := database.BuildRetentionQuery(req)

	fmt.Println("=== Retention Query ===")
	fmt.Println(query)

	ctx := c.Context()
	rows, err := h.chConn.Query(ctx, query, args...)
	if err != nil {
		fmt.Println("Retention Query Error:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to calculate retention"})
	}
	defer rows.Close()

	results := []fiber.Map{}
	cols := rows.Columns()
	colTypes := rows.ColumnTypes()

	for rows.Next() {
		values := make([]interface{}, len(cols))
		pointers := make([]interface{}, len(cols))
		for i := range values {
			if colTypes != nil && i < len(colTypes) {
				pointers[i] = reflect.New(colTypes[i].ScanType()).Interface()
			} else {
				pointers[i] = &values[i]
			}
		}

		if err := rows.Scan(pointers...); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to parse retention results"})
		}

		rowMap := fiber.Map{}
		for i, colName := range cols {
			val := reflect.ValueOf(pointers[i]).Elem().Interface()

			// ClickHouse `sumArray` often returns `[]uint64` or `[]uint8`.
			// To ensure fiber.JSON cleanly serializes it as a JSON array (and not optionally as a base64 string or complex object),
			// we explicitly cast it to a generic slice of interface{} or directly map it safely.
			if colName == "retention_array" {
				switch arr := val.(type) {
				case []uint64:
					val = arr
				case []uint8:
					// Depending on CH driver config, `[]uint8` easily gets base64 encoded by Go JSON marshaler.
					var intArr []uint64
					for _, v := range arr {
						intArr = append(intArr, uint64(v))
					}
					val = intArr
				case []uint16:
					var intArr []uint64
					for _, v := range arr {
						intArr = append(intArr, uint64(v))
					}
					val = intArr
				case []uint32:
					var intArr []uint64
					for _, v := range arr {
						intArr = append(intArr, uint64(v))
					}
					val = intArr
				case []int8:
					var intArr []uint64
					for _, v := range arr {
						intArr = append(intArr, uint64(v))
					}
					val = intArr
				case []int16:
					var intArr []uint64
					for _, v := range arr {
						intArr = append(intArr, uint64(v))
					}
					val = intArr
				case []int32:
					var intArr []uint64
					for _, v := range arr {
						intArr = append(intArr, uint64(v))
					}
					val = intArr
				case []int64:
					var intArr []uint64
					for _, v := range arr {
						intArr = append(intArr, uint64(v))
					}
					val = intArr
				default:
					fmt.Printf("retention_array has unexpected type: %T\n", val)
				}
			}

			rowMap[colName] = val
		}
		results = append(results, rowMap)
	}

	return c.JSON(results)
}

// RetentionUsers returns enriched user profiles for users in a specific retention cohort bucket.
type RetentionUsersRequest struct {
	models.RetentionRequest
	CohortDate string `json:"cohort_date"` // e.g. "2025-01-01"
	PeriodIdx  int    `json:"period_idx"`  // 0 = cohort start, N = Nth period
	Action     string `json:"action"`      // "retained" or "dropped"
	Limit      int    `json:"limit"`
	Offset     int    `json:"offset"`
}

func (h *RetentionsHandler) RetentionUsers(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	if projectID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Project ID is required"})
	}

	var req RetentionUsersRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}
	req.ProjectID = projectID
	if req.Environment == "" {
		req.Environment = c.Query("environment", "live")
	}
	if req.Limit <= 0 || req.Limit > 1000 {
		req.Limit = 100
	}
	if req.Action == "" {
		req.Action = "retained"
	}

	// Expand events
	expandedStart := ExpandVirtualEventNames(h.db, req.ProjectID, req.Environment, []string{req.StartEvent})
	if len(expandedStart) > 0 {
		req.ExpandedStartEvent = expandedStart
	} else {
		req.ExpandedStartEvent = []string{req.StartEvent}
	}

	expandedReturn := ExpandVirtualEventNames(h.db, req.ProjectID, req.Environment, []string{req.ReturnEvent})
	if len(expandedReturn) > 0 {
		req.ExpandedReturnEvent = expandedReturn
	} else {
		req.ExpandedReturnEvent = []string{req.ReturnEvent}
	}

	// Expand global filters
	for i, f := range req.GlobalFilters {
		if f.Property == "event_name" {
			expanded := ExpandVirtualEventNames(h.db, projectID, req.Environment, f.Values)
			req.GlobalFilters[i].Values = expanded
			if len(expanded) > 1 {
				if f.Operator == "eq" || f.Operator == "is" {
					req.GlobalFilters[i].Operator = "in"
				} else if f.Operator == "neq" || f.Operator == "is_not" {
					req.GlobalFilters[i].Operator = "not_in"
				}
			}
		} else {
			expanded := ExpandVirtualPropertyNames(h.db, projectID, req.Environment, f.Property)
			if len(expanded) > 1 || (len(expanded) == 1 && expanded[0] != f.Property) {
				req.GlobalFilters[i].ExpandedProperties = expanded
			}
		}
	}

	usersQuery, args := database.BuildRetentionUsersQuery(req.RetentionRequest, req.CohortDate, req.PeriodIdx, req.Action)

	fmt.Println("=== Retention Users Query ===")
	fmt.Println(usersQuery)

	ctx := c.Context()
	rows, err := h.chConn.Query(ctx, usersQuery, args...)
	if err != nil {
		fmt.Println("Retention Users Query Error:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch retention users"})
	}

	var distinctIDs []string
	for rows.Next() {
		var did string
		if err := rows.Scan(&did); err != nil {
			continue
		}
		distinctIDs = append(distinctIDs, did)
	}
	rows.Close()

	fmt.Println("=== Retention Users Found ===", len(distinctIDs))

	type UserRes struct {
		DistinctID string            `json:"distinct_id"`
		Email      string            `json:"email"`
		Name       string            `json:"name"`
		Properties map[string]string `json:"properties"`
		CreatedAt  string            `json:"created_at"`
	}

	if len(distinctIDs) == 0 {
		return c.JSON(fiber.Map{"users": []UserRes{}})
	}

	start := req.Offset
	if start > len(distinctIDs) {
		start = len(distinctIDs)
	}
	end := start + req.Limit
	if end > len(distinctIDs) {
		end = len(distinctIDs)
	}
	pagedIDs := distinctIDs[start:end]

	// Enrich from persons table
	personProps := make(map[string]UserRes)
	if len(pagedIDs) > 0 {
		enrichQuery := `
			SELECT
				distinct_id,
				argMax(properties, last_seen) AS props,
				max(last_seen) AS latest_seen
			FROM persons
			WHERE project_id = ? AND environment = ? AND distinct_id IN (?)
			GROUP BY distinct_id
		`
		enrichRows, enrichErr := h.chConn.Query(ctx, enrichQuery, req.ProjectID, req.Environment, pagedIDs)
		if enrichErr == nil {
			for enrichRows.Next() {
				var did string
				var props map[string]string
				var lastSeen time.Time
				if err := enrichRows.Scan(&did, &props, &lastSeen); err != nil {
					continue
				}
				email, name := "", ""
				if v, ok := props["$email"]; ok {
					email = v
				} else if v, ok := props["email"]; ok {
					email = v
				}
				if v, ok := props["$name"]; ok {
					name = v
				} else if v, ok := props["name"]; ok {
					name = v
				}
				personProps[did] = UserRes{
					DistinctID: did, Email: email, Name: name,
					Properties: props, CreatedAt: lastSeen.Format(time.RFC3339),
				}
			}
			enrichRows.Close()
		} else {
			fmt.Println("Persons enrich error (non-fatal):", enrichErr)
		}
	}

	var users []UserRes
	for _, did := range pagedIDs {
		if enriched, ok := personProps[did]; ok {
			users = append(users, enriched)
		} else {
			users = append(users, UserRes{DistinctID: did, Properties: map[string]string{}})
		}
	}
	return c.JSON(fiber.Map{"users": users})
}

// ... CRUD Handlers ...

type SaveRetentionRequest struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Environment string      `json:"environment"`
	QueryAST    interface{} `json:"query_ast"`
	IsPinned    bool        `json:"is_pinned"`
}

func (h *RetentionsHandler) CreateSavedRetention(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	projectID := c.Params("project_id")
	var project database.Project
	if err := h.db.First(&project, "id = ?", projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}
	if !checkProjectAccess(h.db, &project, userID) {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied"})
	}

	var req SaveRetentionRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	astBytes, _ := json.Marshal(req.QueryAST)

	if req.Environment == "" {
		req.Environment = "live"
	}

	retention := database.SavedRetention{
		ProjectID:   projectID,
		Environment: req.Environment,
		Name:        req.Name,
		Description: req.Description,
		QueryAST:    astBytes,
		IsPinned:    req.IsPinned,
		CreatedByID: userID,
	}

	if err := h.db.Create(&retention).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to save retention"})
	}
	return c.JSON(retention)
}

func (h *RetentionsHandler) ListSavedRetentions(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	projectID := c.Params("project_id")
	var project database.Project
	if err := h.db.First(&project, "id = ?", projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}
	if !checkProjectAccess(h.db, &project, userID) {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied"})
	}

	var retentions []database.SavedRetention
	query := h.db.Where("project_id = ?", projectID)

	env := c.Query("environment")
	if env != "" {
		query = query.Where("environment = ?", env)
	}

	if err := query.Preload("CreatedBy").Order("created_at DESC").Find(&retentions).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch saved retentions"})
	}

	return c.JSON(retentions)
}

func (h *RetentionsHandler) GetSavedRetention(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	retentionID := c.Params("id")
	var retention database.SavedRetention
	query := h.db.Where("id = ? AND project_id = ?", retentionID, projectID)

	env := c.Query("environment")
	if env != "" {
		query = query.Where("environment = ?", env)
	}

	if err := query.First(&retention).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Saved retention not found"})
	}
	return c.JSON(retention)
}

func (h *RetentionsHandler) UpdateSavedRetention(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	retentionID := c.Params("id")

	var retention database.SavedRetention
	query := h.db.Where("id = ? AND project_id = ?", retentionID, projectID)

	env := c.Query("environment")
	if env != "" {
		query = query.Where("environment = ?", env)
	}

	if err := query.First(&retention).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Saved retention not found"})
	}

	var req struct {
		Name        *string      `json:"name"`
		Description *string      `json:"description"`
		QueryAST    *interface{} `json:"query_ast"`
		IsPinned    *bool        `json:"is_pinned"`
	}

	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	if req.Name != nil {
		retention.Name = *req.Name
	}
	if req.Description != nil {
		retention.Description = *req.Description
	}
	if req.IsPinned != nil {
		retention.IsPinned = *req.IsPinned
	}
	if req.QueryAST != nil {
		astBytes, _ := json.Marshal(*req.QueryAST)
		retention.QueryAST = astBytes
	}

	h.db.Save(&retention)
	return c.JSON(retention)
}

func (h *RetentionsHandler) DeleteSavedRetention(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	retentionID := c.Params("id")

	query := h.db.Where("id = ? AND project_id = ?", retentionID, projectID)

	env := c.Query("environment")
	if env != "" {
		query = query.Where("environment = ?", env)
	}

	if err := query.Delete(&database.SavedRetention{}).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete saved retention"})
	}
	return c.SendStatus(204)
}
