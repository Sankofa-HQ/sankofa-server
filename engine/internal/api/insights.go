package api

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sankofa/engine/internal/database"
	"sankofa/engine/internal/models"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
	"time"
)

type InsightsHandler struct {
	db            *gorm.DB
	chConn        driver.Conn
	eventsHandler *EventsHandler
}

func NewInsightsHandler(db *gorm.DB, chConn driver.Conn, eventsHandler *EventsHandler) *InsightsHandler {
	return &InsightsHandler{db: db, chConn: chConn, eventsHandler: eventsHandler}
}

func (h *InsightsHandler) RegisterRoutes(router fiber.Router, middlewares ...fiber.Handler) {
	insights := router.Group("/projects/:project_id/insights", middlewares...)
	insights.Post("/", h.QueryInsight)
	insights.Post("/users", h.InsightUsers)
	insights.Post("/saved", h.CreateSavedInsight)
	insights.Get("/saved", h.ListSavedInsights)
	insights.Get("/saved/:id", h.GetSavedInsight)
	insights.Put("/saved/:id", h.UpdateSavedInsight)
	insights.Delete("/saved/:id", h.DeleteSavedInsight)
}

// ── QueryInsight executes an Insight time-series query against ClickHouse ──

func (h *InsightsHandler) QueryInsight(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	if projectID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Project ID is required"})
	}

	var req models.InsightRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	req.ProjectID = projectID

	// Extract project from context (populated by middleware)
	if project, ok := c.Locals("project").(database.Project); ok {
		req.Timezone = project.Timezone
	}
	if req.Timezone == "" {
		req.Timezone = "UTC"
	}

	if req.Environment == "" {
		req.Environment = c.Query("environment", "live")
	}

	if len(req.Metrics) == 0 {
		return c.Status(400).JSON(fiber.Map{"error": "At least one metric is required"})
	}

	// ── Pre-process: Expand virtual/merged event names ──
	for i, metric := range req.Metrics {
		if metric.EventName != "" {
			expanded := ExpandVirtualEventNames(h.db, req.ProjectID, req.Environment, []string{metric.EventName})
			if len(expanded) > 0 {
				req.Metrics[i].ExpandedEvents = expanded
			} else {
				req.Metrics[i].ExpandedEvents = []string{metric.EventName}
			}
		}
	}

	// ── Pre-process: Expand virtual/merged property names in global filters ──
	for i, f := range req.GlobalFilters {
		expanded := ExpandVirtualPropertyNames(h.db, projectID, req.Environment, f.Property)
		if len(expanded) > 1 || (len(expanded) == 1 && expanded[0] != f.Property) {
			req.GlobalFilters[i].ExpandedProperties = expanded
		}
	}

	// ── Pre-process: Expand virtual/merged property names in per-metric filters ──
	for mi, metric := range req.Metrics {
		for fi, f := range metric.Filters {
			expanded := ExpandVirtualPropertyNames(h.db, projectID, req.Environment, f.Property)
			if len(expanded) > 1 || (len(expanded) == 1 && expanded[0] != f.Property) {
				req.Metrics[mi].Filters[fi].ExpandedProperties = expanded
			}
		}
	}

	// ── Pre-process: Expand virtual/merged property names in breakdowns ──
	req.ExpandedBreakdowns = make([][]string, len(req.Breakdowns))
	for i, bd := range req.Breakdowns {
		expanded := ExpandVirtualPropertyNames(h.db, projectID, req.Environment, bd)
		if len(expanded) == 0 {
			expanded = []string{bd}
		}
		req.ExpandedBreakdowns[i] = expanded
	}

	// ── Build and execute ClickHouse query ──
	query, args := database.BuildInsightQuery(req)

	fmt.Println("=== Insight Query ===")
	fmt.Println(query)
	fmt.Println("=== Insight Args ===", args)

	ctx := c.Context()
	rows, err := h.chConn.Query(ctx, query, args...)
	if err != nil {
		fmt.Println("Insight Query Error:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to execute insight query"})
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
			fmt.Println("Insight Scan Error:", err)
			return c.Status(500).JSON(fiber.Map{"error": "Failed to parse insight results"})
		}

		rowMap := fiber.Map{}
		for i, colName := range cols {
			val := reflect.ValueOf(pointers[i]).Elem().Interface()
			rowMap[colName] = val
		}
		results = append(results, rowMap)
	}

	return c.JSON(results)
}

type InsightUsersRequest struct {
	models.InsightRequest
	TargetMetric     int       `json:"target_metric"`
	TargetTimeBucket time.Time `json:"target_time_bucket"`
	Segment          string    `json:"segment"` // e.g. "MacOS · 10.15" or ""
	Limit            int       `json:"limit"`
	Offset           int       `json:"offset"`
}

func (h *InsightsHandler) InsightUsers(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	if projectID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Project ID is required"})
	}

	var req InsightUsersRequest
	if err := c.BodyParser(&req); err != nil {
		fmt.Println("InsightUsers BodyParser Error:", err)
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	req.ProjectID = projectID

	// Extract project from context (populated by middleware)
	if project, ok := c.Locals("project").(database.Project); ok {
		req.Timezone = project.Timezone
	}
	if req.Timezone == "" {
		req.Timezone = "UTC"
	}
	if req.Environment == "" {
		req.Environment = c.Query("environment", "live")
	}

	// ── Pre-process: Expand virtual/merged event names ──
	for i, metric := range req.Metrics {
		if metric.EventName != "" {
			expanded := ExpandVirtualEventNames(h.db, req.ProjectID, req.Environment, []string{metric.EventName})
			if len(expanded) > 0 {
				req.Metrics[i].ExpandedEvents = expanded
			} else {
				req.Metrics[i].ExpandedEvents = []string{metric.EventName}
			}
		}
	}

	// ── Pre-process: Expand virtual/merged property names in global filters ──
	for i, f := range req.GlobalFilters {
		expanded := ExpandVirtualPropertyNames(h.db, projectID, req.Environment, f.Property)
		if len(expanded) > 1 || (len(expanded) == 1 && expanded[0] != f.Property) {
			req.GlobalFilters[i].ExpandedProperties = expanded
		}
	}

	// ── Pre-process: Expand virtual/merged property names in per-metric filters ──
	for mi, metric := range req.Metrics {
		for fi, f := range metric.Filters {
			expanded := ExpandVirtualPropertyNames(h.db, projectID, req.Environment, f.Property)
			if len(expanded) > 1 || (len(expanded) == 1 && expanded[0] != f.Property) {
				req.Metrics[mi].Filters[fi].ExpandedProperties = expanded
			}
		}
	}

	// ── Pre-process: Expand virtual/merged property names in breakdowns ──
	req.ExpandedBreakdowns = make([][]string, len(req.Breakdowns))
	for i, bd := range req.Breakdowns {
		expanded := ExpandVirtualPropertyNames(h.db, projectID, req.Environment, bd)
		if len(expanded) == 0 {
			expanded = []string{bd}
		}
		req.ExpandedBreakdowns[i] = expanded
	}

	// ── Build and execute ClickHouse query ──
	query, args := database.BuildInsightUsersQuery(req.InsightRequest, req.TargetMetric, req.TargetTimeBucket, req.Segment)

	fmt.Println("=== Insight Users Query ===")
	fmt.Println(query)
	fmt.Println("=== Insight Users Args ===", args)

	ctx := c.Context()
	rows, err := h.chConn.Query(ctx, query, args...)
	if err != nil {
		fmt.Println("Insight Users Query Error:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to execute insight users query"})
	}
	defer rows.Close()

	var distinctIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			fmt.Println("Insight Users Scan Error:", err)
			continue
		}
		distinctIDs = append(distinctIDs, id)
	}

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

	// Apply limit/offset
	if req.Limit <= 0 {
		req.Limit = 100
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

	personProps := make(map[string]UserRes)
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
		defer enrichRows.Close()
		for enrichRows.Next() {
			var did string
			var props map[string]string
			var lastSeen time.Time
			if err := enrichRows.Scan(&did, &props, &lastSeen); err != nil {
				continue
			}

			email := ""
			name := ""
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
				DistinctID: did,
				Email:      email,
				Name:       name,
				Properties: props,
				CreatedAt:  lastSeen.Format(time.RFC3339),
			}
		}
	}

	// Build final list keeping order from ClickHouse if possible
	finalUsers := make([]UserRes, 0)
	for _, did := range pagedIDs {
		if u, ok := personProps[did]; ok {
			finalUsers = append(finalUsers, u)
		} else {
			// User not found in persons table, return with just ID
			finalUsers = append(finalUsers, UserRes{
				DistinctID: did,
			})
		}
	}

	return c.JSON(fiber.Map{
		"users": finalUsers,
	})
}

// ── CRUD for Saved Insights ──

type SaveInsightRequest struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Environment string      `json:"environment"`
	QueryAST    interface{} `json:"query_ast"`
	IsPinned    bool        `json:"is_pinned"`
}

func (h *InsightsHandler) CreateSavedInsight(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		// This should be handled by RequireAuth, but keeping it for safety in case userID is used below
	}

	projectID := c.Params("project_id")
	if projectID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Project ID is required"})
	}

	var project database.Project
	if err := h.db.First(&project, "id = ?", projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}
	// --- checkProjectAccess is now handled by middleware ---

	var req SaveInsightRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	if req.Name == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Name is required"})
	}

	astBytes, err := json.Marshal(req.QueryAST)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid Query AST"})
	}

	if req.Environment == "" {
		req.Environment = "live"
	}

	insight := database.SavedInsight{
		ProjectID:   projectID,
		Environment: req.Environment,
		Name:        req.Name,
		Description: req.Description,
		QueryAST:    astBytes,
		IsPinned:    req.IsPinned,
		CreatedByID: userID,
	}

	if err := h.db.Create(&insight).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to save insight"})
	}

	return c.JSON(insight)
}

func (h *InsightsHandler) ListSavedInsights(c *fiber.Ctx) error {
	// userID is now handled by middleware check

	projectID := c.Params("project_id")
	if projectID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Project ID is required"})
	}

	var project database.Project
	if err := h.db.First(&project, "id = ?", projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}
	// --- checkProjectAccess is now handled by middleware ---

	var insights []database.SavedInsight
	query := h.db.Where("project_id = ?", projectID)

	env := c.Query("environment")
	if env != "" {
		query = query.Where("environment = ?", env)
	}

	if err := query.Preload("CreatedBy").Order("created_at DESC").Find(&insights).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch saved insights"})
	}

	return c.JSON(insights)
}

func (h *InsightsHandler) GetSavedInsight(c *fiber.Ctx) error {
	// userID is now handled by middleware check

	projectID := c.Params("project_id")
	insightID := c.Params("id")

	var project database.Project
	if err := h.db.First(&project, "id = ?", projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}
	// --- checkProjectAccess is now handled by middleware ---

	var insight database.SavedInsight
	query := h.db.Where("id = ? AND project_id = ?", insightID, projectID)

	env := c.Query("environment")
	if env != "" {
		query = query.Where("environment = ?", env)
	}

	if err := query.First(&insight).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Saved insight not found"})
	}

	return c.JSON(insight)
}

func (h *InsightsHandler) UpdateSavedInsight(c *fiber.Ctx) error {
	// userID is now handled by middleware check

	projectID := c.Params("project_id")
	insightID := c.Params("id")

	var project database.Project
	if err := h.db.First(&project, "id = ?", projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}
	// --- checkProjectAccess is now handled by middleware ---

	var insight database.SavedInsight
	query := h.db.Where("id = ? AND project_id = ?", insightID, projectID)

	env := c.Query("environment")
	if env != "" {
		query = query.Where("environment = ?", env)
	}

	if err := query.First(&insight).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Saved insight not found"})
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
		insight.Name = *req.Name
	}
	if req.Description != nil {
		insight.Description = *req.Description
	}
	if req.IsPinned != nil {
		insight.IsPinned = *req.IsPinned
	}
	if req.QueryAST != nil {
		astBytes, err := json.Marshal(*req.QueryAST)
		if err == nil {
			insight.QueryAST = astBytes
		}
	}

	if err := h.db.Save(&insight).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to update saved insight"})
	}

	return c.JSON(insight)
}

func (h *InsightsHandler) DeleteSavedInsight(c *fiber.Ctx) error {
	// userID is now handled by middleware check

	projectID := c.Params("project_id")
	insightID := c.Params("id")

	var project database.Project
	if err := h.db.First(&project, "id = ?", projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}
	// --- checkProjectAccess is now handled by middleware ---

	query := h.db.Where("id = ? AND project_id = ?", insightID, projectID)

	env := c.Query("environment")
	if env != "" {
		query = query.Where("environment = ?", env)
	}

	if err := query.Delete(&database.SavedInsight{}).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete saved insight"})
	}

	return c.SendStatus(204)
}
