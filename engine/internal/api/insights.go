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
)

type InsightsHandler struct {
	db            *gorm.DB
	chConn        driver.Conn
	eventsHandler *EventsHandler
}

func NewInsightsHandler(db *gorm.DB, chConn driver.Conn, eventsHandler *EventsHandler) *InsightsHandler {
	return &InsightsHandler{db: db, chConn: chConn, eventsHandler: eventsHandler}
}

func (h *InsightsHandler) RegisterRoutes(router fiber.Router, authMiddleware fiber.Handler) {
	insights := router.Group("/projects/:project_id/insights")
	insights.Use(authMiddleware)
	insights.Post("/", h.QueryInsight)
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

	if len(req.Metrics) == 0 {
		return c.Status(400).JSON(fiber.Map{"error": "At least one metric is required"})
	}

	// ── Pre-process: Expand virtual/merged event names ──
	for i, metric := range req.Metrics {
		if metric.EventName != "" {
			expanded := ExpandVirtualEventNames(h.db, req.ProjectID, []string{metric.EventName})
			if len(expanded) > 0 {
				req.Metrics[i].ExpandedEvents = expanded
			} else {
				req.Metrics[i].ExpandedEvents = []string{metric.EventName}
			}
		}
	}

	// ── Pre-process: Expand virtual/merged property names in global filters ──
	for i, f := range req.GlobalFilters {
		expanded := ExpandVirtualPropertyNames(h.db, projectID, f.Property)
		if len(expanded) > 1 || (len(expanded) == 1 && expanded[0] != f.Property) {
			req.GlobalFilters[i].ExpandedProperties = expanded
		}
	}

	// ── Pre-process: Expand virtual/merged property names in per-metric filters ──
	for mi, metric := range req.Metrics {
		for fi, f := range metric.Filters {
			expanded := ExpandVirtualPropertyNames(h.db, projectID, f.Property)
			if len(expanded) > 1 || (len(expanded) == 1 && expanded[0] != f.Property) {
				req.Metrics[mi].Filters[fi].ExpandedProperties = expanded
			}
		}
	}

	// ── Pre-process: Expand virtual/merged property names in breakdowns ──
	req.ExpandedBreakdowns = make([][]string, len(req.Breakdowns))
	for i, bd := range req.Breakdowns {
		expanded := ExpandVirtualPropertyNames(h.db, projectID, bd)
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

// ── CRUD for Saved Insights ──

type SaveInsightRequest struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	QueryAST    interface{} `json:"query_ast"`
	IsPinned    bool        `json:"is_pinned"`
}

func (h *InsightsHandler) CreateSavedInsight(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	projectID := c.Params("project_id")
	if projectID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Project ID is required"})
	}

	var project database.Project
	if err := h.db.First(&project, "id = ?", projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}
	if !checkProjectAccess(h.db, &project, userID) {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied"})
	}

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

	insight := database.SavedInsight{
		ProjectID:   projectID,
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
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	projectID := c.Params("project_id")
	if projectID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Project ID is required"})
	}

	var project database.Project
	if err := h.db.First(&project, "id = ?", projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}
	if !checkProjectAccess(h.db, &project, userID) {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied"})
	}

	var insights []database.SavedInsight
	if err := h.db.Where("project_id = ?", projectID).Preload("CreatedBy").Order("created_at DESC").Find(&insights).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch saved insights"})
	}

	return c.JSON(insights)
}

func (h *InsightsHandler) GetSavedInsight(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	projectID := c.Params("project_id")
	insightID := c.Params("id")

	var project database.Project
	if err := h.db.First(&project, "id = ?", projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}
	if !checkProjectAccess(h.db, &project, userID) {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied"})
	}

	var insight database.SavedInsight
	if err := h.db.Where("id = ? AND project_id = ?", insightID, projectID).First(&insight).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Saved insight not found"})
	}

	return c.JSON(insight)
}

func (h *InsightsHandler) UpdateSavedInsight(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	projectID := c.Params("project_id")
	insightID := c.Params("id")

	var project database.Project
	if err := h.db.First(&project, "id = ?", projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}
	if !checkProjectAccess(h.db, &project, userID) {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied"})
	}

	var insight database.SavedInsight
	if err := h.db.Where("id = ? AND project_id = ?", insightID, projectID).First(&insight).Error; err != nil {
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
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	projectID := c.Params("project_id")
	insightID := c.Params("id")

	var project database.Project
	if err := h.db.First(&project, "id = ?", projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}
	if !checkProjectAccess(h.db, &project, userID) {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied"})
	}

	if err := h.db.Where("id = ? AND project_id = ?", insightID, projectID).Delete(&database.SavedInsight{}).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete saved insight"})
	}

	return c.SendStatus(204)
}
