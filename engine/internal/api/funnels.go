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

type FunnelsHandler struct {
	db            *gorm.DB
	chConn        driver.Conn
	eventsHandler *EventsHandler
}

func NewFunnelsHandler(db *gorm.DB, chConn driver.Conn, eventsHandler *EventsHandler) *FunnelsHandler {
	return &FunnelsHandler{db: db, chConn: chConn, eventsHandler: eventsHandler}
}

func (h *FunnelsHandler) RegisterRoutes(router fiber.Router, authMiddleware fiber.Handler) {
	// POST /api/v1/projects/:project_id/funnels
	funnels := router.Group("/projects/:project_id/funnels")
	funnels.Use(authMiddleware)
	funnels.Post("/", h.CalculateFunnel)
	funnels.Post("/saved", h.CreateSavedFunnel)
	funnels.Get("/saved", h.ListSavedFunnels)
	funnels.Get("/saved/:id", h.GetSavedFunnel)
	funnels.Put("/saved/:id", h.UpdateSavedFunnel)
	funnels.Delete("/saved/:id", h.DeleteSavedFunnel)
}

func (h *FunnelsHandler) CalculateFunnel(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	if projectID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Project ID is required"})
	}

	var req models.FunnelRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	// Ensure projectID is populated from URL path overrides body
	req.ProjectID = projectID

	// Evaluate merged events
	for i, step := range req.Steps {
		if step.EventName != "" {
			expanded := ExpandVirtualEventNames(h.db, req.ProjectID, []string{step.EventName})
			if len(expanded) > 0 {
				req.Steps[i].ExpandedEvents = expanded
			} else {
				req.Steps[i].ExpandedEvents = []string{step.EventName}
			}
		}
	}

	// Expand virtual/merged property names in global filters
	fmt.Printf("=== Funnel GlobalFilters (%d) ===\n", len(req.GlobalFilters))
	for i, f := range req.GlobalFilters {
		fmt.Printf("  Filter[%d]: property=%q operator=%q values=%v\n", i, f.Property, f.Operator, f.Values)
		expanded := h.eventsHandler.expandVirtualPropertyNames(projectID, f.Property)
		fmt.Printf("  Filter[%d]: expanded=%v\n", i, expanded)
		if len(expanded) > 1 || (len(expanded) == 1 && expanded[0] != f.Property) {
			req.GlobalFilters[i].ExpandedProperties = expanded
		}
	}

	// Expand virtual/merged property names in per-step filters
	for si, step := range req.Steps {
		for fi, f := range step.Filters {
			expanded := h.eventsHandler.expandVirtualPropertyNames(projectID, f.Property)
			if len(expanded) > 1 || (len(expanded) == 1 && expanded[0] != f.Property) {
				req.Steps[si].Filters[fi].ExpandedProperties = expanded
			}
		}
	}

	// Expand virtual/merged property names in breakdowns
	req.ExpandedBreakdowns = make([][]string, len(req.Breakdowns))
	for i, bd := range req.Breakdowns {
		expanded := h.eventsHandler.expandVirtualPropertyNames(projectID, bd)
		if len(expanded) == 0 {
			expanded = []string{bd} // fallback to raw key
		}
		req.ExpandedBreakdowns[i] = expanded
		fmt.Printf("  Breakdown[%d]: %q → expanded=%v\n", i, bd, expanded)
	}

	var query string
	var args []any

	var windowSeconds int
	if req.WindowValue > 0 && req.WindowUnit != "" {
		switch req.WindowUnit {
		case "seconds":
			windowSeconds = req.WindowValue
		case "minutes":
			windowSeconds = req.WindowValue * 60
		case "hours":
			windowSeconds = req.WindowValue * 3600
		case "days":
			windowSeconds = req.WindowValue * 86400
		case "weeks":
			windowSeconds = req.WindowValue * 604800
		case "months":
			windowSeconds = req.WindowValue * 2592000
		case "sessions":
			// Assuming a session inactivity break is typically 30m, 1 session interval could map equivalently or just fallback.
			windowSeconds = req.WindowValue * 1800
		default:
			windowSeconds = req.WindowValue * 86400
		}
	} else {
		windowSeconds = 604800 // Default to 7 days if unset
	}

	if models.RequiresSequenceMatch(req) {
		query, args = database.BuildSequenceMatchQuery(req, windowSeconds)
	} else {
		query, args = database.BuildWindowFunnelQuery(req, windowSeconds)
	}

	fmt.Println("=== Funnel Query ===")
	fmt.Println(query)
	fmt.Println("=== Funnel Args ===", args)
	fmt.Println("=== Window Seconds ===", windowSeconds)

	ctx := c.Context()
	rows, err := h.chConn.Query(ctx, query, args...)
	if err != nil {
		fmt.Println("Funnel Query Gen:", query)
		fmt.Println("Funnel Query Args:", args)
		fmt.Println("Funnel Query Error:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to calculate funnel"})
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
			fmt.Println("Funnel Scan Error:", err)
			return c.Status(500).JSON(fiber.Map{"error": "Failed to parse funnel results"})
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

type SaveFunnelRequest struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	QueryAST    interface{} `json:"query_ast"` // Accepts any JSON object
	IsPinned    bool        `json:"is_pinned"`
}

func (h *FunnelsHandler) CreateSavedFunnel(c *fiber.Ctx) error {
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

	var req SaveFunnelRequest
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

	funnel := database.SavedFunnel{
		ProjectID:   projectID,
		Name:        req.Name,
		Description: req.Description,
		QueryAST:    astBytes,
		IsPinned:    req.IsPinned,
		CreatedByID: userID,
	}

	if err := h.db.Create(&funnel).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to save funnel"})
	}

	return c.JSON(funnel)
}

func (h *FunnelsHandler) ListSavedFunnels(c *fiber.Ctx) error {
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

	var funnels []database.SavedFunnel
	if err := h.db.Where("project_id = ?", projectID).Preload("CreatedBy").Order("created_at DESC").Find(&funnels).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch saved funnels"})
	}

	return c.JSON(funnels)
}

func (h *FunnelsHandler) GetSavedFunnel(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	projectID := c.Params("project_id")
	funnelID := c.Params("id")

	var project database.Project
	if err := h.db.First(&project, "id = ?", projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}
	if !checkProjectAccess(h.db, &project, userID) {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied"})
	}

	var funnel database.SavedFunnel
	if err := h.db.Where("id = ? AND project_id = ?", funnelID, projectID).First(&funnel).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Saved funnel not found"})
	}

	return c.JSON(funnel)
}

func (h *FunnelsHandler) UpdateSavedFunnel(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	projectID := c.Params("project_id")
	funnelID := c.Params("id")

	var project database.Project
	if err := h.db.First(&project, "id = ?", projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}
	if !checkProjectAccess(h.db, &project, userID) {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied"})
	}

	var funnel database.SavedFunnel
	if err := h.db.Where("id = ? AND project_id = ?", funnelID, projectID).First(&funnel).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Saved funnel not found"})
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
		funnel.Name = *req.Name
	}
	if req.Description != nil {
		funnel.Description = *req.Description
	}
	if req.IsPinned != nil {
		funnel.IsPinned = *req.IsPinned
	}
	if req.QueryAST != nil {
		astBytes, err := json.Marshal(*req.QueryAST)
		if err == nil {
			funnel.QueryAST = astBytes
		}
	}

	if err := h.db.Save(&funnel).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to update saved funnel"})
	}

	return c.JSON(funnel)
}

func (h *FunnelsHandler) DeleteSavedFunnel(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	projectID := c.Params("project_id")
	funnelID := c.Params("id")

	var project database.Project
	if err := h.db.First(&project, "id = ?", projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}
	if !checkProjectAccess(h.db, &project, userID) {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied"})
	}

	if err := h.db.Where("id = ? AND project_id = ?", funnelID, projectID).Delete(&database.SavedFunnel{}).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete saved funnel"})
	}

	return c.SendStatus(204)
}
