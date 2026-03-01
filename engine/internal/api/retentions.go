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

	// Expand Events
	expandedStart := ExpandVirtualEventNames(h.db, req.ProjectID, []string{req.StartEvent})
	if len(expandedStart) > 0 {
		req.ExpandedStartEvent = expandedStart
	} else {
		req.ExpandedStartEvent = []string{req.StartEvent}
	}

	expandedReturn := ExpandVirtualEventNames(h.db, req.ProjectID, []string{req.ReturnEvent})
	if len(expandedReturn) > 0 {
		req.ExpandedReturnEvent = expandedReturn
	} else {
		req.ExpandedReturnEvent = []string{req.ReturnEvent}
	}

	// Filter Expansions
	for i, f := range req.GlobalFilters {
		expanded := h.eventsHandler.expandVirtualPropertyNames(projectID, f.Property)
		if len(expanded) > 1 || (len(expanded) == 1 && expanded[0] != f.Property) {
			req.GlobalFilters[i].ExpandedProperties = expanded
		}
	}

	req.ExpandedBreakdowns = make([][]string, len(req.Breakdowns))
	for i, bd := range req.Breakdowns {
		expanded := h.eventsHandler.expandVirtualPropertyNames(projectID, bd)
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
			rowMap[colName] = val
		}
		results = append(results, rowMap)
	}

	return c.JSON(results)
}

// ... CRUD Handlers ...

type SaveRetentionRequest struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
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

	retention := database.SavedRetention{
		ProjectID:   projectID,
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
	if err := h.db.Where("project_id = ?", projectID).Preload("CreatedBy").Order("created_at DESC").Find(&retentions).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch saved retentions"})
	}

	return c.JSON(retentions)
}

func (h *RetentionsHandler) GetSavedRetention(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	retentionID := c.Params("id")
	var retention database.SavedRetention
	if err := h.db.Where("id = ? AND project_id = ?", retentionID, projectID).First(&retention).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Saved retention not found"})
	}
	return c.JSON(retention)
}

func (h *RetentionsHandler) UpdateSavedRetention(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	retentionID := c.Params("id")

	var retention database.SavedRetention
	if err := h.db.Where("id = ? AND project_id = ?", retentionID, projectID).First(&retention).Error; err != nil {
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

	if err := h.db.Where("id = ? AND project_id = ?", retentionID, projectID).Delete(&database.SavedRetention{}).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete saved retention"})
	}
	return c.SendStatus(204)
}
