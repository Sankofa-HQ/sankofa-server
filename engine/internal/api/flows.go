package api

import (
	"encoding/json"
	"log"
	"sankofa/engine/internal/database"
	"sankofa/engine/internal/models"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type FlowsHandler struct {
	db            *gorm.DB
	chConn        driver.Conn
	eventsHandler *EventsHandler
}

func NewFlowsHandler(db *gorm.DB, chConn driver.Conn, eventsHandler *EventsHandler) *FlowsHandler {
	return &FlowsHandler{
		db:            db,
		chConn:        chConn,
		eventsHandler: eventsHandler,
	}
}

func (h *FlowsHandler) RegisterRoutes(router fiber.Router, authMiddleware fiber.Handler) {
	flows := router.Group("/projects/:project_id/flows")
	flows.Use(authMiddleware)

	flows.Post("/calculate", h.CalculateFlow)
	flows.Post("/", h.CreateSavedFlow)
	flows.Get("/", h.ListSavedFlows)
	flows.Get("/:id", h.GetSavedFlow)
	flows.Put("/:id", h.UpdateSavedFlow)
	flows.Delete("/:id", h.DeleteSavedFlow)
}

func (h *FlowsHandler) CalculateFlow(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	if projectID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Project ID is required"})
	}

	var req models.FlowRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}
	req.ProjectID = projectID

	// Expand start event if it's virtual/merged
	ctx := c.Context()
	expandedStart := ExpandVirtualEventNames(h.db, req.ProjectID, []string{req.StartEvent})
	if len(expandedStart) > 0 {
		req.StartEventExpanded = expandedStart
	} else {
		req.StartEventExpanded = []string{req.StartEvent}
	}

	query, args := database.BuildFlowQuery(req)

	rows, err := h.chConn.Query(ctx, query, args...)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to calculate flow"})
	}
	defer rows.Close()

	// Parse Nivo Sankey Results
	nodeSet := make(map[string]models.FlowNode)
	var links []models.FlowLink

	for rows.Next() {
		var source string
		var target string
		var value uint64
		var stepLevel uint32

		if err := rows.Scan(&source, &target, &value, &stepLevel); err != nil {
			log.Printf("Flow scan error: %v", err)
			return c.Status(500).JSON(fiber.Map{"error": "Failed to parse flow results"})
		}

		if _, exists := nodeSet[source]; !exists {
			nodeSet[source] = models.FlowNode{ID: source}
		}
		if _, exists := nodeSet[target]; !exists {
			nodeSet[target] = models.FlowNode{ID: target}
		}

		links = append(links, models.FlowLink{
			Source: source,
			Target: target,
			Value:  int(value),
		})
	}

	var nodes []models.FlowNode
	for _, n := range nodeSet {
		nodes = append(nodes, n)
	}

	return c.JSON(models.FlowResult{
		Nodes: nodes,
		Links: links,
	})
}

type SaveFlowRequest struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	QueryAST    interface{} `json:"query_ast"`
	IsPinned    bool        `json:"is_pinned"`
}

func (h *FlowsHandler) CreateSavedFlow(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	var req SaveFlowRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	astBytes, _ := json.Marshal(req.QueryAST)

	flow := database.SavedFlow{
		ProjectID:   projectID,
		Name:        req.Name,
		Description: req.Description,
		QueryAST:    astBytes,
		IsPinned:    req.IsPinned,
		CreatedByID: userID,
	}

	if err := h.db.Create(&flow).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to save flow"})
	}

	return c.JSON(flow)
}

func (h *FlowsHandler) ListSavedFlows(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	var flows []database.SavedFlow

	if err := h.db.Where("project_id = ?", projectID).Order("created_at desc").Find(&flows).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to list flows"})
	}

	return c.JSON(flows)
}

func (h *FlowsHandler) GetSavedFlow(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	flowID := c.Params("id")

	var flow database.SavedFlow
	if err := h.db.Where("id = ? AND project_id = ?", flowID, projectID).First(&flow).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Flow not found"})
	}

	return c.JSON(flow)
}

func (h *FlowsHandler) UpdateSavedFlow(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	flowID := c.Params("id")

	var req SaveFlowRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	var flow database.SavedFlow
	if err := h.db.Where("id = ? AND project_id = ?", flowID, projectID).First(&flow).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Flow not found"})
	}

	flow.Name = req.Name
	flow.Description = req.Description
	flow.IsPinned = req.IsPinned
	if req.QueryAST != nil {
		astBytes, _ := json.Marshal(req.QueryAST)
		flow.QueryAST = astBytes
	}

	if err := h.db.Save(&flow).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to update flow"})
	}

	return c.JSON(flow)
}

func (h *FlowsHandler) DeleteSavedFlow(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	flowID := c.Params("id")

	if err := h.db.Where("id = ? AND project_id = ?", flowID, projectID).Delete(&database.SavedFlow{}).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete flow"})
	}

	return c.SendStatus(204)
}
