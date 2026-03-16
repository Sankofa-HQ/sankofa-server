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
	flows.Post("/users", h.FlowUsers)
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
	if req.Environment == "" {
		req.Environment = c.Query("environment", "live")
	}

	// Expand virtual/merged events for the primary anchor
	ctx := c.Context()
	if len(req.Steps) > 0 && req.Steps[0].EventName != "" {
		// Multi-step mode: expand the FIRST step's event for virtual event matching
		expandedStart := ExpandVirtualEventNames(h.db, req.ProjectID, []string{req.Steps[0].EventName})
		if len(expandedStart) > 0 {
			req.StartEventExpanded = expandedStart
		} else {
			req.StartEventExpanded = []string{req.Steps[0].EventName}
		}
	} else if req.StartEvent != "" {
		// Legacy mode: expand start_event
		expandedStart := ExpandVirtualEventNames(h.db, req.ProjectID, []string{req.StartEvent})
		if len(expandedStart) > 0 {
			req.StartEventExpanded = expandedStart
		} else {
			req.StartEventExpanded = []string{req.StartEvent}
		}
	}

	query, args := database.BuildFlowQuery(req)

	rows, err := h.chConn.Query(ctx, query, args...)
	if err != nil {
		log.Printf("Flow query error: %v\nQuery: %s\nArgs: %v", err, query, args)
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

// FlowUsersRequest handles requests to fetch distinct persons for a specific flow node/step
type FlowUsersRequest struct {
	models.FlowRequest
	NodeID string `json:"node_id"`
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
}

func (h *FlowsHandler) FlowUsers(c *fiber.Ctx) error {
	projectID := c.Params("project_id")
	if projectID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Project ID is required"})
	}

	var req FlowUsersRequest
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
	if req.Offset < 0 {
		req.Offset = 0
	}

	// Expand virtual/merged events for the primary anchor
	if len(req.Steps) > 0 && req.Steps[0].EventName != "" {
		expandedStart := ExpandVirtualEventNames(h.db, req.ProjectID, []string{req.Steps[0].EventName})
		if len(expandedStart) > 0 {
			req.StartEventExpanded = expandedStart
		} else {
			req.StartEventExpanded = []string{req.Steps[0].EventName}
		}
	} else if req.StartEvent != "" {
		expandedStart := ExpandVirtualEventNames(h.db, req.ProjectID, []string{req.StartEvent})
		if len(expandedStart) > 0 {
			req.StartEventExpanded = expandedStart
		} else {
			req.StartEventExpanded = []string{req.StartEvent}
		}
	}

	ctx := c.Context()
	query, args := database.BuildFlowUsersQuery(req.FlowRequest, req.NodeID)

	// We only need distinct_ids to join with 'persons' table
	rows, err := h.chConn.Query(ctx, query, args...)
	if err != nil {
		log.Printf("Flow users query error: %v\nQuery: %s\nArgs: %v", err, query, args)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to calculate flow users"})
	}
	defer rows.Close()

	var distinctIDs []string
	for rows.Next() {
		var distinctID string
		if err := rows.Scan(&distinctID); err != nil {
			log.Printf("Flow users scan error: %v", err)
			continue
		}
		distinctIDs = append(distinctIDs, distinctID)
	}

	// Apply basic limit/offset
	start := req.Offset
	if start > len(distinctIDs) {
		start = len(distinctIDs)
	}
	end := start + req.Limit
	if end > len(distinctIDs) {
		end = len(distinctIDs)
	}
	pagedIDs := distinctIDs[start:end]

	if len(pagedIDs) == 0 {
		return c.JSON(fiber.Map{"users": []any{}, "total": len(distinctIDs)})
	}

	personsQuery := `
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
			WHERE project_id = ? AND distinct_id IN (?)
			GROUP BY distinct_id
		) p
		LEFT JOIN person_aliases pa 
			ON p.distinct_id = pa.distinct_id
		GROUP BY p.distinct_id, p.props, p.latest_seen
	`

	personsArgs := []any{projectID, req.Environment, pagedIDs}

	personsMap := make(map[string]map[string]any)
	personsRows, err := h.chConn.Query(ctx, personsQuery, personsArgs...)
	if err == nil {
		for personsRows.Next() {
			var distinctID, propsStr string
			var lastSeen uint64
			var aliases []string
			
			if err := personsRows.Scan(&distinctID, &propsStr, &lastSeen, &aliases); err == nil {
				user := map[string]any{
					"distinct_id": distinctID,
					"aliases":     aliases,
					"created_at":  lastSeen,
				}
				
				var props map[string]any
				if propsStr != "" {
					if err := json.Unmarshal([]byte(propsStr), &props); err == nil {
						user["properties"] = props
						if email, ok := props["$email"].(string); ok {
							user["email"] = email
						} else if email, ok := props["email"].(string); ok {
							user["email"] = email
						}
						if name, ok := props["$name"].(string); ok {
							user["name"] = name
						} else if name, ok := props["name"].(string); ok {
							user["name"] = name
						}
					}
				}
				personsMap[distinctID] = user
			}
		}
		personsRows.Close()
	} else {
		log.Printf("Flow users person fetch error: %v", err)
	}

	var users []map[string]any
	for _, did := range pagedIDs {
		if p, ok := personsMap[did]; ok {
			users = append(users, p)
		} else {
			users = append(users, map[string]any{
				"distinct_id": did,
			})
		}
	}

	return c.JSON(fiber.Map{
		"users": users,
		"total": len(distinctIDs), // Total subset matching this specific flow path
	})
}


type SaveFlowRequest struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Environment string      `json:"environment"`
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

	if req.Environment == "" {
		req.Environment = "live"
	}

	flow := database.SavedFlow{
		ProjectID:   projectID,
		Environment: req.Environment,
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

	query := h.db.Where("project_id = ?", projectID)
	env := c.Query("environment")
	if env != "" {
		query = query.Where("environment = ?", env)
	}

	if err := query.Order("created_at desc").Find(&flows).Error; err != nil {
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

	query := h.db.Where("id = ? AND project_id = ?", flowID, projectID)
	env := c.Query("environment")
	if env != "" {
		query = query.Where("environment = ?", env)
	}

	if err := query.Delete(&database.SavedFlow{}).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete flow"})
	}

	return c.SendStatus(204)
}
