package api

import (
	"fmt"
	"sankofa/engine/internal/database"
	"sankofa/engine/internal/models"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type FunnelsHandler struct {
	db     *gorm.DB
	chConn driver.Conn
}

func NewFunnelsHandler(db *gorm.DB, chConn driver.Conn) *FunnelsHandler {
	return &FunnelsHandler{db: db, chConn: chConn}
}

func (h *FunnelsHandler) RegisterRoutes(router fiber.Router, authMiddleware fiber.Handler) {
	// POST /api/v1/projects/:project_id/funnels
	funnels := router.Group("/projects/:project_id/funnels")
	funnels.Use(authMiddleware)
	funnels.Post("/", h.CalculateFunnel)
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

	var query string
	var args []any

	if models.RequiresSequenceMatch(req) {
		query, args = database.BuildSequenceMatchQuery(req)
	} else {
		// Give a default funnel window if not standardly specified
		defaultWindow := 604800 // 7 days in seconds, can be passed or extracted
		query, args = database.BuildWindowFunnelQuery(req, defaultWindow)
	}

	ctx := c.Context()
	rows, err := h.chConn.Query(ctx, query, args...)
	if err != nil {
		fmt.Println("Funnel Query Gen:", query)
		fmt.Println("Funnel Query Args:", args)
		fmt.Println("Funnel Query Error:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to calculate funnel"})
	}
	defer rows.Close()

	var results []fiber.Map
	cols := rows.Columns()

	for rows.Next() {
		values := make([]interface{}, len(cols))
		pointers := make([]interface{}, len(cols))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			fmt.Println("Funnel Scan Error:", err)
			return c.Status(500).JSON(fiber.Map{"error": "Failed to parse funnel results"})
		}

		rowMap := fiber.Map{}
		for i, colName := range cols {
			rowMap[colName] = values[i]
		}
		results = append(results, rowMap)
	}

	return c.JSON(results)
}
