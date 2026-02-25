package api

import (
	"fmt"
	"reflect"
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
