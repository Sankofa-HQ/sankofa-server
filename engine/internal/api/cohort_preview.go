package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"sankofa/engine/internal/database"

	"github.com/gofiber/fiber/v2"
)

// PreviewCohort - POST /api/v1/cohorts/preview
// Accepts the same filter rules as the people list and returns just the count
func (h *CohortsHandler) PreviewCohort(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(uint)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	var req struct {
		ProjectID   uint            `json:"project_id"`
		Environment string          `json:"environment"`
		Filters     json.RawMessage `json:"filters"`
		Rules       json.RawMessage `json:"rules"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}

	// Resolve project
	var project database.Project
	if req.ProjectID > 0 {
		if err := h.DB.First(&project, req.ProjectID).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
		}
	} else {
		var user database.User
		if err := h.DB.First(&user, userID).Error; err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to get user"})
		}
		if user.CurrentProjectID == nil {
			return c.Status(400).JSON(fiber.Map{"error": "No project selected"})
		}
		if err := h.DB.First(&project, *user.CurrentProjectID).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
		}
	}

	env := req.Environment
	if env == "" {
		env = "live"
	}
	projID := strconv.Itoa(int(project.ID))

	// Parse filters (same format as /api/v1/people)
	// Parse AST instead of flattened rules
	var ast CohortAST
	var payload json.RawMessage

	// Favor req.Rules specifically over legacy req.Filters
	if len(req.Rules) > 0 {
		payload = req.Rules
	} else if len(req.Filters) > 0 {
		payload = req.Filters
	}

	if len(payload) > 0 {
		json.Unmarshal(payload, &ast)

		// If ast.Logic is empty, it means we might have received the legacy flattened `filters: [...]` array.
		if ast.Logic == "" && len(ast.Filters) == 0 && len(ast.Groups) == 0 {
			var flatFilters []Filter
			if err := json.Unmarshal(payload, &flatFilters); err == nil && len(flatFilters) > 0 {
				ast.Filters = flatFilters
				ast.Logic = "AND"
			}
		}
	}

	// Build the subquery using BuildCohortSQL
	cohortSQL, cohortArgs := BuildCohortSQL(projID, env, ast)
	log.Printf("DEBUG COHORT PREVIEW SQL: %s", cohortSQL)
	log.Printf("DEBUG COHORT PREVIEW ARGS: %v", cohortArgs)
	if cohortSQL == "(SELECT '' WHERE 1=0)" {
		// Just count all if empty, or return 0? The old code returned all for empty filters.
		var total uint64
		err := h.CH.QueryRow(context.Background(),
			"SELECT count(DISTINCT distinct_id) FROM persons WHERE project_id = ? AND environment = ?",
			projID, env,
		).Scan(&total)
		if err != nil {
			log.Println("Preview count error:", err)
			return c.JSON(fiber.Map{"count": 0})
		}
		return c.JSON(fiber.Map{"count": total})
	}

	countQuery := fmt.Sprintf(`SELECT count(DISTINCT distinct_id) FROM (%s)`, cohortSQL)

	var total uint64
	if err := h.CH.QueryRow(context.Background(), countQuery, cohortArgs...).Scan(&total); err != nil {
		log.Println("Preview count query error:", err)
		return c.JSON(fiber.Map{"count": 0})
	}

	return c.JSON(fiber.Map{"count": total})
}
