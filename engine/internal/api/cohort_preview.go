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
	var filters []Filter
	if len(req.Filters) > 0 {
		json.Unmarshal(req.Filters, &filters)
	}

	// If no filters, count all users
	if len(filters) == 0 {
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

	// Build the same query as ListPeople but just count
	countQuery := `
		SELECT count(DISTINCT p.distinct_id) 
		FROM (
			SELECT 
				distinct_id,
				argMax(properties, last_seen) AS props,
				max(last_seen) AS latest_seen
			FROM persons
			WHERE project_id = ? AND environment = ?
			GROUP BY distinct_id
		) p
		LEFT JOIN person_aliases pa 
			ON p.distinct_id = pa.distinct_id
		WHERE 1=1
	`
	args := []interface{}{projID, env}

	// Apply filters (simplified version of people.go logic)
	for _, f := range filters {
		if f.FilterType == "user_property" {
			if f.Property == "" {
				continue
			}
			valStr := fmt.Sprintf("%v", f.Value)
			switch f.Operator {
			case "is":
				countQuery += fmt.Sprintf(" AND p.props['%s'] = ?", sanitizeKey(f.Property))
				args = append(args, valStr)
			case "is_not":
				countQuery += fmt.Sprintf(" AND p.props['%s'] != ?", sanitizeKey(f.Property))
				args = append(args, valStr)
			case "contains":
				countQuery += fmt.Sprintf(" AND positionCaseInsensitive(p.props['%s'], ?) > 0", sanitizeKey(f.Property))
				args = append(args, valStr)
			case "does_not_contain":
				countQuery += fmt.Sprintf(" AND positionCaseInsensitive(p.props['%s'], ?) = 0", sanitizeKey(f.Property))
				args = append(args, valStr)
			case "is_set":
				countQuery += fmt.Sprintf(" AND mapContains(p.props, '%s')", sanitizeKey(f.Property))
			case "is_not_set":
				countQuery += fmt.Sprintf(" AND NOT mapContains(p.props, '%s')", sanitizeKey(f.Property))
			case "gt":
				countQuery += fmt.Sprintf(" AND toFloat64OrZero(p.props['%s']) > toFloat64OrZero(?)", sanitizeKey(f.Property))
				args = append(args, valStr)
			case "lt":
				countQuery += fmt.Sprintf(" AND toFloat64OrZero(p.props['%s']) < toFloat64OrZero(?)", sanitizeKey(f.Property))
				args = append(args, valStr)
			case "gte":
				countQuery += fmt.Sprintf(" AND toFloat64OrZero(p.props['%s']) >= toFloat64OrZero(?)", sanitizeKey(f.Property))
				args = append(args, valStr)
			case "lte":
				countQuery += fmt.Sprintf(" AND toFloat64OrZero(p.props['%s']) <= toFloat64OrZero(?)", sanitizeKey(f.Property))
				args = append(args, valStr)
			}
		} else if f.FilterType == "event" {
			subQuery := `SELECT distinct_id FROM events WHERE project_id = ? AND environment = ? AND event_name = ?`
			subArgs := []interface{}{projID, env, f.EventName}

			// Time range
			var timeSeconds int64 = 30 * 86400
			if f.TimeRange == "all" {
				timeSeconds = 0
			} else if len(f.TimeRange) > 0 {
				val, _ := strconv.Atoi(f.TimeRange[:len(f.TimeRange)-1])
				unit := f.TimeRange[len(f.TimeRange)-1]
				switch unit {
				case 'h':
					timeSeconds = int64(val) * 3600
				case 'd':
					timeSeconds = int64(val) * 86400
				case 'm':
					timeSeconds = int64(val) * 86400 * 30
				}
			}
			if timeSeconds > 0 {
				subQuery += ` AND timestamp >= now() - INTERVAL ? SECOND`
				subArgs = append(subArgs, timeSeconds)
			}

			// Metric
			metricOp := ">="
			switch f.Operator {
			case "gt":
				metricOp = ">"
			case "lt":
				metricOp = "<"
			case "gte":
				metricOp = ">="
			case "lte":
				metricOp = "<="
			case "eq":
				metricOp = "="
			case "neq":
				metricOp = "!="
			}

			valFloat := 0.0
			if v, ok := f.Value.(float64); ok {
				valFloat = v
			} else if v, ok := f.Value.(string); ok {
				valFloat, _ = strconv.ParseFloat(v, 64)
			}

			havingClause := fmt.Sprintf("count() %s ?", metricOp)
			subArgs = append(subArgs, valFloat)
			subQuery += " GROUP BY distinct_id HAVING " + havingClause

			if f.BehaviorType == "did" {
				countQuery += fmt.Sprintf(" AND p.distinct_id IN (%s)", subQuery)
				args = append(args, subArgs...)
			} else {
				countQuery += fmt.Sprintf(" AND p.distinct_id NOT IN (%s)", subQuery)
				args = append(args, subArgs...)
			}
		}
	}

	var total uint64
	if err := h.CH.QueryRow(context.Background(), countQuery, args...).Scan(&total); err != nil {
		log.Println("Preview count query error:", err)
		return c.JSON(fiber.Map{"count": 0})
	}

	return c.JSON(fiber.Map{"count": total})
}
