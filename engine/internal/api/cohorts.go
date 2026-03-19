package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"sankofa/engine/internal/database"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type CohortsHandler struct {
	DB *gorm.DB
	CH driver.Conn
}

func NewCohortsHandler(db *gorm.DB, ch driver.Conn) *CohortsHandler {
	return &CohortsHandler{DB: db, CH: ch}
}

// --- AST STRUCTS ---

type CohortASTGroup struct {
	ID      string   `json:"id"`
	Logic   string   `json:"logic"` // "AND", "OR"
	Filters []Filter `json:"filters"`
}

type CohortAST struct {
	Logic   string           `json:"logic"` // "AND", "OR"
	Groups  []CohortASTGroup `json:"groups"`
	Filters []Filter         `json:"filters"` // Fallback or flat array
}

// --- SQL BUILDER ---

// buildFiltersSQL builds the sql queries for a specific group of filters
func buildFiltersSQL(db *gorm.DB, projectID string, env string, filters []Filter, logic string) (string, []interface{}) {
	var subqueries []string
	var args []interface{}

	for _, f := range filters {
		// Base Subquery for a filter is a selection of distinct_ids that meet the conditions
		subQuery := "SELECT distinct_id FROM persons WHERE project_id = ? AND environment = ?"
		subArgs := []interface{}{projectID, env}

		if f.FilterType == "cohort" {
			cohortIDStr := fmt.Sprintf("%v", f.Value)
			if f.CohortID != nil {
				cohortIDStr = fmt.Sprintf("%v", f.CohortID)
			}

			var nestedCohort database.Cohort
			if err := db.First(&nestedCohort, "id = ?", cohortIDStr).Error; err != nil {
				log.Println("Nested cohort not found:", cohortIDStr)
				continue
			}

			var nestedCohortSQL string
			var nestedCohortArgs []interface{}

			if nestedCohort.Type == "static" {
				nestedCohortSQL = "SELECT distinct_id FROM cohort_static_members WHERE project_id = ? AND cohort_id = ? GROUP BY distinct_id HAVING sum(sign) > 0"
				nestedCohortArgs = []interface{}{projectID, nestedCohort.ID}
			} else {
				var nestedAst CohortAST
				if err := json.Unmarshal(nestedCohort.Rules, &nestedAst); err == nil {
					nestedCohortSQL, nestedCohortArgs = BuildCohortSQL(db, projectID, env, nestedAst)
				} else {
					log.Println("Failed to parse nested cohort rules:", err)
				}
			}

			if nestedCohortSQL != "" {
				if f.BehaviorType == "did_not" {
					subQuery += fmt.Sprintf(" AND distinct_id NOT IN (%s)", nestedCohortSQL)
				} else {
					subQuery += fmt.Sprintf(" AND distinct_id IN (%s)", nestedCohortSQL)
				}
				subArgs = append(subArgs, nestedCohortArgs...)
				subqueries = append(subqueries, subQuery)
				args = append(args, subArgs...)
			}
			continue
		}

		if f.FilterType == "user_property" {
			if f.Property == "" {
				continue
			}
			valStr := fmt.Sprintf("%v", f.Value)

			// To check property without requiring full base table struct, use map schema
			subQuery = `SELECT distinct_id FROM (SELECT distinct_id, argMax(properties, last_seen) AS props FROM persons WHERE project_id = ? AND environment = ? GROUP BY distinct_id) p WHERE 1=1`

			switch f.Operator {
			case "is":
				subQuery += fmt.Sprintf(" AND p.props['%s'] = ?", sanitizeKey(f.Property))
				subArgs = append(subArgs, valStr)
			case "is_not":
				subQuery += fmt.Sprintf(" AND p.props['%s'] != ?", sanitizeKey(f.Property))
				subArgs = append(subArgs, valStr)
			case "contains":
				subQuery += fmt.Sprintf(" AND positionCaseInsensitive(p.props['%s'], ?) > 0", sanitizeKey(f.Property))
				subArgs = append(subArgs, valStr)
			case "does_not_contain":
				subQuery += fmt.Sprintf(" AND positionCaseInsensitive(p.props['%s'], ?) = 0", sanitizeKey(f.Property))
				subArgs = append(subArgs, valStr)
			case "is_set":
				subQuery += fmt.Sprintf(" AND mapContains(p.props, '%s')", sanitizeKey(f.Property))
			case "is_not_set":
				subQuery += fmt.Sprintf(" AND NOT mapContains(p.props, '%s')", sanitizeKey(f.Property))
			case "gt":
				subQuery += fmt.Sprintf(" AND toFloat64OrZero(p.props['%s']) > toFloat64OrZero(?)", sanitizeKey(f.Property))
				subArgs = append(subArgs, valStr)
			case "lt":
				subQuery += fmt.Sprintf(" AND toFloat64OrZero(p.props['%s']) < toFloat64OrZero(?)", sanitizeKey(f.Property))
				subArgs = append(subArgs, valStr)
			case "gte":
				subQuery += fmt.Sprintf(" AND toFloat64OrZero(p.props['%s']) >= toFloat64OrZero(?)", sanitizeKey(f.Property))
				subArgs = append(subArgs, valStr)
			case "lte":
				subQuery += fmt.Sprintf(" AND toFloat64OrZero(p.props['%s']) <= toFloat64OrZero(?)", sanitizeKey(f.Property))
				subArgs = append(subArgs, valStr)
			}

			subqueries = append(subqueries, subQuery)
			args = append(args, subArgs...)

		} else if f.FilterType == "event" {
			expandedNames := ExpandVirtualEventNames(db, projectID, env, []string{f.EventName})
			placeholders := make([]string, len(expandedNames))
			eventArgs := []interface{}{projectID, env}
			for i, name := range expandedNames {
				placeholders[i] = "?"
				eventArgs = append(eventArgs, name)
			}
			eventSub := fmt.Sprintf(`SELECT distinct_id FROM events WHERE project_id = ? AND environment = ? AND event_name IN (%s)`, strings.Join(placeholders, ","))

			// Time range
			timeSql, timeArgs := ParseTimeRangeSql("timestamp", f.TimeRange)
			if timeSql == "" && f.TimeRange != "all" {
				timeSql, timeArgs = ParseTimeRangeSql("timestamp", "30d") // Default
			}
			if timeSql != "" {
				eventSub += " AND " + timeSql
				eventArgs = append(eventArgs, timeArgs...)
			}

			// Where clause filters on event properties
			if len(f.WhereFilters) > 0 {
				wcSQL, wcArgs := buildWhereClauseSQL(db, f.WhereFilters, projectID, env)
				if wcSQL != "" {
					eventSub += " AND " + wcSQL
					eventArgs = append(eventArgs, wcArgs...)
				}
			}

			// Metric Operator
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
			} else if v, ok := f.Value.(int); ok {
				valFloat = float64(v)
			}

			havingClause := fmt.Sprintf("count() %s ?", metricOp)
			eventSub += " GROUP BY distinct_id HAVING " + havingClause
			eventArgs = append(eventArgs, valFloat)

			// Resolve "did" vs "did_not"
			if f.BehaviorType == "did" {
				subqueries = append(subqueries, eventSub)
				args = append(args, eventArgs...)
			} else {
				// DID NOT is all persons minus the subquery
				inverseQuery := fmt.Sprintf(`SELECT distinct_id FROM persons WHERE project_id = ? AND environment = ? AND distinct_id NOT IN (%s)`, eventSub)
				invArgs := []interface{}{projectID, env}
				invArgs = append(invArgs, eventArgs...)
				subqueries = append(subqueries, inverseQuery)
				args = append(args, invArgs...)
			}
		}
	}

	if len(subqueries) == 0 {
		return "(SELECT '' WHERE 1=0)", nil // Return empty set if no filters
	}

	joiner := " INTERSECT "
	if logic == "OR" {
		joiner = " UNION DISTINCT "
	}

	return fmt.Sprintf("(%s)", strings.Join(subqueries, joiner)), args
}

// BuildCohortSQL translates the JSON AST into a ClickHouse subquery
func BuildCohortSQL(db *gorm.DB, projectID string, env string, ast CohortAST) (string, []interface{}) {
	if env == "" {
		env = "live"
	}
	var groupQueries []string
	var finalArgs []interface{}

	if len(ast.Groups) > 0 {
		for _, group := range ast.Groups {
			groupLogic := group.Logic
			if groupLogic == "" {
				groupLogic = "AND"
			}
			gSQL, gArgs := buildFiltersSQL(db, projectID, env, group.Filters, groupLogic)
			if gSQL != "(SELECT '' WHERE 1=0)" {
				groupQueries = append(groupQueries, gSQL)
				finalArgs = append(finalArgs, gArgs...)
			}
		}
	} else if len(ast.Filters) > 0 {
		gSQL, gArgs := buildFiltersSQL(db, projectID, env, ast.Filters, ast.Logic)
		if gSQL != "(SELECT '' WHERE 1=0)" {
			groupQueries = append(groupQueries, gSQL)
			finalArgs = append(finalArgs, gArgs...)
		}
	}

	if len(groupQueries) == 0 {
		return "(SELECT distinct_id FROM persons WHERE project_id = ? AND environment = ?)", []interface{}{projectID, env}
	}

	joiner := " INTERSECT "
	rootLogic := ast.Logic
	if rootLogic == "OR" {
		joiner = " UNION DISTINCT "
	}

	return fmt.Sprintf("(%s)", strings.Join(groupQueries, joiner)), finalArgs
}

// --- HANDLERS ---

func (h *CohortsHandler) CreateCohort(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok || userID == "" {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}
	var req struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Type        string          `json:"type"`
		ProjectID   string          `json:"project_id"`
		Environment string          `json:"environment"`
		Rules       json.RawMessage `json:"rules"`   // Dynamic
		Members     []string        `json:"members"` // Static initial members?
	}

	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}

	// Create SQLite Entry
	cohort := database.Cohort{
		ProjectID:   req.ProjectID,
		Name:        req.Name,
		Description: req.Description,
		Type:        req.Type,
		Environment: req.Environment,
		Rules:       req.Rules,
		CreatedByID: userID,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	if err := h.DB.Create(&cohort).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to create cohort"})
	}

	// If Static, insert members to ClickHouse
	if req.Type == "static" && len(req.Members) > 0 {
		go func() {
			ctx := context.Background()
			batch, err := h.CH.PrepareBatch(ctx, "INSERT INTO cohort_static_members (project_id, cohort_id, distinct_id, sign)")
			if err != nil {
				log.Println("❌ Failed to prepare static cohort batch:", err)
				return
			}
			for _, mid := range req.Members {
				_ = batch.Append(req.ProjectID, cohort.ID, mid, int8(1))
			}
			if err := batch.Send(); err != nil {
				log.Println("❌ Failed to write static members:", err)
			}
		}()
	}

	// Reload to get CreatedBy User
	h.DB.Preload("CreatedBy").First(&cohort, "id = ?", cohort.ID)

	creatorName := "Team"
	if cohort.CreatedBy != nil {
		creatorName = cohort.CreatedBy.FullName
		if creatorName == "" {
			creatorName = cohort.CreatedBy.Email
		}
	}

	// Build enriched response
	res := fiber.Map{
		"id":            cohort.ID,
		"name":          cohort.Name,
		"description":   cohort.Description,
		"type":          cohort.Type,
		"environment":   cohort.Environment,
		"rules":         string(cohort.Rules),
		"created_at":    cohort.CreatedAt.Format("Jan 02, 2006"),
		"updated_at":    cohort.UpdatedAt.Format("Jan 02, 2006"),
		"created_by_id": cohort.CreatedByID,
		"creator_name":  creatorName,
	}

	return c.Status(201).JSON(res)
}

func (h *CohortsHandler) ListCohorts(c *fiber.Ctx) error {
	projectID := c.Query("project_id")
	if projectID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing project_id"})
	}

	var cohorts []database.Cohort
	query := h.DB.Preload("CreatedBy").Where("project_id = ?", projectID)

	env := c.Query("environment")
	if env != "" {
		query = query.Where("environment = ?", env)
	}

	if err := query.Order("created_at DESC").Find(&cohorts).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch cohorts"})
	}

	// Build enriched response
	type CohortResponse struct {
		ID          string `json:"id"`
		Name        string `json:"name"`
		Description string `json:"description"`
		Type        string `json:"type"`
		Environment string `json:"environment"`
		Rules       string `json:"rules"`
		CreatedAt   string `json:"created_at"`
		UpdatedAt   string `json:"updated_at"`
		CreatedByID string `json:"created_by_id"`
		CreatorName string `json:"creator_name"`
	}

	var result []CohortResponse
	for _, c := range cohorts {
		creatorName := "Team"
		if c.CreatedBy != nil {
			creatorName = c.CreatedBy.FullName
			if creatorName == "" {
				creatorName = c.CreatedBy.Email
			}
		}
		result = append(result, CohortResponse{
			ID:          c.ID,
			Name:        c.Name,
			Description: c.Description,
			Type:        c.Type,
			Environment: c.Environment,
			Rules:       string(c.Rules),
			CreatedAt:   c.CreatedAt.Format("Jan 02, 2006"),
			UpdatedAt:   c.UpdatedAt.Format("Jan 02, 2006"),
			CreatedByID: c.CreatedByID,
			CreatorName: creatorName,
		})
	}

	if result == nil {
		result = []CohortResponse{}
	}

	return c.JSON(result)
}

func (h *CohortsHandler) GetCohort(c *fiber.Ctx) error {
	id := c.Params("id")
	var cohort database.Cohort
	if err := h.DB.First(&cohort, "id = ?", id).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Cohort not found"})
	}
	return c.JSON(cohort)
}

func (h *CohortsHandler) DeleteCohort(c *fiber.Ctx) error {
	id := c.Params("id")
	if err := h.DB.Delete(&database.Cohort{}, "id = ?", id).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete cohort"})
	}
	return c.SendStatus(204)
}

func (h *CohortsHandler) UpdateCohort(c *fiber.Ctx) error {
	id := c.Params("id")

	var cohort database.Cohort
	if err := h.DB.First(&cohort, "id = ?", id).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Cohort not found"})
	}

	var req struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Rules       json.RawMessage `json:"rules"`
	}

	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}

	cohort.Name = req.Name
	cohort.Description = req.Description
	if len(req.Rules) > 0 {
		cohort.Rules = req.Rules
	}
	cohort.UpdatedAt = time.Now()

	if err := h.DB.Save(&cohort).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to update cohort"})
	}

	// Reload to get CreatedBy User
	h.DB.Preload("CreatedBy").First(&cohort, "id = ?", cohort.ID)

	creatorName := "Team"
	if cohort.CreatedBy != nil {
		creatorName = cohort.CreatedBy.FullName
		if creatorName == "" {
			creatorName = cohort.CreatedBy.Email
		}
	}

	// Build enriched response
	res := fiber.Map{
		"id":            cohort.ID,
		"name":          cohort.Name,
		"description":   cohort.Description,
		"type":          cohort.Type,
		"environment":   cohort.Environment,
		"rules":         string(cohort.Rules),
		"created_at":    cohort.CreatedAt.Format("Jan 02, 2006"),
		"updated_at":    cohort.UpdatedAt.Format("Jan 02, 2006"),
		"created_by_id": cohort.CreatedByID,
		"creator_name":  creatorName,
	}

	return c.JSON(res)
}

// AddMembers (Static)
func (h *CohortsHandler) AddMembers(c *fiber.Ctx) error {
	idStr := c.Params("id")
	cohortID := idStr
	var req struct {
		DistinctIDs []string `json:"distinct_ids"`
		ProjectID   string   `json:"project_id"` // Should verify match with cohort
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}

	go func() {
		ctx := context.Background()
		batch, err := h.CH.PrepareBatch(ctx, "INSERT INTO cohort_static_members (project_id, cohort_id, distinct_id, sign)")
		if err != nil {
			log.Println("❌ Failed to prepare static batch:", err)
			return
		}
		for _, mid := range req.DistinctIDs {
			_ = batch.Append(req.ProjectID, cohortID, mid, int8(1))
		}
		if err := batch.Send(); err != nil {
			log.Println("❌ Failed to add members:", err)
		}
	}()

	return c.SendStatus(200)
}

// RemoveMembers (Static)
func (h *CohortsHandler) RemoveMembers(c *fiber.Ctx) error {
	idStr := c.Params("id")
	cohortID := idStr
	var req struct {
		DistinctIDs []string `json:"distinct_ids"`
		ProjectID   string   `json:"project_id"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}

	go func() {
		ctx := context.Background()
		batch, err := h.CH.PrepareBatch(ctx, "INSERT INTO cohort_static_members (project_id, cohort_id, distinct_id, sign)")
		if err != nil {
			log.Println("❌ Failed to prepare static batch:", err)
			return
		}
		for _, mid := range req.DistinctIDs {
			_ = batch.Append(req.ProjectID, cohortID, mid, int8(-1))
		}
		if err := batch.Send(); err != nil {
			log.Println("❌ Failed to remove members:", err)
		}
	}()

	return c.SendStatus(200)
}
