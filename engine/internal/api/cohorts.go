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

type CohortAST struct {
	Logic   string         `json:"logic"` // "AND", "OR"
	Filters []CohortFilter `json:"filters"`
}

type CohortFilter struct {
	Type          string      `json:"type"` // "event", "user_property"
	EventName     string      `json:"event_name,omitempty"`
	TimeframeDays int         `json:"timeframe_days,omitempty"`
	PropertyName  string      `json:"property_name,omitempty"`
	Operator      string      `json:"operator"`
	Value         interface{} `json:"value"`
}

// --- SQL BUILDER ---

// BuildCohortSQL translates the JSON AST into a ClickHouse subquery
func BuildCohortSQL(projectID string, ast CohortAST) (string, []interface{}) {
	var subqueries []string
	var args []interface{}

	for _, filter := range ast.Filters {
		if filter.Type == "event" {
			// e.g., "Users who did 'Checkout' >= 3 times in 30 days"
			query := `
				SELECT distinct_id 
				FROM events 
				WHERE project_id = ? 
				AND event_name = ? 
				AND timestamp >= now() - INTERVAL ? DAY 
				GROUP BY distinct_id 
				HAVING count() %s ?`

			// Inject the operator safely
			op := filter.Operator
			if op != ">" && op != "<" && op != ">=" && op != "<=" && op != "=" && op != "!=" {
				op = ">=" // Default safe
			}

			query = fmt.Sprintf(query, op)
			subqueries = append(subqueries, query)

			args = append(args, projectID, filter.EventName, filter.TimeframeDays, filter.Value)

		} else if filter.Type == "user_property" {
			// e.g., "Users where country == 'Ghana'"
			// Note: We use JSONExtractString assuming properties are stored as JSON Maps in ClickHouse
			// Actually, in `persons` table, properties are `Map(String, String)`.
			// So we use simple map access: properties['key']

			query := `
				SELECT distinct_id 
				FROM persons 
				WHERE project_id = ? 
				AND properties[?] %s ?`

			// Adjust operator (e.g., "==" in JSON becomes "=" in SQL)
			sqlOperator := filter.Operator
			if sqlOperator == "==" {
				sqlOperator = "="
			}
			// Whitelist operators
			if sqlOperator != "=" && sqlOperator != "!=" && sqlOperator != "LIKE" && sqlOperator != "ILIKE" {
				sqlOperator = "="
			}

			// Handle "contains"
			if filter.Operator == "contains" {
				query = `SELECT distinct_id FROM persons WHERE project_id = ? AND positionCaseInsensitive(properties[?], ?) > 0`
				subqueries = append(subqueries, query)
				args = append(args, projectID, filter.PropertyName, filter.Value)
				continue
			}

			query = fmt.Sprintf(query, sqlOperator)
			subqueries = append(subqueries, query)

			args = append(args, projectID, filter.PropertyName, filter.Value)
		}
	}

	if len(subqueries) == 0 {
		return "SELECT ''", nil // Return empty set if no filters
	}

	// Join the subqueries based on the root Logic
	joiner := " INTERSECT "
	if ast.Logic == "OR" {
		joiner = " UNION DISTINCT "
	}

	// Wrap the whole thing in parentheses so it can be used as a subquery
	finalSQL := fmt.Sprintf("(%s)", strings.Join(subqueries, joiner))

	return finalSQL, args
}

// --- HANDLERS ---

func (h *CohortsHandler) CreateCohort(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(uint)
	var req struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Type        string          `json:"type"`
		ProjectID   uint            `json:"project_id"`
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
				_ = batch.Append(uint64(req.ProjectID), uint64(cohort.ID), mid, int8(1))
			}
			if err := batch.Send(); err != nil {
				log.Println("❌ Failed to write static members:", err)
			}
		}()
	}

	return c.Status(201).JSON(cohort)
}

func (h *CohortsHandler) ListCohorts(c *fiber.Ctx) error {
	projectID := c.Query("project_id")
	if projectID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing project_id"})
	}

	var cohorts []database.Cohort
	if err := h.DB.Preload("CreatedBy").Where("project_id = ?", projectID).Order("created_at DESC").Find(&cohorts).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch cohorts"})
	}

	// Build enriched response
	type CohortResponse struct {
		ID          uint   `json:"id"`
		Name        string `json:"name"`
		Description string `json:"description"`
		Type        string `json:"type"`
		Rules       string `json:"rules"`
		CreatedAt   string `json:"created_at"`
		UpdatedAt   string `json:"updated_at"`
		CreatedByID uint   `json:"created_by_id"`
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
	if err := h.DB.First(&cohort, id).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Cohort not found"})
	}
	return c.JSON(cohort)
}

func (h *CohortsHandler) DeleteCohort(c *fiber.Ctx) error {
	id := c.Params("id")
	if err := h.DB.Delete(&database.Cohort{}, id).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete cohort"})
	}
	return c.SendStatus(204)
}

// AddMembers (Static)
func (h *CohortsHandler) AddMembers(c *fiber.Ctx) error {
	idStr := c.Params("id")
	cohortID, _ := strconv.Atoi(idStr)
	var req struct {
		DistinctIDs []string `json:"distinct_ids"`
		ProjectID   uint     `json:"project_id"` // Should verify match with cohort
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
			_ = batch.Append(uint64(req.ProjectID), uint64(cohortID), mid, int8(1))
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
	cohortID, _ := strconv.Atoi(idStr)
	var req struct {
		DistinctIDs []string `json:"distinct_ids"`
		ProjectID   uint     `json:"project_id"`
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
			_ = batch.Append(uint64(req.ProjectID), uint64(cohortID), mid, int8(-1))
		}
		if err := batch.Send(); err != nil {
			log.Println("❌ Failed to remove members:", err)
		}
	}()

	return c.SendStatus(200)
}
