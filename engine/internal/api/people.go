package api

import (
	"context"
	"log"
	"strconv"
	"time"

	"sankofa/engine/internal/database"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type PeopleHandler struct {
	DB *gorm.DB
	CH driver.Conn
}

func NewPeopleHandler(db *gorm.DB, ch driver.Conn) *PeopleHandler {
	return &PeopleHandler{DB: db, CH: ch}
}

type PersonProfile struct {
	DistinctID string            `json:"distinct_id"`
	Properties map[string]string `json:"properties"`
	LastSeen   time.Time         `json:"last_seen"`
	Aliases    []string          `json:"aliases"`
}

// ListPeople - GET /api/v1/people
// Params: project_id (opt), limit, offset, search
func (h *PeopleHandler) ListPeople(c *fiber.Ctx) error {
	// 1. Auth & Context
	userID, ok := c.Locals("user_id").(uint)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	// Resolve Project
	var project database.Project
	queryProjectID := c.Query("project_id", "")

	if queryProjectID != "" {
		if err := h.DB.First(&project, queryProjectID).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
		}
	} else {
		// Fallback to user's current project
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

	limit := c.QueryInt("limit", 50)
	if limit > 200 {
		limit = 200
	}
	offset := c.QueryInt("offset", 0)
	search := c.Query("search", "")
	environment := c.Query("environment", "live")

	projID := strconv.Itoa(int(project.ID))

	// 2. Build ClickHouse Query
	baseQuery := `
		SELECT 
			p.distinct_id,
			p.properties,
			p.last_seen,
			groupArray(pa.alias_id) as aliases
		FROM persons p
		LEFT JOIN person_aliases pa 
			ON p.distinct_id = pa.distinct_id 
			AND p.project_id = pa.project_id 
			AND p.environment = pa.environment
		WHERE p.project_id = ? AND p.environment = ?
	`
	args := []interface{}{projID, environment}

	if search != "" {
		baseQuery += " AND (positionCaseInsensitive(p.distinct_id, ?) > 0)"
		args = append(args, search)
	}

	baseQuery += `
		GROUP BY p.distinct_id, p.properties, p.last_seen
		ORDER BY p.last_seen DESC 
		LIMIT ? OFFSET ?
	`
	args = append(args, limit, offset)

	rows, err := h.CH.Query(context.Background(), baseQuery, args...)
	if err != nil {
		log.Println("Details: ClickHouse ListPeople Error:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch people"})
	}
	defer rows.Close()

	var people []PersonProfile
	for rows.Next() {
		var p PersonProfile
		if err := rows.Scan(&p.DistinctID, &p.Properties, &p.LastSeen, &p.Aliases); err != nil {
			continue
		}
		// Filter empty aliases
		var cleanAliases []string
		for _, a := range p.Aliases {
			if a != "" {
				cleanAliases = append(cleanAliases, a)
			}
		}
		p.Aliases = cleanAliases
		if p.Aliases == nil {
			p.Aliases = []string{}
		}
		people = append(people, p)
	}

	// Return empty list instead of null
	if people == nil {
		people = []PersonProfile{}
	}

	return c.JSON(fiber.Map{
		"data":   people,
		"limit":  limit,
		"offset": offset,
	})
}

// GetPerson - GET /api/v1/people/:id
func (h *PeopleHandler) GetPerson(c *fiber.Ctx) error {
	distinctID := c.Params("id")
	if distinctID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Missing distinct_id"})
	}

	// 1. Auth & Context
	userID, ok := c.Locals("user_id").(uint)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	// Resolve Project
	var project database.Project
	queryProjectID := c.Query("project_id", "")

	if queryProjectID != "" {
		if err := h.DB.First(&project, queryProjectID).Error; err != nil {
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

	environment := c.Query("environment", "live")
	projID := strconv.Itoa(int(project.ID))

	// 2. Fetch Person from ClickHouse
	query := `
		SELECT 
			p.distinct_id,
			p.properties,
			p.last_seen,
			groupArray(pa.alias_id) as aliases
		FROM persons p
		LEFT JOIN person_aliases pa 
			ON p.distinct_id = pa.distinct_id 
			AND p.project_id = pa.project_id 
			AND p.environment = pa.environment
		WHERE p.project_id = ? AND p.environment = ? AND p.distinct_id = ?
		GROUP BY p.distinct_id, p.properties, p.last_seen
		LIMIT 1
	`

	var p PersonProfile
	if err := h.CH.QueryRow(context.Background(), query, projID, environment, distinctID).Scan(
		&p.DistinctID, &p.Properties, &p.LastSeen, &p.Aliases,
	); err != nil {
		if err.Error() == "sql: no rows in result set" { // Check standard sql error or driver specific
			return c.Status(404).JSON(fiber.Map{"error": "Person not found"})
		}
		log.Println("Details: ClickHouse GetPerson Error:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch person"})
	}

	// Clean aliases
	var cleanAliases []string
	for _, a := range p.Aliases {
		if a != "" {
			cleanAliases = append(cleanAliases, a)
		}
	}
	p.Aliases = cleanAliases
	if p.Aliases == nil {
		p.Aliases = []string{}
	}

	return c.JSON(fiber.Map{"data": p})
}
