package api

import (
	"strconv"
	"time"

	"sankofa/engine/internal/database"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

// ProjectHandler holds dependencies for project routes
type ProjectHandler struct {
	DB *gorm.DB
}

func NewProjectHandler(db *gorm.DB) *ProjectHandler {
	return &ProjectHandler{DB: db}
}

func (h *ProjectHandler) RegisterRoutes(api fiber.Router) {
	projects := api.Group("/projects")
	projects.Get("/", h.GetProjects) // ?org_id=1
	projects.Post("/", h.CreateProject)

	orgs := api.Group("/organizations")
	orgs.Get("/", h.GetOrganizations)
}

func (h *ProjectHandler) GetOrganizations(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(uint)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	var memberships []database.OrganizationMember
	if err := h.DB.Where("user_id = ?", userID).Find(&memberships).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch memberships"})
	}

	var orgIDs []uint
	for _, m := range memberships {
		orgIDs = append(orgIDs, m.OrganizationID)
	}

	var orgs []database.Organization
	if len(orgIDs) > 0 {
		h.DB.Where("id IN ?", orgIDs).Find(&orgs)
	}

	return c.JSON(orgs)
}

func (h *ProjectHandler) GetProjects(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(uint)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	orgID := c.Query("org_id")
	if orgID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "org_id is required"})
	}

	// Check if user has access to this org
	var membership database.OrganizationMember
	if err := h.DB.Where("organization_id = ? AND user_id = ?", orgID, userID).First(&membership).Error; err != nil {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied to organization"})
	}

	// Fetch projects where user is a member
	// Query: Select P.* from Projects P join ProjectMembers PM on P.ID = PM.ProjectID where PM.UserID = ? AND P.OrganizationID = ?
	var projects []database.Project
	err := h.DB.Raw(`
		SELECT p.* 
		FROM projects p 
		JOIN project_members pm ON p.id = pm.project_id 
		WHERE pm.user_id = ? AND p.organization_id = ?
	`, userID, orgID).Scan(&projects).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch projects"})
	}

	return c.JSON(projects)
}

func (h *ProjectHandler) CreateProject(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(uint)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	type CreateProjectReq struct {
		Name           string `json:"name"`
		OrganizationID uint   `json:"org_id"`
	}
	var req CreateProjectReq
	if err := c.BodyParser(&req); err != nil {
		return c.SendStatus(400)
	}

	// Check Organization Role (must be Owner or Admin equivalent? For now assume Owner/Member is fine)
	var orgMem database.OrganizationMember
	if err := h.DB.Where("organization_id = ? AND user_id = ?", req.OrganizationID, userID).First(&orgMem).Error; err != nil {
		return c.Status(403).JSON(fiber.Map{"error": "Not a member of this organization"})
	}

	apiKey, _ := generateAPIKey()
	project := database.Project{
		OrganizationID: req.OrganizationID,
		Name:           req.Name,
		APIKey:         apiKey,
		Timezone:       "UTC", // Default
		Region:         "us-east-1",
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	tx := h.DB.Begin()
	if err := tx.Create(&project).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to create project"})
	}

	// Add Creator as Admin
	if err := tx.Create(&database.ProjectMember{
		ProjectID: project.ID,
		UserID:    userID, // Already uint from locals
		Role:      "Admin",
		CreatedAt: time.Now(),
	}).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to add member"})
	}

	tx.Commit()
	return c.JSON(project)
}

// Helper to avoid import cycles / duplication. In real app, put in utils.
func parseUint(s string) uint {
	val, _ := strconv.ParseUint(s, 10, 64)
	return uint(val)
}
