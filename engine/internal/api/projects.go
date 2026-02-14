package api

import (
	"crypto/rand"
	"encoding/hex"

	"log"
	"strconv"
	"time"

	"sankofa/engine/internal/database"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

// ProjectHandler holds dependencies for project routes
type ProjectHandler struct {
	DB *gorm.DB
	CH driver.Conn
}

func NewProjectHandler(db *gorm.DB, ch driver.Conn) *ProjectHandler {
	return &ProjectHandler{DB: db, CH: ch}
}

func (h *ProjectHandler) RegisterRoutes(api fiber.Router, authMiddleware fiber.Handler) {
	projects := api.Group("/projects", authMiddleware)
	projects.Get("/", h.GetProjects) // ?org_id=1
	projects.Post("/", h.CreateProject)
	projects.Put("/:id", h.UpdateProject) // ?org_id=1
	projects.Delete("/:id", h.DeleteProject)

	orgs := api.Group("/organizations", authMiddleware)
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
	err := h.DB.Preload("Organization").Preload("CreatedBy").Raw(`
		SELECT p.* 
		FROM projects p 
		JOIN project_members pm ON p.id = pm.project_id 
		WHERE pm.user_id = ? AND p.organization_id = ?
	`, userID, orgID).Find(&projects).Error

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch projects"})
	}

	// Enrich with Usage Stats from ClickHouse
	type ProjectWithStats struct {
		database.Project
		EventCount       uint64 `json:"event_count"`
		UserProfileCount uint64 `json:"user_profile_count"`
	}

	var enriched []ProjectWithStats

	for _, p := range projects {
		stats := ProjectWithStats{Project: p}

		if h.CH != nil {
			// Count Events
			tenantID := strconv.Itoa(int(p.ID))
			env := p.Environment
			if env == "" {
				env = "live"
			}

			// Event Count
			var eventCount uint64
			if err := h.CH.QueryRow(c.Context(), "SELECT count() FROM events WHERE tenant_id = ? AND environment = ?", tenantID, env).Scan(&eventCount); err == nil {
				stats.EventCount = eventCount
			}

			// User Profile Count
			var userCount uint64
			// Note: persons table also needs environment column or logical separation?
			// Assuming persons are also separated by environment. If not, this might need adjustment.
			// Let's assume 'persons' are environment specific. If the schema doesn't have it, we might need to check.
			// For now, I will add the environment check assuming consistency.
			if err := h.CH.QueryRow(c.Context(), "SELECT uniq(distinct_id) FROM events WHERE tenant_id = ? AND environment = ?", tenantID, env).Scan(&userCount); err == nil {
				stats.UserProfileCount = userCount
			}
		}

		enriched = append(enriched, stats)
	}

	return c.JSON(enriched)
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
	testApiKey, _ := generateTestAPIKey() // Assuming helper or just use random for now

	project := database.Project{
		OrganizationID: req.OrganizationID,
		Name:           req.Name,
		APIKey:         apiKey,
		TestAPIKey:     testApiKey,
		Timezone:       "UTC", // Default
		Region:         "us-east-1",
		CreatedByID:    userID,
		Environment:    "test",
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

	if err := tx.Commit().Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Commit failed"})
	}

	// Reload with associations for frontend
	if err := h.DB.Preload("CreatedBy").Preload("Organization").First(&project, project.ID).Error; err != nil {
		log.Println("Error reloading project:", err)
	}

	return c.JSON(project)
}

// UpdateProject - PUT /v1/projects/:id
func (h *ProjectHandler) UpdateProject(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(uint)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	projectID, err := strconv.Atoi(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid project ID"})
	}

	type Request struct {
		Name        string `json:"name"`
		Timezone    string `json:"timezone"`
		Environment string `json:"environment"`
	}
	var req Request
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request body"})
	}

	// 1. Verify Access (Admin role required to update settings?)
	// For simplicity, checking membership. In real app check Role (Admin/Owner).
	var member database.ProjectMember
	if err := h.DB.Where("project_id = ? AND user_id = ?", projectID, userID).First(&member).Error; err != nil {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied"})
	}
	if member.Role != "Admin" && member.Role != "Owner" { // Assuming Owner exists or Admin is top
		return c.Status(403).JSON(fiber.Map{"error": "Only admins can update project settings"})
	}

	// 2. Update
	var project database.Project
	if err := h.DB.First(&project, projectID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}

	if req.Name != "" {
		project.Name = req.Name
	}
	if req.Timezone != "" {
		project.Timezone = req.Timezone
	}
	if req.Environment != "" {
		project.Environment = req.Environment
	}

	if err := h.DB.Save(&project).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to update project"})
	}

	// Enrich with stats
	type ProjectWithStats struct {
		database.Project
		EventCount       uint64 `json:"event_count"`
		UserProfileCount uint64 `json:"user_profile_count"`
	}
	stats := ProjectWithStats{Project: project}

	if h.CH != nil {
		tenantID := strconv.Itoa(int(project.ID))
		env := project.Environment
		if env == "" {
			env = "live"
		}

		var eventCount uint64
		if err := h.CH.QueryRow(c.Context(), "SELECT count() FROM events WHERE tenant_id = ? AND environment = ?", tenantID, env).Scan(&eventCount); err == nil {
			stats.EventCount = eventCount
		}

		var userCount uint64
		if err := h.CH.QueryRow(c.Context(), "SELECT uniq(distinct_id) FROM events WHERE tenant_id = ? AND environment = ?", tenantID, env).Scan(&userCount); err == nil {
			stats.UserProfileCount = userCount
		}
	}

	return c.JSON(stats)
}

// DeleteProject - DELETE /v1/projects/:id
func (h *ProjectHandler) DeleteProject(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(uint)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	projectID, err := strconv.Atoi(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid project ID"})
	}

	// 1. Verify Access (Admin/Owner role required)
	var member database.ProjectMember
	if err := h.DB.Where("project_id = ? AND user_id = ?", projectID, userID).First(&member).Error; err != nil {
		return c.Status(403).JSON(fiber.Map{"error": "Access denied"})
	}
	if member.Role != "Admin" && member.Role != "Owner" {
		return c.Status(403).JSON(fiber.Map{"error": "Only admins can delete projects"})
	}

	// 2. Delete Project and related data
	tx := h.DB.Begin()

	// Delete Project Members
	if err := tx.Where("project_id = ?", projectID).Delete(&database.ProjectMember{}).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete project members"})
	}

	// Delete Project
	if err := tx.Delete(&database.Project{}, projectID).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete project"})
	}

	tx.Commit()

	return c.JSON(fiber.Map{"status": "ok", "message": "Project deleted successfully"})
}

// Helper to avoid import cycles / duplication. In real app, put in utils.
func parseUint(s string) uint {
	val, _ := strconv.ParseUint(s, 10, 64)
	return uint(val)
}

func generateTestAPIKey() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "sk_test_" + hex.EncodeToString(bytes), nil
}
