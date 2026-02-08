package api

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strconv"
	"time"

	"sankofa/engine/internal/database"

	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type OrganizationHandler struct {
	DB *gorm.DB
}

func NewOrganizationHandler(db *gorm.DB) *OrganizationHandler {
	return &OrganizationHandler{DB: db}
}

func (h *OrganizationHandler) RegisterRoutes(router fiber.Router) {
	// These routes are expected to be under /v1/orgs/:org_id
	// and protected by RequireAuth + RequireOrgAccess middleware
	router.Post("/projects", h.CreateProject)
	router.Post("/invite", h.InviteMember)
	router.Get("/members", h.GetMembers)
	router.Delete("/members/:user_id", h.RemoveMember)
}

// CreateProject - /v1/orgs/:org_id/projects
func (h *OrganizationHandler) CreateProject(c *fiber.Ctx) error {
	userID, _ := c.Locals("user_id").(uint)
	orgID, _ := c.Locals("org_id").(uint) // Added by middleware

	type Request struct {
		Name     string `json:"name"`
		Region   string `json:"region"`
		Timezone string `json:"timezone"`
	}
	var req Request
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	// Defaults
	if req.Region == "" {
		req.Region = "us-east-1"
	}
	if req.Timezone == "" {
		req.Timezone = "UTC"
	}

	apiKey, _ := generateOrgAPIKey()

	project := database.Project{
		OrganizationID: orgID,
		Name:           req.Name,
		APIKey:         apiKey,
		Region:         req.Region,
		Timezone:       req.Timezone,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	tx := h.DB.Begin()

	// 1. Create Project
	if err := tx.Create(&project).Error; err != nil {
		tx.Rollback()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to create project"})
	}

	// 2. Add Creator as Admin
	if err := tx.Create(&database.ProjectMember{
		ProjectID: project.ID,
		UserID:    userID,
		Role:      "Admin",
		CreatedAt: time.Now(),
	}).Error; err != nil {
		tx.Rollback()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to add project member"})
	}

	tx.Commit()

	return c.JSON(project)
}

// InviteMember - /v1/orgs/:org_id/invite
func (h *OrganizationHandler) InviteMember(c *fiber.Ctx) error {
	orgID, _ := c.Locals("org_id").(uint)

	type InviteRequest struct {
		Email       string `json:"email"`
		OrgRole     string `json:"org_role"` // Member, Admin
		ProjectIDs  []uint `json:"project_ids"`
		TeamIDs     []uint `json:"team_ids"`
		ProjectRole string `json:"project_role"` // Viewer, Editor, Admin
	}
	var req InviteRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request"})
	}

	// 1. Check if user exists
	var user database.User
	if err := h.DB.Where("email = ?", req.Email).First(&user).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": "User not found. Invite flow for non-users not implemented."})
		}
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Database error"})
	}

	tx := h.DB.Begin()

	// 2. Add/Update Organization Member
	var orgMember database.OrganizationMember
	err := tx.Where("organization_id = ? AND user_id = ?", orgID, user.ID).First(&orgMember).Error
	if err == gorm.ErrRecordNotFound {
		// Create new
		orgMember = database.OrganizationMember{
			OrganizationID: orgID,
			UserID:         user.ID,
			Role:           req.OrgRole,
			CreatedAt:      time.Now(),
		}
		if err := tx.Create(&orgMember).Error; err != nil {
			tx.Rollback()
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to add org member"})
		}
	} else if err == nil {
		// Update role if exists
		if err := tx.Model(&orgMember).Update("role", req.OrgRole).Error; err != nil {
			tx.Rollback()
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to update org member"})
		}
	} else {
		tx.Rollback()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Database error checking org member"})
	}

	// 3. Loop through Project IDs and add/update Project Member
	for _, pid := range req.ProjectIDs {
		// Verify project belongs to org
		var count int64
		tx.Model(&database.Project{}).Where("id = ? AND organization_id = ?", pid, orgID).Count(&count)
		if count == 0 {
			continue // Skip invalid projects
		}

		var projMember database.ProjectMember
		err := tx.Where("project_id = ? AND user_id = ?", pid, user.ID).First(&projMember).Error
		if err == gorm.ErrRecordNotFound {
			if err := tx.Create(&database.ProjectMember{
				ProjectID: pid,
				UserID:    user.ID,
				Role:      req.ProjectRole,
				CreatedAt: time.Now(),
			}).Error; err != nil {
				tx.Rollback()
				return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to add project member"})
			}
		} else {
			if err := tx.Model(&projMember).Update("role", req.ProjectRole).Error; err != nil {
				tx.Rollback()
				return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to update project member"})
			}
		}
	}

	// 4. Loop through Team IDs and add/update Team Member
	for _, tid := range req.TeamIDs {
		// Verify team belongs to org
		var count int64
		tx.Model(&database.Team{}).Where("id = ? AND organization_id = ?", tid, orgID).Count(&count)
		if count == 0 {
			continue
		}

		var teamMember database.TeamMember
		err := tx.Where("team_id = ? AND user_id = ?", tid, user.ID).First(&teamMember).Error
		if err == gorm.ErrRecordNotFound {
			if err := tx.Create(&database.TeamMember{
				TeamID:    tid,
				UserID:    user.ID,
				Role:      "Member", // Default to Member for now
				CreatedAt: time.Now(),
			}).Error; err != nil {
				tx.Rollback()
				return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to add team member"})
			}
		}
	}

	tx.Commit()

	return c.JSON(fiber.Map{"status": "invited", "user": user.Email})
}

// GetMembers - GET /v1/orgs/:org_id/members
func (h *OrganizationHandler) GetMembers(c *fiber.Ctx) error {
	orgID, _ := c.Locals("org_id").(uint)

	type MemberResponse struct {
		UserID   uint      `json:"user_id"`
		FullName string    `json:"full_name"`
		Email    string    `json:"email"`
		Role     string    `json:"role"`
		JoinedAt time.Time `json:"joined_at"`
	}

	// Join OrganizationMember with User
	var members []MemberResponse
	err := h.DB.Table("organization_members").
		Select("users.id as user_id, users.full_name, users.email, organization_members.role, organization_members.created_at as joined_at").
		Joins("JOIN users ON users.id = organization_members.user_id").
		Where("organization_members.organization_id = ?", orgID).
		Scan(&members).Error

	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to fetch members"})
	}

	return c.JSON(members)
}

// RemoveMember - DELETE /v1/orgs/:org_id/members/:user_id
func (h *OrganizationHandler) RemoveMember(c *fiber.Ctx) error {
	orgID, _ := c.Locals("org_id").(uint)
	targetUserIDStr := c.Params("user_id")
	targetUserID, err := strconv.Atoi(targetUserIDStr)
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid user ID"})
	}

	// Prevent removing yourself (optional, but good practice usually, OR handled by frontend)
	// For now, let's allow it but maybe warn? Or just allow.

	tx := h.DB.Begin()

	// 1. Remove from Organization
	if err := tx.Where("organization_id = ? AND user_id = ?", orgID, targetUserID).Delete(&database.OrganizationMember{}).Error; err != nil {
		tx.Rollback()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to remove member"})
	}

	// 2. Remove from all Projects in this Organization
	// We need to find all projects for this org first.
	// OR we can do a DELETE with JOIN/Subquery logic, but Gorm might be tricky.
	// Simpler: Find project IDs first.
	var projectIDs []uint
	tx.Model(&database.Project{}).Where("organization_id = ?", orgID).Pluck("id", &projectIDs)

	if len(projectIDs) > 0 {
		if err := tx.Where("project_id IN ? AND user_id = ?", projectIDs, targetUserID).Delete(&database.ProjectMember{}).Error; err != nil {
			tx.Rollback()
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to remove project memberships"})
		}
	}

	tx.Commit()

	return c.SendStatus(http.StatusOK)
}

func generateOrgAPIKey() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "sk_live_" + hex.EncodeToString(bytes), nil
}

// --- Team Handlers ---

// CreateTeam - POST /v1/orgs/:org_id/teams
func (h *OrganizationHandler) CreateTeam(c *fiber.Ctx) error {
	orgID, _ := c.Locals("org_id").(uint)

	type Request struct {
		Name string `json:"name"`
	}
	var req Request
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	team := database.Team{
		OrganizationID: orgID,
		Name:           req.Name,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	if err := h.DB.Create(&team).Error; err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to create team"})
	}

	return c.JSON(team)
}

// GetTeams - GET /v1/orgs/:org_id/teams
func (h *OrganizationHandler) GetTeams(c *fiber.Ctx) error {
	orgID, _ := c.Locals("org_id").(uint)

	var teams []database.Team
	if err := h.DB.Where("organization_id = ?", orgID).Find(&teams).Error; err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to fetch teams"})
	}

	return c.JSON(teams)
}

// AddTeamMember - POST /v1/orgs/:org_id/teams/:team_id/members
func (h *OrganizationHandler) AddTeamMember(c *fiber.Ctx) error {
	teamID, err := strconv.Atoi(c.Params("team_id"))
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid team ID"})
	}

	type Request struct {
		UserID uint   `json:"user_id"`
		Role   string `json:"role"`
	}
	var req Request
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	// Verify team belongs to org (middleware handles org_id check, but we should check team ownership)
	// For simplicity assumes team_id is valid for now or we trust the caller has access to the org.

	member := database.TeamMember{
		TeamID:    uint(teamID),
		UserID:    req.UserID,
		Role:      req.Role, // Member, Lead
		CreatedAt: time.Now(),
	}

	if err := h.DB.Create(&member).Error; err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to add member to team"})
	}

	return c.JSON(member)
}

// AssignTeamProject - POST /v1/orgs/:org_id/teams/:team_id/projects
func (h *OrganizationHandler) AssignTeamProject(c *fiber.Ctx) error {
	teamID, err := strconv.Atoi(c.Params("team_id"))
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid team ID"})
	}

	type Request struct {
		ProjectID uint   `json:"project_id"`
		Role      string `json:"role"`
	}
	var req Request
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	tp := database.TeamProject{
		TeamID:    uint(teamID),
		ProjectID: req.ProjectID,
		Role:      req.Role,
		CreatedAt: time.Now(),
	}

	if err := h.DB.Create(&tp).Error; err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to assign project to team"})
	}

	return c.JSON(tp)
}
