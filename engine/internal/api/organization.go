package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"sankofa/engine/internal/database"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type OrganizationHandler struct {
	DB *gorm.DB
	CH driver.Conn
}

func NewOrganizationHandler(db *gorm.DB, ch driver.Conn) *OrganizationHandler {
	return &OrganizationHandler{DB: db, CH: ch}
}

func (h *OrganizationHandler) RegisterRoutes(router fiber.Router) {
	// These routes are expected to be under /v1/orgs/:org_id
	// and protected by RequireAuth + RequireOrgAccess middleware
	router.Get("/", h.GetOrganization)
	router.Put("/", h.UpdateOrganization)
	router.Delete("/", h.DeleteOrganization)
	router.Post("/projects", h.CreateProject)
	router.Post("/invite", h.InviteMember)
	router.Get("/members", h.GetMembers)
	router.Post("/members", h.GetMembers)
	router.Delete("/members/:user_id", h.RemoveMember)
	router.Post("/:id/leave", h.LeaveOrganization)
}

// ... existing code ...

func (h *OrganizationHandler) LeaveOrganization(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	orgID := c.Params("id")
	if orgID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid organization ID"})
	}

	// Calculate how many owners are left
	var ownerCount int64
	if err := h.DB.Model(&database.OrganizationMember{}).
		Where("organization_id = ? AND role = 'Owner'", orgID).
		Count(&ownerCount).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to check ownership"})
	}

	// Check if this user is the last owner
	var membership database.OrganizationMember
	if err := h.DB.Where("organization_id = ? AND user_id = ?", orgID, userID).First(&membership).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Membership not found"})
	}

	if membership.Role == "Owner" && ownerCount == 1 {
		return c.Status(403).JSON(fiber.Map{"error": "Cannot leave organization as the last owner. Promote someone else or delete the organization."})
	}

	// Remove membership
	if err := h.DB.Delete(&membership).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to leave organization"})
	}

	return c.JSON(fiber.Map{"status": "ok", "message": "Left organization successfully"})
}

// CreateOrganization - POST /api/orgs
func (h *OrganizationHandler) CreateOrganization(c *fiber.Ctx) error {
	userID, _ := c.Locals("user_id").(string)

	type Request struct {
		Name        string `json:"name"`
		CompanySize string `json:"company_size"`
		Industry    string `json:"industry"`
		Region      string `json:"region"`
	}
	var req Request
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	if req.Name == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Organization name is required"})
	}

	// Defaults
	if req.Region == "" {
		req.Region = "us-east-1"
	}

	tx := h.DB.Begin()

	// 1. Create Organization
	org := database.Organization{
		Name:        req.Name,
		Slug:        generateSlug(req.Name),
		Plan:        "Free",
		CompanySize: req.CompanySize,
		Industry:    req.Industry,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	if err := tx.Create(&org).Error; err != nil {
		tx.Rollback()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to create organization"})
	}

	// 2. Link User as Owner
	if err := tx.Create(&database.OrganizationMember{
		OrganizationID: org.ID,
		UserID:         userID,
		Role:           "Owner",
		CreatedAt:      time.Now(),
	}).Error; err != nil {
		tx.Rollback()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to link user to org"})
	}

	// 3. Create Default Project
	liveKey, _ := generateOrgAPIKey("live")
	testKey, _ := generateOrgAPIKey("test")

	project := database.Project{
		OrganizationID: org.ID,
		Name:           req.Name, // Use Org Name as default Project Name
		APIKey:         liveKey,
		TestAPIKey:     testKey,
		Timezone:       "UTC",
		Region:         req.Region,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	if err := tx.Create(&project).Error; err != nil {
		tx.Rollback()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to create default project"})
	}

	// 4. Link User as Project Admin
	if err := tx.Create(&database.ProjectMember{
		ProjectID: project.ID,
		UserID:    userID,
		Role:      "Admin",
		CreatedAt: time.Now(),
	}).Error; err != nil {
		tx.Rollback()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to link user to project"})
	}

	tx.Commit()

	return c.JSON(org)
}

// GetOrganization - GET /api/v1/orgs/:org_id
func (h *OrganizationHandler) GetOrganization(c *fiber.Ctx) error {
	orgID, _ := c.Locals("org_id").(string) // middleware

	var org database.Organization
	if err := h.DB.First(&org, "id = ?", orgID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Organization not found"})
	}

	return c.JSON(org)
}

// UpdateOrganization - PUT /api/v1/orgs/:org_id
func (h *OrganizationHandler) UpdateOrganization(c *fiber.Ctx) error {
	orgID, _ := c.Locals("org_id").(string)

	type Request struct {
		Name         string `json:"name"`
		BillingEmail string `json:"billing_email"`
		CompanySize  string `json:"company_size"`
		Industry     string `json:"industry"`
	}
	var req Request
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request"})
	}

	var org database.Organization
	if err := h.DB.First(&org, "id = ?", orgID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Organization not found"})
	}

	if req.Name != "" {
		org.Name = req.Name
	}
	if req.BillingEmail != "" {
		org.BillingEmail = req.BillingEmail
	}
	if req.CompanySize != "" {
		org.CompanySize = req.CompanySize
	}
	if req.Industry != "" {
		org.Industry = req.Industry
	}

	if err := h.DB.Save(&org).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to update organization"})
	}

	return c.JSON(org)
}

// DeleteOrganization - DELETE /api/v1/orgs/:org_id
func (h *OrganizationHandler) DeleteOrganization(c *fiber.Ctx) error {
	orgID, _ := c.Locals("org_id").(string)
	userID, _ := c.Locals("user_id").(string)

	// Verify user is Owner
	var member database.OrganizationMember
	if err := h.DB.Where("organization_id = ? AND user_id = ?", orgID, userID).First(&member).Error; err != nil {
		return c.Status(403).JSON(fiber.Map{"error": "Not a member of this organization"})
	}
	if member.Role != "Owner" {
		return c.Status(403).JSON(fiber.Map{"error": "Only owners can delete an organization"})
	}

	tx := h.DB.Begin()

	// Delete Projects, Teams, Members, Org itself
	// Note: GORM with proper foreign keys might cascade, but manual cleanup is safer if not configured.

	// 1. Delete Projects (and their members/data if cascaded)
	if err := tx.Where("organization_id = ?", orgID).Delete(&database.Project{}).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete projects"})
	}

	// 2. Delete Teams
	if err := tx.Where("organization_id = ?", orgID).Delete(&database.Team{}).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete teams"})
	}

	// 3. Delete Members
	if err := tx.Where("organization_id = ?", orgID).Delete(&database.OrganizationMember{}).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete members"})
	}

	// 4. Delete Organization
	if err := tx.Delete(&database.Organization{}, orgID).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete organization"})
	}

	tx.Commit()

	return c.JSON(fiber.Map{"status": "ok", "message": "Organization deleted"})
}

// Helper to avoid dependency on internal/api packet private func in auth.go if they are different files in same package.
// They are in same package 'api', so 'generateSlug' from auth.go SHOULD be visible if it is in the same package.
// Let's verify if auth.go is in package api. Yes it is.

// CreateProject - /v1/orgs/:org_id/projects
func (h *OrganizationHandler) CreateProject(c *fiber.Ctx) error {
	userID, _ := c.Locals("user_id").(string)
	orgID, _ := c.Locals("org_id").(string) // Added by middleware

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

	liveKey, _ := generateOrgAPIKey("live")
	testKey, _ := generateOrgAPIKey("test")

	project := database.Project{
		OrganizationID: orgID,
		Name:           req.Name,
		APIKey:         liveKey,
		TestAPIKey:     testKey,
		Region:         req.Region,
		Timezone:       req.Timezone,
		CreatedByID:    userID,
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
	orgID, _ := c.Locals("org_id").(string)

	type InviteRequest struct {
		Email       string   `json:"email"`
		OrgRole     string   `json:"org_role"` // Member, Admin
		ProjectIDs  []string `json:"project_ids"`
		TeamIDs     []string `json:"team_ids"`
		ProjectRole string   `json:"project_role"` // Viewer, Editor, Admin
	}
	var req InviteRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request"})
	}

	req.Email = strings.ToLower(strings.TrimSpace(req.Email))

	// 1. Check if user exists and is already in the org
	var user database.User
	if err := h.DB.Where("LOWER(email) = ?", req.Email).First(&user).Error; err == nil {
		var count int64
		h.DB.Model(&database.OrganizationMember{}).Where("organization_id = ? AND user_id = ?", orgID, user.ID).Count(&count)
		if count > 0 {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "User is already a member of this organization"})
		}
	}

	// 2. Check if an active invite already exists
	var existingInvite database.OrganizationInvite
	err := h.DB.Where("organization_id = ? AND LOWER(email) = ? AND accepted = ? AND expires_at > ?", orgID, req.Email, false, time.Now()).First(&existingInvite).Error
	if err == nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "An active invite has already been sent to this email address"})
	}

	// Convert ProjectIDs and TeamIDs to JSON strings
	projectIDsJSON, _ := json.Marshal(req.ProjectIDs)
	teamIDsJSON, _ := json.Marshal(req.TeamIDs)

	invite := database.OrganizationInvite{
		OrganizationID: orgID,
		Email:          req.Email,
		Role:           req.OrgRole,
		ProjectIDs:     string(projectIDsJSON),
		TeamIDs:        string(teamIDsJSON),
		ProjectRole:    req.ProjectRole,
		ExpiresAt:      time.Now().AddDate(0, 0, 7), // 7 days expiration
	}

	if err := h.DB.Create(&invite).Error; err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to create invite"})
	}

	// TODO: Integrate actual email service here
	// Mock email log
	inviteLink := fmt.Sprintf("%s/invite?token=%s", "http://localhost:3000", invite.Token)
	log.Printf("📧 MOCK EMAIL: Sending invite to %s. Link: %s\n", req.Email, inviteLink)

	return c.JSON(fiber.Map{
		"status": "invite_sent",
		"email":  req.Email,
	})
}

// VerifyInvite - GET /api/v1/orgs/invite/verify?token=...
// Used by the registration page to extract and lock the email address
func (h *OrganizationHandler) VerifyInvite(c *fiber.Ctx) error {
	token := c.Query("token")
	if token == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Token is required", "valid": false})
	}

	var invite database.OrganizationInvite
	if err := h.DB.Where("token = ?", token).First(&invite).Error; err != nil {
		return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": "Invite not found or invalid", "valid": false})
	}

	if invite.Accepted {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invite already accepted", "valid": false})
	}

	if time.Now().After(invite.ExpiresAt) {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invite has expired", "valid": false})
	}

	return c.JSON(fiber.Map{
		"valid": true,
		"email": invite.Email,
	})
}

// AcceptInvite - POST /api/v1/orgs/invite/accept
func (h *OrganizationHandler) AcceptInvite(c *fiber.Ctx) error {
	userID, _ := c.Locals("user_id").(string)

	type Request struct {
		Token string `json:"token"`
	}
	var req Request
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request"})
	}

	var invite database.OrganizationInvite
	if err := h.DB.Where("token = ?", req.Token).First(&invite).Error; err != nil {
		return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": "Invite not found or invalid"})
	}

	if invite.Accepted {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invite already accepted"})
	}

	if time.Now().After(invite.ExpiresAt) {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invite has expired"})
	}

	// Verify user exists
	var user database.User
	if err := h.DB.First(&user, "id = ?", userID).Error; err != nil {
		return c.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "User not found"})
	}

	// 🚨 CRITICAL FIX: Ensure the logged-in user's email matches the invite!
	if user.Email != invite.Email {
		return c.Status(http.StatusForbidden).JSON(fiber.Map{"error": "This invite was sent to a different email address. Please log in with the correct account or sign up."})
	}

	// Unmarshal JSON arrays
	var projectIDs []string
	var teamIDs []string
	json.Unmarshal([]byte(invite.ProjectIDs), &projectIDs)
	json.Unmarshal([]byte(invite.TeamIDs), &teamIDs)

	tx := h.DB.Begin()

	// 1. Add/Update Organization Member
	var orgMember database.OrganizationMember
	err := tx.Where("organization_id = ? AND user_id = ?", invite.OrganizationID, user.ID).First(&orgMember).Error
	if err == gorm.ErrRecordNotFound {
		orgMember = database.OrganizationMember{
			OrganizationID: invite.OrganizationID,
			UserID:         user.ID,
			Role:           invite.Role,
			CreatedAt:      time.Now(),
		}
		if err := tx.Create(&orgMember).Error; err != nil {
			tx.Rollback()
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to join organization"})
		}
	} else if err == nil {
		if err := tx.Model(&orgMember).Update("role", invite.Role).Error; err != nil {
			tx.Rollback()
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to update org role"})
		}
	}

	// 2. Loop through Project IDs and add/update Project Member
	for _, pid := range projectIDs {
		var projMember database.ProjectMember
		err := tx.Where("project_id = ? AND user_id = ?", pid, user.ID).First(&projMember).Error
		if err == gorm.ErrRecordNotFound {
			if err := tx.Create(&database.ProjectMember{
				ProjectID: pid,
				UserID:    user.ID,
				Role:      invite.ProjectRole,
				CreatedAt: time.Now(),
			}).Error; err != nil {
				tx.Rollback()
				return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to join project"})
			}
		} else {
			if err := tx.Model(&projMember).Update("role", invite.ProjectRole).Error; err != nil {
				tx.Rollback()
				return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to update project role"})
			}
		}
	}

	// 3. Loop through Team IDs and add/update Team Member
	for _, tid := range teamIDs {
		var teamMember database.TeamMember
		err := tx.Where("team_id = ? AND user_id = ?", tid, user.ID).First(&teamMember).Error
		if err == gorm.ErrRecordNotFound {
			if err := tx.Create(&database.TeamMember{
				TeamID:    tid,
				UserID:    user.ID,
				Role:      "Member",
				CreatedAt: time.Now(),
			}).Error; err != nil {
				tx.Rollback()
				return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to join team"})
			}
		}
	}

	// 4. Update user's current project context
	if len(projectIDs) > 0 {
		if err := tx.Model(&user).Update("current_project_id", projectIDs[0]).Error; err != nil {
			tx.Rollback()
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to update current project"})
		}
	}

	// 5. Mark invite as accepted
	if err := tx.Model(&invite).Update("accepted", true).Error; err != nil {
		tx.Rollback()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to mark invite accepted"})
	}

	tx.Commit()

	return c.JSON(fiber.Map{"success": true, "organization": invite.Organization})
}

// GetMembers - GET /v1/orgs/:org_id/members
func (h *OrganizationHandler) GetMembers(c *fiber.Ctx) error {
	orgID, _ := c.Locals("org_id").(string)

	type MemberResponse struct {
		UserID   string    `json:"user_id"`
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
	orgID, _ := c.Locals("org_id").(string)
	targetUserID := c.Params("user_id")
	if targetUserID == "" {
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
	var projectIDs []string
	tx.Model(&database.Project{}).Where("organization_id = ?", orgID).Pluck("id", &projectIDs)

	if len(projectIDs) > 0 {
		if err := tx.Where("project_id IN ? AND user_id = ?", projectIDs, targetUserID).Delete(&database.ProjectMember{}).Error; err != nil {
			tx.Rollback()
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to remove project memberships"})
		}
	}

	// 3. Remove from all Teams in this Organization
	var teamIDs []string
	tx.Model(&database.Team{}).Where("organization_id = ?", orgID).Pluck("id", &teamIDs)

	if len(teamIDs) > 0 {
		if err := tx.Where("team_id IN ? AND user_id = ?", teamIDs, targetUserID).Delete(&database.TeamMember{}).Error; err != nil {
			tx.Rollback()
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to remove team memberships"})
		}
	}

	tx.Commit()

	return c.SendStatus(http.StatusOK)
}

func generateOrgAPIKey(env string) (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	prefix := "sk_live_"
	if env == "test" {
		prefix = "sk_test_"
	}
	return prefix + hex.EncodeToString(bytes), nil
}

// --- Team Handlers ---

// CreateTeam - POST /v1/orgs/:org_id/teams
func (h *OrganizationHandler) CreateTeam(c *fiber.Ctx) error {
	orgID, _ := c.Locals("org_id").(string)

	type Request struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	var req Request
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	team := database.Team{
		OrganizationID: orgID,
		Name:           req.Name,
		Description:    req.Description,
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
	orgID, _ := c.Locals("org_id").(string)

	var teams []database.Team
	if err := h.DB.Where("organization_id = ?", orgID).Find(&teams).Error; err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to fetch teams"})
	}

	return c.JSON(teams)
}

// GetTeamMembers - GET /v1/orgs/:org_id/teams/:team_id/members
func (h *OrganizationHandler) GetTeamMembers(c *fiber.Ctx) error {
	teamID := c.Params("team_id")
	if teamID == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid team ID"})
	}

	// Fetch team members, joined with User info for display
	var members []struct {
		ID        uint      `json:"id"`
		UserID    string    `json:"user_id"`
		TeamID    string    `json:"team_id"`
		Role      string    `json:"role"`
		CreatedAt time.Time `json:"created_at"`
		FullName  string    `json:"full_name"`
		Email     string    `json:"email"`
	}

	err := h.DB.Table("team_members").
		Select("team_members.*, users.full_name, users.email").
		Joins("left join users on users.id = team_members.user_id").
		Where("team_members.team_id = ?", teamID).
		Scan(&members).Error

	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to fetch team members"})
	}

	return c.JSON(members)
}

// UpdateTeam - PUT /v1/orgs/:org_id/teams/:team_id
func (h *OrganizationHandler) UpdateTeam(c *fiber.Ctx) error {
	teamID := c.Params("team_id")
	orgID, _ := c.Locals("org_id").(string)

	if teamID == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid team ID"})
	}

	type Request struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	var req Request
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	var team database.Team
	if err := h.DB.Where("id = ? AND organization_id = ?", teamID, orgID).First(&team).Error; err != nil {
		return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": "Team not found"})
	}

	team.Name = req.Name
	team.Description = req.Description
	team.UpdatedAt = time.Now()

	if err := h.DB.Save(&team).Error; err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to update team"})
	}

	return c.JSON(team)
}

// DeleteTeam - DELETE /v1/orgs/:org_id/teams/:team_id
func (h *OrganizationHandler) DeleteTeam(c *fiber.Ctx) error {
	teamID := c.Params("team_id")
	orgID, _ := c.Locals("org_id").(string)

	if teamID == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid team ID"})
	}

	// Verify the team belongs to the organization
	var team database.Team
	if err := h.DB.Where("id = ? AND organization_id = ?", teamID, orgID).First(&team).Error; err != nil {
		return c.Status(http.StatusNotFound).JSON(fiber.Map{"error": "Team not found"})
	}

	tx := h.DB.Begin()

	// Delete team members first
	if err := tx.Where("team_id = ?", teamID).Delete(&database.TeamMember{}).Error; err != nil {
		tx.Rollback()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to delete team members"})
	}

	// Delete team
	if err := tx.Delete(&team).Error; err != nil {
		tx.Rollback()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to delete team"})
	}

	tx.Commit()

	return c.SendStatus(http.StatusNoContent)
}

// AddTeamMember - POST /v1/orgs/:org_id/teams/:team_id/members
func (h *OrganizationHandler) AddTeamMember(c *fiber.Ctx) error {
	teamID := c.Params("team_id")
	if teamID == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid team ID"})
	}

	type Request struct {
		UserID string `json:"user_id"`
		Role   string `json:"role"`
	}
	var req Request
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	// Check if the user is already a member
	var count int64
	h.DB.Model(&database.TeamMember{}).Where("team_id = ? AND user_id = ?", teamID, req.UserID).Count(&count)
	if count > 0 {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "User is already a member of this team"})
	}

	member := database.TeamMember{
		TeamID:    teamID,
		UserID:    req.UserID,
		Role:      req.Role, // Member, Lead
		CreatedAt: time.Now(),
	}

	if err := h.DB.Create(&member).Error; err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") || strings.Contains(err.Error(), "Duplicate entry") {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "User is already a member of this team"})
		}
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to add member to team"})
	}

	return c.JSON(member)
}

// RemoveTeamMember - DELETE /v1/orgs/:org_id/teams/:team_id/members/:user_id
func (h *OrganizationHandler) RemoveTeamMember(c *fiber.Ctx) error {
	teamID := c.Params("team_id")
	userID := c.Params("user_id")

	if teamID == "" || userID == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid team ID or user ID"})
	}

	if err := h.DB.Where("team_id = ? AND user_id = ?", teamID, userID).Delete(&database.TeamMember{}).Error; err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to remove member from team"})
	}

	return c.SendStatus(http.StatusNoContent)
}

// UpdateTeamProjects - PUT /v1/orgs/:org_id/teams/:team_id/projects
func (h *OrganizationHandler) UpdateTeamProjects(c *fiber.Ctx) error {
	teamID := c.Params("team_id")
	if teamID == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid team ID"})
	}

	type Request struct {
		ProjectIDs []string `json:"project_ids"`
	}
	var req Request
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	tx := h.DB.Begin()

	// 1. Remove existing projects for this team
	if err := tx.Where("team_id = ?", teamID).Delete(&database.TeamProject{}).Error; err != nil {
		tx.Rollback()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to clear team projects"})
	}

	// 2. Insert new projects
	for _, pid := range req.ProjectIDs {
		tp := database.TeamProject{
			TeamID:    teamID,
			ProjectID: pid,
			Role:      "Viewer", // Teams get Viewer by default for now
			CreatedAt: time.Now(),
		}
		if err := tx.Create(&tp).Error; err != nil {
			tx.Rollback()
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to assign project to team"})
		}
	}

	tx.Commit()
	return c.JSON(fiber.Map{"status": "ok", "message": "Team projects updated"})
}

// GetTeamProjects - GET /v1/orgs/:org_id/teams/:team_id/projects
func (h *OrganizationHandler) GetTeamProjects(c *fiber.Ctx) error {
	teamID := c.Params("team_id")
	if teamID == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid team ID"})
	}

	var teamProjects []database.TeamProject
	if err := h.DB.Where("team_id = ?", teamID).Find(&teamProjects).Error; err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to fetch team projects"})
	}

	return c.JSON(teamProjects)
}

// GetUserAccess - GET /v1/orgs/:org_id/members/:user_id/access
func (h *OrganizationHandler) GetUserAccess(c *fiber.Ctx) error {
	userID := c.Params("user_id")
	orgID, _ := c.Locals("org_id").(string)

	if userID == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid user ID"})
	}

	// 1. Get explicit projects
	var projectMembers []database.ProjectMember
	// Fetch ONLY projects within this org
	h.DB.Joins("JOIN projects ON projects.id = project_members.project_id").
		Where("project_members.user_id = ? AND projects.organization_id = ?", userID, orgID).
		Find(&projectMembers)

	var projectIDs []string
	for _, pm := range projectMembers {
		projectIDs = append(projectIDs, pm.ProjectID)
	}

	// 2. Get teams
	var teamMembers []database.TeamMember
	h.DB.Joins("JOIN teams ON teams.id = team_members.team_id").
		Where("team_members.user_id = ? AND teams.organization_id = ?", userID, orgID).
		Find(&teamMembers)

	var teamIDs []string
	for _, tm := range teamMembers {
		teamIDs = append(teamIDs, tm.TeamID)
	}

	return c.JSON(fiber.Map{
		"project_ids": projectIDs,
		"team_ids":    teamIDs,
	})
}

// UpdateUserAccess - PUT /v1/orgs/:org_id/members/:user_id/access
func (h *OrganizationHandler) UpdateUserAccess(c *fiber.Ctx) error {
	userID := c.Params("user_id")
	orgID, _ := c.Locals("org_id").(string)

	if userID == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid user ID"})
	}

	type Request struct {
		ProjectIDs  []string `json:"project_ids"`
		TeamIDs     []string `json:"team_ids"`
		ProjectRole string   `json:"project_role"`
	}
	var req Request
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request body"})
	}

	if req.ProjectRole == "" {
		req.ProjectRole = "Viewer" // Fallback
	}

	tx := h.DB.Begin()

	// 1. Clear explicitly assigned projects for this user in this org
	// This is slightly tricky since project_members doesn't hold org_id.
	// We delete project_members where project_id IS IN (projects of this org)
	if err := tx.Exec(`
		DELETE FROM project_members 
		WHERE user_id = ? AND project_id IN (SELECT id FROM projects WHERE organization_id = ?)
	`, userID, orgID).Error; err != nil {
		tx.Rollback()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to clear existing projects"})
	}

	// 2. Clear explicit team memberships for this user in this org
	if err := tx.Exec(`
		DELETE FROM team_members 
		WHERE user_id = ? AND team_id IN (SELECT id FROM teams WHERE organization_id = ?)
	`, userID, orgID).Error; err != nil {
		tx.Rollback()
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to clear existing teams"})
	}

	// 3. Insert new Project Memberships
	for _, pid := range req.ProjectIDs {
		if err := tx.Create(&database.ProjectMember{
			ProjectID: pid,
			UserID:    userID,
			Role:      req.ProjectRole,
			CreatedAt: time.Now(),
		}).Error; err != nil {
			tx.Rollback()
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to map new project"})
		}
	}

	// 4. Insert new Team Memberships
	for _, tid := range req.TeamIDs {
		if err := tx.Create(&database.TeamMember{
			TeamID:    tid,
			UserID:    userID,
			Role:      "Member",
			CreatedAt: time.Now(),
		}).Error; err != nil {
			tx.Rollback()
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to map new team"})
		}
	}

	tx.Commit()

	return c.JSON(fiber.Map{"status": "ok", "message": "User access updated successfully"})
}

// --- Usage & Billing ---

// GetUsage - GET /v1/orgs/:org_id/usage
func (h *OrganizationHandler) GetUsage(c *fiber.Ctx) error {
	orgID, _ := c.Locals("org_id").(string)
	log.Printf("🔹 GetUsage called for OrgID: %s", orgID)

	// 1. Get Org Plan
	var org database.Organization
	if err := h.DB.First(&org, "id = ?", orgID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Organization not found"})
	}

	// 2. Fetch Plan Limits from DB
	var plan database.Plan
	if err := h.DB.First(&plan, "name = ?", org.Plan).Error; err != nil {
		log.Printf("⚠️ Plan '%s' not found, falling back to Free", org.Plan)
		// Fallback if plan not found - or maybe error?
		// Let's fallback to Free just in case
		h.DB.First(&plan, "name = 'Free'")
	}
	log.Printf("🔹 Plan Limits: %+v", plan)

	limits := map[string]int64{
		"events":   plan.EventLimit,
		"profiles": plan.ProfileLimit,
		"replays":  plan.ReplayLimit,
	}

	// 3. Get Usage from ClickHouse
	var liveEvents, testEvents uint64
	var liveProfiles, testProfiles uint64

	if h.CH != nil {
		strOrgID := orgID

		// Events - Live
		if err := h.CH.QueryRow(c.Context(), "SELECT count() FROM events WHERE organization_id = ? AND environment = 'live'", strOrgID).Scan(&liveEvents); err != nil {
			log.Println("ClickHouse Usage Query Error (Live Events):", err)
		}
		// Events - Test
		if err := h.CH.QueryRow(c.Context(), "SELECT count() FROM events WHERE organization_id = ? AND environment = 'test'", strOrgID).Scan(&testEvents); err != nil {
			log.Println("ClickHouse Usage Query Error (Test Events):", err)
		}

		// Profiles - Live
		if err := h.CH.QueryRow(c.Context(), "SELECT uniq(distinct_id) FROM persons WHERE organization_id = ? AND environment = 'live'", strOrgID).Scan(&liveProfiles); err != nil {
			log.Println("ClickHouse Usage Query Error (Live Profiles):", err)
		}
		// Profiles - Test
		if err := h.CH.QueryRow(c.Context(), "SELECT uniq(distinct_id) FROM persons WHERE organization_id = ? AND environment = 'test'", strOrgID).Scan(&testProfiles); err != nil {
			log.Println("ClickHouse Usage Query Error (Test Profiles):", err)
		}
	}

	// Response
	return c.JSON(fiber.Map{
		"plan": fiber.Map{
			"name":   org.Plan,
			"limits": limits,
		},
		"usage": fiber.Map{
			"events_live":    liveEvents,
			"events_test":    testEvents,
			"events_total":   liveEvents + testEvents,
			"profiles_live":  liveProfiles,
			"profiles_test":  testProfiles,
			"profiles_total": liveProfiles + testProfiles,
			"replays":        0, // Mock for now
		},
		"period": fiber.Map{
			"start": time.Now().AddDate(0, 0, -15), // Mock cycle
			"end":   time.Now().AddDate(0, 0, 15),
		},
	})
}
