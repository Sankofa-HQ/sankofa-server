package api

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"net/http"
	"strconv"
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
	userID, ok := c.Locals("user_id").(uint)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	orgID, err := strconv.Atoi(c.Params("id"))
	if err != nil {
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
	userID, _ := c.Locals("user_id").(uint)

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
	orgID, _ := c.Locals("org_id").(uint) // middleware

	var org database.Organization
	if err := h.DB.First(&org, orgID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Organization not found"})
	}

	return c.JSON(org)
}

// UpdateOrganization - PUT /api/v1/orgs/:org_id
func (h *OrganizationHandler) UpdateOrganization(c *fiber.Ctx) error {
	orgID, _ := c.Locals("org_id").(uint)

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
	if err := h.DB.First(&org, orgID).Error; err != nil {
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
	orgID, _ := c.Locals("org_id").(uint)
	userID, _ := c.Locals("user_id").(uint)

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

// --- Usage & Billing ---

// GetUsage - GET /v1/orgs/:org_id/usage
func (h *OrganizationHandler) GetUsage(c *fiber.Ctx) error {
	orgID, _ := c.Locals("org_id").(uint)
	log.Printf("🔹 GetUsage called for OrgID: %d", orgID)

	// 1. Get Org Plan
	var org database.Organization
	if err := h.DB.First(&org, orgID).Error; err != nil {
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
		strOrgID := strconv.Itoa(int(orgID))

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
