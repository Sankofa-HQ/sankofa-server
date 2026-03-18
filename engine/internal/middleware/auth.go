package middleware

import (
	"fmt"
	"strings"

	"sankofa/engine/internal/database"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"gorm.io/gorm"
)

// AuthMiddleware holds dependencies
type AuthMiddleware struct {
	DB     *gorm.DB
	Secret []byte
}

func NewAuthMiddleware(db *gorm.DB, secret string) *AuthMiddleware {
	if secret == "" {
		secret = "super-secret-key-change-me"
	}
	return &AuthMiddleware{
		DB:     db,
		Secret: []byte(secret),
	}
}

// --- Role Hierarchies ---
// Higher number = more power.

var orgRoleHierarchy = map[string]int{
	"Member": 10,
	"Admin":  20,
	"Owner":  30,
}

var projectRoleHierarchy = map[string]int{
	"Viewer": 10,
	"Editor": 20,
	"Admin":  30,
}

func hasSufficientOrgRole(userRole, requiredRole string) bool {
	return orgRoleHierarchy[userRole] >= orgRoleHierarchy[requiredRole]
}

func hasSufficientProjectRole(userRole, requiredRole string) bool {
	return projectRoleHierarchy[userRole] >= projectRoleHierarchy[requiredRole]
}

// --- Middleware ---

// RequireAuth checks for a valid user session (JWT) and injects user_id into locals.
func (m *AuthMiddleware) RequireAuth(c *fiber.Ctx) error {
	authHeader := c.Get("Authorization")
	if authHeader == "" {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized: Missing token"})
	}

	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 || parts[0] != "Bearer" {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized: Invalid token format"})
	}
	tokenString := parts[1]

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return m.Secret, nil
	})

	if err != nil || !token.Valid {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized: Invalid token"})
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized: Invalid claims"})
	}

	userID, ok := claims["user_id"].(string)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized: Invalid user ID format in token"})
	}
	c.Locals("user_id", userID)

	return c.Next()
}

// RequireOrgAccess checks if the user has at least `minRole` in the organization (:org_id param).
// It also injects org_id and org_role into locals.
func (m *AuthMiddleware) RequireOrgAccess(minRole string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		userID, ok := c.Locals("user_id").(string)
		if !ok || userID == "" {
			return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
		}

		orgID := c.Params("org_id")
		if orgID == "" {
			orgID, _ = c.Locals("org_id").(string)
		}
		if orgID == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Missing organization context"})
		}

		var member database.OrganizationMember
		if err := m.DB.Where("organization_id = ? AND user_id = ?", orgID, userID).First(&member).Error; err != nil {
			return c.Status(403).JSON(fiber.Map{"error": "You are not a member of this organization"})
		}

		if !hasSufficientOrgRole(member.Role, minRole) {
			return c.Status(403).JSON(fiber.Map{
				"error": fmt.Sprintf("Insufficient permissions. Required: %s, your role: %s", minRole, member.Role),
			})
		}

		c.Locals("org_id", orgID)
		c.Locals("org_role", member.Role)
		return c.Next()
	}
}

// RequireProjectAccess checks if the user has at least `minRole` in the current project.
// The project ID must be provided via the x-project-id header.
// Org Owners/Admins bypass the project member check (they implicitly have Admin-level access).
func (m *AuthMiddleware) RequireProjectAccess(minRole string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		userID, ok := c.Locals("user_id").(string)
		if !ok || userID == "" {
			return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
		}

		projectID := c.Get("x-project-id")
		if projectID == "" {
			projectID = c.Params("project_id")
		}
		if projectID == "" {
			// Also check locals (in case it was set by a preceding middleware)
			projectID, _ = c.Locals("project_id").(string)
		}
		if projectID == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Missing project ID (header 'x-project-id' or path param 'project_id' required)"})
		}

		// --- Org-Owner bypass ---
		// If the user is an Owner or Admin of the org that owns this project,
		// grant them implicit Admin-level project access.
		var project database.Project
		if err := m.DB.Where("id = ?", projectID).First(&project).Error; err == nil {
			var orgMember database.OrganizationMember
			if m.DB.Where("organization_id = ? AND user_id = ?", project.OrganizationID, userID).First(&orgMember).Error == nil {
				if orgMember.Role == "Owner" || orgMember.Role == "Admin" {
					c.Locals("project_id", projectID)
					c.Locals("project_role", orgMember.Role) // use org role as effective project role
					c.Locals("project", project)             // Store full project context
					return c.Next()
				}
			}
		}

		// --- Normal Project Member check ---
		var member database.ProjectMember
		if err := m.DB.Where("project_id = ? AND user_id = ?", projectID, userID).First(&member).Error; err != nil {
			return c.Status(403).JSON(fiber.Map{"error": "Access denied: you are not a member of this project"})
		}

		// If we didn't bypass above, we still need the full project for the handler context
		if err := m.DB.Where("id = ?", projectID).First(&project).Error; err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "Project not found"})
		}

		if !hasSufficientProjectRole(member.Role, minRole) {
			return c.Status(403).JSON(fiber.Map{
				"error": fmt.Sprintf("Insufficient permissions. Required: %s, your role: %s", minRole, member.Role),
			})
		}

		c.Locals("project_id", projectID)
		c.Locals("project_role", member.Role)
		c.Locals("project", project) // Store full project context
		return c.Next()
	}
}
