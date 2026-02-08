package middleware

import (
	"fmt"
	"strconv"
	"strings"

	"sankofa/engine/internal/database"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"gorm.io/gorm"
)

// AuthMiddleware holds dependencies
type AuthMiddleware struct {
	DB *gorm.DB
}

func NewAuthMiddleware(db *gorm.DB) *AuthMiddleware {
	return &AuthMiddleware{DB: db}
}

// Secret key (duplicate of auth.go for now, should be shared)
var jwtSecret = []byte("super-secret-key-change-me")

// RequireAuth checks for valid user session (JWT)
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
		return jwtSecret, nil
	})

	if err != nil || !token.Valid {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized: Invalid token"})
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized: Invalid claims"})
	}

	// Inject user_id into locals
	// claims["user_id"] is float64 in JSON
	userID := uint(claims["user_id"].(float64))
	c.Locals("user_id", userID) // Store as uint

	return c.Next()
}

// RequireProjectAccess ensures user has a specific role in the project
// Role levels: Viewer (1), Editor (2), Admin (3)
// For simplicity, we just pass the minimum role string and check equivalence or map to int
func (m *AuthMiddleware) RequireProjectAccess(minRole string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		userID := c.Locals("user_id") // Assumes RequireAuth ran first
		if userID == nil {
			return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
		}

		projectIDStr := c.Get("x-project-id")
		if projectIDStr == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Missing x-project-id header"})
		}

		projectID, err := strconv.Atoi(projectIDStr)
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid project ID"})
		}

		var member database.ProjectMember
		if err := m.DB.Where("project_id = ? AND user_id = ?", projectID, userID).First(&member).Error; err != nil {
			return c.Status(403).JSON(fiber.Map{"error": "Access denied to project"})
		}

		// Simple Role Check
		if !hasSufficientRole(member.Role, minRole) {
			return c.Status(403).JSON(fiber.Map{"error": "Insufficient permissions"})
		}

		// Inject project context
		c.Locals("project_id", uint(projectID))
		return c.Next()
	}
}

// RequireOrgAccess ensures user has a specific role in the organization
func (m *AuthMiddleware) RequireOrgAccess(minRole string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		userID := c.Locals("user_id") // Assumes RequireAuth ran first
		if userID == nil {
			return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
		}

		orgIDStr := c.Params("org_id")
		if orgIDStr == "" {
			return c.Status(400).JSON(fiber.Map{"error": "Missing org_id param"})
		}

		orgID, err := strconv.Atoi(orgIDStr)
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid org ID"})
		}

		var member database.OrganizationMember
		if err := m.DB.Where("organization_id = ? AND user_id = ?", orgID, userID).First(&member).Error; err != nil {
			return c.Status(403).JSON(fiber.Map{"error": "Access denied to organization"})
		}

		// Role Check
		if !hasSufficientOrgRole(member.Role, minRole) {
			return c.Status(403).JSON(fiber.Map{"error": "Insufficient permissions"})
		}

		// Inject org context
		c.Locals("org_id", uint(orgID))
		return c.Next()
	}
}

func hasSufficientRole(userRole, requiredRole string) bool {
	roles := map[string]int{
		"Viewer": 1,
		"Editor": 2,
		"Admin":  3,
	}
	// "Owner" maps to Admin for project access if not explicitly defined
	if userRole == "Owner" {
		userRole = "Admin"
	}

	return roles[userRole] >= roles[requiredRole]
}

func hasSufficientOrgRole(userRole, requiredRole string) bool {
	roles := map[string]int{
		"Member": 1,
		"Admin":  2,
		"Owner":  3,
	}
	return roles[userRole] >= roles[requiredRole]
}
