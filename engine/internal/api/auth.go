package api

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"sankofa/engine/internal/database"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

// AuthHandler holds dependencies for auth routes
type AuthHandler struct {
	DB *gorm.DB
}

func NewAuthHandler(db *gorm.DB) *AuthHandler {
	return &AuthHandler{DB: db}
}

func (h *AuthHandler) RegisterRoutes(api fiber.Router) {
	auth := api.Group("/auth")
	auth.Post("/register", h.Register)
	auth.Post("/login", h.Login)
	auth.Get("/me", h.Me)
	auth.Put("/me", h.UpdateMe)
	auth.Put("/password", h.UpdatePassword)
}

// Helper to extract user ID from token
func (h *AuthHandler) extractUserFromToken(c *fiber.Ctx) (uint, error) {
	authHeader := c.Get("Authorization")
	if len(authHeader) < 7 || authHeader[:7] != "Bearer " {
		return 0, fmt.Errorf("missing or invalid token")
	}
	tokenString := authHeader[7:]

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return jwtSecret, nil
	})

	if err != nil || !token.Valid {
		return 0, fmt.Errorf("invalid token")
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return 0, fmt.Errorf("invalid token claims")
	}

	return uint(claims["user_id"].(float64)), nil
}

func (h *AuthHandler) UpdateMe(c *fiber.Ctx) error {
	userID, err := h.extractUserFromToken(c)
	if err != nil {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	type UpdateReq struct {
		FullName  string `json:"full_name"`
		Email     string `json:"email"`
		AvatarURL string `json:"avatar_url"`
	}
	var req UpdateReq
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}

	var user database.User
	if err := h.DB.First(&user, userID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "User not found"})
	}

	if req.FullName != "" {
		user.FullName = req.FullName
	}
	if req.Email != "" {
		// potential: check for email uniqueness if changed
		user.Email = req.Email
	}
	if req.AvatarURL != "" {
		user.AvatarURL = req.AvatarURL
	}
	user.UpdatedAt = time.Now()

	if err := h.DB.Save(&user).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to update profile"})
	}

	return c.JSON(user)
}

func (h *AuthHandler) UpdatePassword(c *fiber.Ctx) error {
	userID, err := h.extractUserFromToken(c)
	if err != nil {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	type PasswordReq struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	var req PasswordReq
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}

	if len(req.NewPassword) < 8 {
		return c.Status(400).JSON(fiber.Map{"error": "Password must be at least 8 characters"})
	}

	var user database.User
	if err := h.DB.First(&user, userID).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "User not found"})
	}

	// Verify current password
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.CurrentPassword)); err != nil {
		return c.Status(401).JSON(fiber.Map{"error": "Incorrect current password"})
	}

	// Hash new password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to hash password"})
	}

	user.PasswordHash = string(hashedPassword)
	user.UpdatedAt = time.Now()

	if err := h.DB.Save(&user).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to update password"})
	}

	return c.JSON(fiber.Map{"status": "ok"})
}

func (h *AuthHandler) Register(c *fiber.Ctx) error {
	type RegisterReq struct {
		Email            string `json:"email"`
		Password         string `json:"password"`
		FullName         string `json:"full_name"`
		OrganizationName string `json:"organization_name"` // changed from organization_name
	}
	var req RegisterReq
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}

	// 1. Validate
	if len(req.Password) < 8 {
		return c.Status(400).JSON(fiber.Map{"error": "Password must be at least 8 characters"})
	}
	// Check if email exists
	var existingUser database.User
	if err := h.DB.Select("id").Where("email = ?", req.Email).First(&existingUser).Error; err == nil {
		return c.Status(409).JSON(fiber.Map{"error": "Email already registered"})
	}

	// 2. Hash Password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to hash password"})
	}

	tx := h.DB.Begin()

	// 3. Create User
	user := database.User{
		Email:        req.Email,
		PasswordHash: string(hashedPassword),
		FullName:     req.FullName,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	if err := tx.Create(&user).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to create user"})
	}

	// 4. Create Organization
	if req.OrganizationName == "" {
		req.OrganizationName = req.FullName + "'s Org"
	}
	org := database.Organization{
		Name:      req.OrganizationName,
		Slug:      generateSlug(req.OrganizationName),
		Plan:      "Free",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := tx.Create(&org).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to create organization"})
	}

	// 5. Link User to Org (Owner)
	if err := tx.Create(&database.OrganizationMember{
		OrganizationID: org.ID,
		UserID:         user.ID,
		Role:           "Owner",
		CreatedAt:      time.Now(),
	}).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to link user to org"})
	}

	// 6. Create Default Project
	apiKey, _ := generateAPIKey()
	project := database.Project{
		OrganizationID: org.ID,
		Name:           "Production", // Default name
		APIKey:         apiKey,
		Timezone:       "UTC",
		Region:         "us-east-1",
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}
	if err := tx.Create(&project).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to create project"})
	}

	// 7. Link User to Project (Admin)
	if err := tx.Create(&database.ProjectMember{
		ProjectID: project.ID,
		UserID:    user.ID,
		Role:      "Admin",
		CreatedAt: time.Now(),
	}).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to link user to project"})
	}

	// Update user's current project ID
	if err := tx.Model(&user).Update("current_project_id", project.ID).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to update user context"})
	}

	if err := tx.Commit().Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Transaction commit failed"})
	}

	// 8. Generate Token
	token, err := generateJWT(user.ID, user.Email)
	if err != nil {
		// Note: User is created but token failed. Client can login.
		log.Println("JWT Generation failed:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to generate token"})
	}

	// Return Success
	return c.JSON(fiber.Map{
		"status":   "ok",
		"token":    token,
		"redirect": "/dashboard",
		"user": fiber.Map{
			"id":        user.ID,
			"email":     user.Email,
			"full_name": user.FullName,
			"org_id":    org.ID, // convenient for frontend
		},
	})
}

func (h *AuthHandler) Login(c *fiber.Ctx) error {
	type LoginReq struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	var req LoginReq
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid input"})
	}

	var user database.User
	if err := h.DB.First(&user, "email = ?", req.Email).Error; err != nil {
		return c.Status(401).JSON(fiber.Map{"error": "Invalid credentials"})
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		return c.Status(401).JSON(fiber.Map{"error": "Invalid credentials"})
	}

	// Generate Token
	token, err := generateJWT(user.ID, user.Email)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to generate token"})
	}

	return c.JSON(fiber.Map{
		"status":          "ok",
		"token":           token,
		"last_project_id": user.CurrentProjectID,
	})
}

func (h *AuthHandler) Me(c *fiber.Ctx) error {
	userID, err := h.extractUserFromToken(c)
	if err != nil {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	var user database.User
	if err := h.DB.Preload("CurrentProject").First(&user, "id = ?", userID).Error; err != nil {
		return c.Status(401).JSON(fiber.Map{"error": "User not found"})
	}

	// Fetch memberships
	var orgMemberships []database.OrganizationMember
	h.DB.Preload("Organization").Where("user_id = ?", user.ID).Find(&orgMemberships)

	var projectMemberships []database.ProjectMember
	h.DB.Where("user_id = ?", user.ID).Find(&projectMemberships)

	return c.JSON(fiber.Map{
		"user":                user,
		"org_memberships":     orgMemberships,
		"project_memberships": projectMemberships,
	})
}

// Helpers

func generateSlug(name string) string {
	// Simple slug generation: lowercase, replace spaces with hyphens
	// robust slug generation would use a library
	return name // Placeholder, needs actual implementation
}

func generateAPIKey() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "sk_live_" + hex.EncodeToString(bytes), nil
}

// Secret key for JWT (Should be in env)
var jwtSecret = []byte("super-secret-key-change-me")

func SetJWTSecret(secret string) {
	if secret != "" {
		jwtSecret = []byte(secret)
	}
}

func generateJWT(userID uint, email string) (string, error) {
	claims := jwt.MapClaims{
		"user_id": userID,
		"email":   email,
		"exp":     time.Now().Add(time.Hour * 24 * 14).Unix(), // 14 days
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(jwtSecret)
}
