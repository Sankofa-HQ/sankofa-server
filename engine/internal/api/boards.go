package api

import (
	"log"

	"sankofa/engine/internal/database"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type BoardsHandler struct {
	DB *gorm.DB
	CH driver.Conn
}

func NewBoardsHandler(db *gorm.DB, ch driver.Conn) *BoardsHandler {
	return &BoardsHandler{DB: db, CH: ch}
}

func (h *BoardsHandler) RegisterRoutes(router fiber.Router, authMiddleware fiber.Handler) {
	boards := router.Group("/boards", authMiddleware)

	boards.Get("/", h.ListBoards)
	boards.Get("/:id", h.GetBoard)
	boards.Post("/:id/pin", h.PinBoard)
}

func getProjectFromContext(c *fiber.Ctx, db *gorm.DB, userID string) (*database.Project, error) {
	projectID := c.Get("x-project-id")
	if projectID == "" {
		projectID = c.Query("project_id", "")
	}

	if projectID == "" {
		var user database.User
		if err := db.First(&user, "id = ?", userID).Error; err != nil {
			return nil, c.Status(500).JSON(fiber.Map{"error": "Failed to get user"})
		}
		if user.CurrentProjectID == nil {
			return nil, c.Status(400).JSON(fiber.Map{"error": "No project selected"})
		}
		projectID = *user.CurrentProjectID
	}

	var project database.Project
	if err := db.First(&project, "id = ?", projectID).Error; err != nil {
		return nil, c.Status(404).JSON(fiber.Map{"error": "Project not found"})
	}

	var member database.ProjectMember
	if err := db.First(&member, "project_id = ? AND user_id = ?", project.ID, userID).Error; err != nil {
		return nil, c.Status(403).JSON(fiber.Map{"error": "Access denied"})
	}

	return &project, nil
}

func (h *BoardsHandler) ListBoards(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	var boards []database.Board

	var defaultBoard database.Board
	if err := h.DB.Where("project_id = ? AND is_system = ?", project.ID, true).First(&defaultBoard).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			defaultBoard = database.Board{
				ProjectID:   project.ID,
				Name:        "Analytics Overview",
				Description: "Real-time performance across all channels.",
				IsSystem:    true,
				IsPinned:    true,
				CreatedByID: userID,
			}
			if err := h.DB.Create(&defaultBoard).Error; err != nil {
				log.Println("Failed to create default board for project:", project.ID, err)
			}
		}
	}

	if err := h.DB.Preload("Widgets").Where("project_id = ?", project.ID).Order("is_system DESC, created_at ASC").Find(&boards).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch boards"})
	}

	return c.JSON(fiber.Map{"boards": boards})
}

func (h *BoardsHandler) GetBoard(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	boardID := c.Params("id")
	var board database.Board

	if err := h.DB.Preload("Widgets").Where("id = ? AND project_id = ?", boardID, project.ID).First(&board).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Board not found"})
	}

	return c.JSON(fiber.Map{"board": board})
}

// PinBoard sets a specific board as the user's default pinned board for the current project
func (h *BoardsHandler) PinBoard(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	boardID := c.Params("id")

	// Verify board exists and belongs to the project
	var targetBoard database.Board
	if err := h.DB.Where("id = ? AND project_id = ?", boardID, project.ID).First(&targetBoard).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Board not found"})
	}

	// Transaction to safely perform unpin/pin
	tx := h.DB.Begin()

	// 1. Unpin all current boards for this project
	if err := tx.Model(&database.Board{}).Where("project_id = ?", project.ID).Update("is_pinned", false).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to update board statuses"})
	}

	// 2. Pin the requested board
	if err := tx.Model(&targetBoard).Update("is_pinned", true).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to pin board"})
	}

	tx.Commit()

	return c.JSON(fiber.Map{
		"message": "Board pinned successfully",
		"board":   targetBoard,
	})
}
