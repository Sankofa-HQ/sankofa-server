package api

import (
	"encoding/json"
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
	boards.Post("/", h.CreateBoard)
	boards.Get("/:id", h.GetBoard)
	boards.Post("/:id/pin", h.PinBoard)

	// Edit Mode Routes
	boards.Put("/:id/layout", h.UpdateBoardLayout)
	boards.Post("/:id/widgets", h.AddBoardWidget)
	boards.Delete("/:id/widgets/:widgetId", h.DeleteBoardWidget)
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

// CreateBoard handles the creation of a new custom dashboard
func (h *BoardsHandler) CreateBoard(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	type CreateBoardRequest struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	var req CreateBoardRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request payload"})
	}

	if req.Name == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Board name is required"})
	}

	board := database.Board{
		ProjectID:   project.ID,
		Name:        req.Name,
		Description: req.Description,
		IsSystem:    false,
		IsPinned:    false,
		CreatedByID: userID,
	}

	if err := h.DB.Create(&board).Error; err != nil {
		log.Println("Failed to create new board:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to create board"})
	}

	return c.Status(201).JSON(fiber.Map{
		"message": "Board created successfully",
		"board":   board,
	})
}

// UpdateBoardLayout handles saving the react-grid-layout JSON array
func (h *BoardsHandler) UpdateBoardLayout(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	boardID := c.Params("id")

	var board database.Board
	if err := h.DB.Where("id = ? AND project_id = ?", boardID, project.ID).First(&board).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Board not found"})
	}

	if board.IsSystem {
		return c.Status(403).JSON(fiber.Map{"error": "Cannot modify system board layout"})
	}

	// We parse arbitrary JSON (the layout array mapped from react-grid-layout)
	type UpdateLayoutRequest struct {
		Layout []map[string]interface{} `json:"layout"`
	}

	var req UpdateLayoutRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid payload format"})
	}

	// Convert back to string/bytes for DB storage since layout is raw message
	layoutBytes, err := json.Marshal(req.Layout)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to serialize layout"})
	}

	if err := h.DB.Model(&board).Update("layout", layoutBytes).Error; err != nil {
		log.Println("Failed to update layout:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to save layout"})
	}

	return c.JSON(fiber.Map{"message": "Layout saved", "board": board})
}

// AddBoardWidget adds a new widget to a board referencing an external query (Saved Funnel, etc)
func (h *BoardsHandler) AddBoardWidget(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	boardID := c.Params("id")
	var board database.Board
	if err := h.DB.Where("id = ? AND project_id = ?", boardID, project.ID).First(&board).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Board not found"})
	}

	if board.IsSystem {
		return c.Status(403).JSON(fiber.Map{"error": "Cannot modify system board"})
	}

	type AddWidgetRequest struct {
		Name     string                 `json:"name"`
		Type     string                 `json:"type"`      // "flow", "funnel", "insight", "retention", "line-chart"
		QueryAST map[string]interface{} `json:"query_ast"` // Payload to execute it later
	}

	var req AddWidgetRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid payload format"})
	}

	queryBytes, _ := json.Marshal(req.QueryAST)

	widget := database.BoardWidget{
		BoardID:  board.ID,
		Name:     req.Name,
		Type:     req.Type,
		QueryAST: queryBytes,
	}

	if err := h.DB.Create(&widget).Error; err != nil {
		log.Println("Failed to add board widget:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to add widget"})
	}

	return c.JSON(fiber.Map{"message": "Widget added", "widget": widget})
}

// DeleteBoardWidget removes a widget by its ID
func (h *BoardsHandler) DeleteBoardWidget(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	boardID := c.Params("id")
	widgetID := c.Params("widgetId")

	var board database.Board
	if err := h.DB.Where("id = ? AND project_id = ?", boardID, project.ID).First(&board).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Board not found"})
	}

	if board.IsSystem {
		return c.Status(403).JSON(fiber.Map{"error": "Cannot modify system board"})
	}

	if err := h.DB.Where("id = ? AND board_id = ?", widgetID, board.ID).Delete(&database.BoardWidget{}).Error; err != nil {
		log.Println("Failed to delete widget:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to remove widget"})
	}

	return c.JSON(fiber.Map{"message": "Widget removed"})
}
