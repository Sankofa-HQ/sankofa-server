package api

import (
	"encoding/json"
	"log"
	"strings"

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
	boards.Get("/shared", h.ListSharedBoards)
	boards.Get("/:id", h.GetBoard)
	boards.Put("/:id", h.UpdateBoard)
	boards.Delete("/:id", h.DeleteBoard)
	boards.Post("/:id/pin", h.PinBoard)
	boards.Post("/:id/duplicate", h.DuplicateBoard)

	// Sharing
	boards.Post("/:id/share", h.ShareBoard)
	boards.Get("/:id/shares", h.ListShares)
	boards.Delete("/:id/shares/:shareId", h.RevokeShare)
	boards.Get("/:id/permission", h.GetBoardPermission)

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

	// Access check: user must be either a direct project member OR have at
	// least one board share in this project (shared dashboards access pattern).
	var member database.ProjectMember
	isMember := db.Where("project_id = ? AND user_id = ?", project.ID, userID).First(&member).Error == nil

	if !isMember {
		// Check if the user has any board-level share access in this project
		var shareCount int64
		db.Model(&database.BoardShare{}).
			Joins("JOIN boards ON boards.id = board_shares.board_id").
			Where("boards.project_id = ? AND board_shares.shared_with_type = 'user' AND board_shares.shared_with_id = ?", project.ID, userID).
			Count(&shareCount)

		if shareCount == 0 {
			// Also check team-based shares
			var teamIDs []string
			db.Model(&database.TeamMember{}).Where("user_id = ?", userID).Pluck("team_id", &teamIDs)
			if len(teamIDs) > 0 {
				db.Model(&database.BoardShare{}).
					Joins("JOIN boards ON boards.id = board_shares.board_id").
					Where("boards.project_id = ? AND board_shares.shared_with_type = 'team' AND board_shares.shared_with_id IN ?", project.ID, teamIDs).
					Count(&shareCount)
			}
		}

		if shareCount == 0 {
			// Check project-wide board shares
			var projectShareCount int64
			db.Model(&database.BoardShare{}).
				Joins("JOIN boards ON boards.id = board_shares.board_id").
				Where("boards.project_id = ? AND board_shares.shared_with_type = 'project' AND board_shares.shared_with_id = ?", project.ID, project.ID).
				Count(&projectShareCount)
			if projectShareCount == 0 {
				return nil, c.Status(403).JSON(fiber.Map{"error": "Access denied"})
			}
		}
	}

	return &project, nil
}

// getBoardPermissionLevel returns the permission level: "owner", "editor", "viewer", or "" (no access)
func (h *BoardsHandler) getBoardPermissionLevel(userID string, board *database.Board) string {
	// Owner
	if board.CreatedByID == userID {
		return "owner"
	}

	// Direct user share
	var directShare []database.BoardShare
	if err := h.DB.Limit(1).Where("board_id = ? AND shared_with_type = 'user' AND shared_with_id = ?", board.ID, userID).Find(&directShare).Error; err == nil && len(directShare) > 0 {
		return directShare[0].Permission
	}

	// Team share — check if user is a member of any team that has access
	var teamShares []database.BoardShare
	if err := h.DB.Where("board_id = ? AND shared_with_type = 'team'", board.ID).Find(&teamShares).Error; err == nil {
		for _, ts := range teamShares {
			var tm []database.TeamMember
			if err := h.DB.Limit(1).Where("team_id = ? AND user_id = ?", ts.SharedWithID, userID).Find(&tm).Error; err == nil && len(tm) > 0 {
				return ts.Permission
			}
		}
	}

	// Project-wide share — check if user is a member of the project
	var projectShare []database.BoardShare
	if err := h.DB.Limit(1).Where("board_id = ? AND shared_with_type = 'project' AND shared_with_id = ?", board.ID, board.ProjectID).Find(&projectShare).Error; err == nil && len(projectShare) > 0 {
		var pm []database.ProjectMember
		if err := h.DB.Limit(1).Where("project_id = ? AND user_id = ?", board.ProjectID, userID).Find(&pm).Error; err == nil && len(pm) > 0 {
			return projectShare[0].Permission
		}
	}

	return ""
}

func (h *BoardsHandler) ListBoards(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil || project == nil {
		return err
	}

	environment := c.Query("environment", "live")

	// ── 1. System board (auto-create if missing) ──────────────────────────
	var defaultBoard database.Board
	if err := h.DB.Where("project_id = ? AND environment = ? AND is_system = ?", project.ID, environment, true).First(&defaultBoard).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			defaultBoard = database.Board{
				ProjectID:   project.ID,
				Name:        "Analytics Overview",
				Description: "Real-time performance across all channels.",
				Environment: environment,
				IsSystem:    true,
				IsPinned:    true,
				CreatedByID: userID,
			}
			if err := h.DB.Create(&defaultBoard).Error; err != nil {
				log.Println("Failed to create default board for project:", project.ID, err)
			}
		}
	}

	// ── 2. Own boards ─────────────────────────────────────────────────────
	var ownBoards []database.Board
	if err := h.DB.Preload("Widgets").Preload("CreatedBy").
		Where("project_id = ? AND environment = ? AND created_by_id = ?", project.ID, environment, userID).
		Order("is_system DESC, created_at ASC").
		Find(&ownBoards).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch boards"})
	}

	// Include system boards created by others (e.g. first user created the system board)
	var systemBoards []database.Board
	if err := h.DB.Preload("Widgets").Preload("CreatedBy").
		Where("project_id = ? AND environment = ? AND is_system = ? AND created_by_id != ?", project.ID, environment, true, userID).
		Find(&systemBoards).Error; err == nil {
		ownBoards = append(systemBoards, ownBoards...)
	}

	// ── 3. Boards shared with this user (via user, team, or project) ──────
	// Collect shared board IDs
	sharedIDSet := make(map[string]string) // boardID -> "user"|"team"|"project"

	// Direct user shares
	var userShareBoardIDs []string
	h.DB.Model(&database.BoardShare{}).
		Joins("JOIN boards ON boards.id = board_shares.board_id").
		Where("board_shares.shared_with_type = 'user' AND board_shares.shared_with_id = ? AND boards.project_id = ? AND boards.environment = ?", userID, project.ID, environment).
		Pluck("board_shares.board_id", &userShareBoardIDs)
	for _, id := range userShareBoardIDs {
		sharedIDSet[id] = "user"
	}

	// Team shares
	var teamIDs []string
	h.DB.Model(&database.TeamMember{}).Where("user_id = ?", userID).Pluck("team_id", &teamIDs)
	if len(teamIDs) > 0 {
		var teamShareBoardIDs []string
		h.DB.Model(&database.BoardShare{}).
			Joins("JOIN boards ON boards.id = board_shares.board_id").
			Where("board_shares.shared_with_type = 'team' AND board_shares.shared_with_id IN ? AND boards.project_id = ? AND boards.environment = ?", teamIDs, project.ID, environment).
			Pluck("board_shares.board_id", &teamShareBoardIDs)
		for _, id := range teamShareBoardIDs {
			if _, exists := sharedIDSet[id]; !exists {
				sharedIDSet[id] = "team"
			}
		}
	}

	// Project-wide shares
	var projectShareBoardIDs []string
	h.DB.Model(&database.BoardShare{}).
		Joins("JOIN boards ON boards.id = board_shares.board_id").
		Where("board_shares.shared_with_type = 'project' AND board_shares.shared_with_id = ? AND boards.project_id = ? AND boards.environment = ?", project.ID, project.ID, environment).
		Pluck("board_shares.board_id", &projectShareBoardIDs)
	for _, id := range projectShareBoardIDs {
		if _, exists := sharedIDSet[id]; !exists {
			sharedIDSet[id] = "project"
		}
	}

	// Remove boards the user already owns
	ownBoardIDSet := make(map[string]bool)
	for _, b := range ownBoards {
		ownBoardIDSet[b.ID] = true
	}

	type BoardWithMeta struct {
		database.Board
		Permission string `json:"permission,omitempty"`
		SharedVia  string `json:"shared_via,omitempty"`
		ShareCount int64  `json:"share_count"`
	}

	var result []BoardWithMeta
	for _, b := range ownBoards {
		var shareCount int64
		h.DB.Model(&database.BoardShare{}).Where("board_id = ?", b.ID).Count(&shareCount)
		result = append(result, BoardWithMeta{Board: b, ShareCount: shareCount})
	}

	if len(sharedIDSet) > 0 {
		var sharedIDs []string
		for id := range sharedIDSet {
			if !ownBoardIDSet[id] {
				sharedIDs = append(sharedIDs, id)
			}
		}
		if len(sharedIDs) > 0 {
			var sharedBoards []database.Board
			h.DB.Preload("Widgets").Preload("CreatedBy").
				Where("id IN ?", sharedIDs).
				Find(&sharedBoards)
			for _, b := range sharedBoards {
				perm := h.getBoardPermissionLevel(userID, &b)
				result = append(result, BoardWithMeta{
					Board:      b,
					Permission: perm,
					SharedVia:  sharedIDSet[b.ID],
				})
			}
		}
	}

	return c.JSON(fiber.Map{"boards": result})
}

func (h *BoardsHandler) GetBoard(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	boardID := c.Params("id")
	var board database.Board

	if err := h.DB.Preload("Widgets").Preload("CreatedBy").Where("id = ? AND project_id = ?", boardID, project.ID).First(&board).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Board not found"})
	}

	// Check access: owner, system, or shared
	if !board.IsSystem && board.CreatedByID != userID {
		perm := h.getBoardPermissionLevel(userID, &board)
		if perm == "" {
			return c.Status(403).JSON(fiber.Map{"error": "You don't have access to this board"})
		}
	}

	permission := "owner"
	if board.IsSystem {
		permission = "editor"
	} else if board.CreatedByID != userID {
		permission = h.getBoardPermissionLevel(userID, &board)
	}

	return c.JSON(fiber.Map{"board": board, "permission": permission})
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
		Environment string `json:"environment"`
	}

	var req CreateBoardRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request payload"})
	}

	if req.Name == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Board name is required"})
	}

	env := req.Environment
	if env == "" {
		env = "live"
	}

	board := database.Board{
		ProjectID:   project.ID,
		Name:        req.Name,
		Description: req.Description,
		Environment: env,
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

// UpdateBoard handles renaming/updating a board's details
func (h *BoardsHandler) UpdateBoard(c *fiber.Ctx) error {
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
		return c.Status(403).JSON(fiber.Map{"error": "Cannot modify system boards"})
	}

	// Check permission: owner or editor
	perm := h.getBoardPermissionLevel(userID, &board)
	if perm != "owner" && perm != "editor" {
		return c.Status(403).JSON(fiber.Map{"error": "You don't have permission to edit this board"})
	}

	type UpdateBoardRequest struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	var req UpdateBoardRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request payload"})
	}

	updates := map[string]interface{}{}
	if req.Name != "" {
		updates["name"] = req.Name
	}
	if req.Description != "" {
		updates["description"] = req.Description
	}

	if len(updates) == 0 {
		return c.Status(400).JSON(fiber.Map{"error": "No changes to apply"})
	}

	if err := h.DB.Model(&board).Updates(updates).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to update board"})
	}

	h.DB.Preload("CreatedBy").First(&board, "id = ?", board.ID)

	return c.JSON(fiber.Map{"message": "Board updated", "board": board})
}

// DeleteBoard permanently removes a custom board
func (h *BoardsHandler) DeleteBoard(c *fiber.Ctx) error {
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
		return c.Status(403).JSON(fiber.Map{"error": "Cannot delete system boards"})
	}

	// Only owner can delete
	if board.CreatedByID != userID {
		return c.Status(403).JSON(fiber.Map{"error": "Only the board owner can delete it"})
	}

	tx := h.DB.Begin()

	// Delete shares first
	if err := tx.Where("board_id = ?", board.ID).Delete(&database.BoardShare{}).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete board shares"})
	}

	// Delete widgets
	if err := tx.Where("board_id = ?", board.ID).Delete(&database.BoardWidget{}).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete board widgets"})
	}

	// Delete board
	if err := tx.Delete(&board).Error; err != nil {
		tx.Rollback()
		return c.Status(500).JSON(fiber.Map{"error": "Failed to delete board"})
	}

	tx.Commit()

	return c.JSON(fiber.Map{"message": "Board deleted successfully"})
}

// DuplicateBoard creates a copy of a board with all its widgets
func (h *BoardsHandler) DuplicateBoard(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	boardID := c.Params("id")
	var original database.Board
	if err := h.DB.Preload("Widgets").Where("id = ? AND project_id = ?", boardID, project.ID).First(&original).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Board not found"})
	}

	// Create the copy (layout will be set after widgets are created)
	newBoard := database.Board{
		ProjectID:   project.ID,
		Name:        original.Name + " (Copy)",
		Description: original.Description,
		Environment: original.Environment,
		IsSystem:    false,
		IsPinned:    false,
		CreatedByID: userID,
	}

	if err := h.DB.Create(&newBoard).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to duplicate board"})
	}

	idMapping := make(map[string]string) // oldWidgetID → newWidgetID

	if original.IsSystem {
		// System boards have no DB widgets — create them from the default system widget definitions
		type systemWidgetDef struct {
			Name       string
			SystemType string
			W          int
			H          int
		}
		defaultWidgets := []systemWidgetDef{
			{Name: "Active Users", SystemType: "active_users", W: 3, H: 2},
			{Name: "New Users", SystemType: "new_users", W: 3, H: 2},
			{Name: "Avg Session", SystemType: "avg_session", W: 3, H: 2},
			{Name: "Total Events", SystemType: "total_events", W: 3, H: 2},
			{Name: "Active Users Today", SystemType: "active_users_today", W: 3, H: 3},
			{Name: "Visitors Trend", SystemType: "active_visitors_trend", W: 9, H: 3},
			{Name: "Top Events", SystemType: "top_events", W: 6, H: 4},
			{Name: "Top Users", SystemType: "top_users", W: 6, H: 4},
			{Name: "Geo Breakdown", SystemType: "geo_breakdown", W: 3, H: 3},
			{Name: "Browser Distribution", SystemType: "browser_breakdown", W: 3, H: 3},
			{Name: "OS Distribution", SystemType: "device_breakdown", W: 3, H: 3},
			{Name: "Platform Distribution", SystemType: "platform_breakdown", W: 3, H: 3},
		}

		var layoutItems []map[string]interface{}
		// Layout positions matching the system dashboard grid
		positions := []struct{ x, y int }{
			{0, 0}, {3, 0}, {6, 0}, {9, 0}, // KPI row (y=0)
			{9, 2}, {0, 2}, // Active Today + Visitors Trend (y=2)
			{0, 5}, {6, 5}, // Top Events + Top Users (y=5)
			{0, 9}, {3, 9}, {6, 9}, {9, 9}, // Breakdowns (y=9)
		}

		for i, def := range defaultWidgets {
			queryAST, _ := json.Marshal(map[string]string{"system_type": def.SystemType})
			newWidget := database.BoardWidget{
				BoardID:  newBoard.ID,
				Name:     def.Name,
				Type:     "system",
				QueryAST: queryAST,
			}
			if err := h.DB.Create(&newWidget).Error; err != nil {
				log.Println("Failed to create system widget:", err)
				continue
			}
			layoutItems = append(layoutItems, map[string]interface{}{
				"i": newWidget.ID,
				"x": positions[i].x,
				"y": positions[i].y,
				"w": def.W,
				"h": def.H,
			})
		}

		if layoutJSON, err := json.Marshal(layoutItems); err == nil {
			h.DB.Model(&newBoard).Update("layout", layoutJSON)
		}
	} else {
		// Custom board — copy widgets and remap layout IDs
		for _, w := range original.Widgets {
			newWidget := database.BoardWidget{
				BoardID:  newBoard.ID,
				Name:     w.Name,
				Type:     w.Type,
				QueryAST: w.QueryAST,
			}
			if err := h.DB.Create(&newWidget).Error; err != nil {
				log.Println("Failed to copy widget:", err)
				continue
			}
			idMapping[w.ID] = newWidget.ID
		}

		// Remap layout IDs
		if len(original.Layout) > 0 {
			var layoutItems []map[string]interface{}
			if err := json.Unmarshal(original.Layout, &layoutItems); err == nil {
				for idx, item := range layoutItems {
					if oldID, ok := item["i"].(string); ok {
						if newID, exists := idMapping[oldID]; exists {
							layoutItems[idx]["i"] = newID
						}
					}
				}
				if remappedLayout, err := json.Marshal(layoutItems); err == nil {
					h.DB.Model(&newBoard).Update("layout", remappedLayout)
				}
			}
		}
	}

	// Reload with widgets
	h.DB.Preload("Widgets").Preload("CreatedBy").First(&newBoard, "id = ?", newBoard.ID)

	return c.Status(201).JSON(fiber.Map{"message": "Board duplicated", "board": newBoard})
}

// ShareBoard shares a board with a user, team, or entire project
func (h *BoardsHandler) ShareBoard(c *fiber.Ctx) error {
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

	// Only owner can share
	if board.CreatedByID != userID {
		return c.Status(403).JSON(fiber.Map{"error": "Only the board owner can share it"})
	}

	type ShareRequest struct {
		SharedWithType string `json:"shared_with_type"` // "user", "team", "project"
		SharedWithID   string `json:"shared_with_id"`
		Permission     string `json:"permission"` // "viewer", "editor"
	}

	var req ShareRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request payload"})
	}

	// Validate shared_with_type
	req.SharedWithType = strings.ToLower(req.SharedWithType)
	if req.SharedWithType != "user" && req.SharedWithType != "team" && req.SharedWithType != "project" {
		return c.Status(400).JSON(fiber.Map{"error": "shared_with_type must be 'user', 'team', or 'project'"})
	}

	// Validate permission
	req.Permission = strings.ToLower(req.Permission)
	if req.Permission != "viewer" && req.Permission != "editor" {
		req.Permission = "viewer"
	}

	if req.SharedWithID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "shared_with_id is required"})
	}

	// Don't share with yourself
	if req.SharedWithType == "user" && req.SharedWithID == userID {
		return c.Status(400).JSON(fiber.Map{"error": "Cannot share a board with yourself"})
	}

	// Check if already shared with this target
	var existing database.BoardShare
	if err := h.DB.Where("board_id = ? AND shared_with_type = ? AND shared_with_id = ?",
		board.ID, req.SharedWithType, req.SharedWithID).First(&existing).Error; err == nil {
		// Update permission
		h.DB.Model(&existing).Update("permission", req.Permission)
		return c.JSON(fiber.Map{"message": "Share permission updated", "share": existing})
	}

	share := database.BoardShare{
		BoardID:        board.ID,
		SharedWithType: req.SharedWithType,
		SharedWithID:   req.SharedWithID,
		Permission:     req.Permission,
		SharedByID:     userID,
	}

	if err := h.DB.Create(&share).Error; err != nil {
		log.Println("Failed to create board share:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to share board"})
	}

	// Preload shared_by
	h.DB.Preload("SharedBy").First(&share, "id = ?", share.ID)

	return c.Status(201).JSON(fiber.Map{"message": "Board shared", "share": share})
}

// ListShares returns all shares for a board
func (h *BoardsHandler) ListShares(c *fiber.Ctx) error {
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

	var shares []database.BoardShare
	if err := h.DB.Preload("SharedBy").Where("board_id = ?", board.ID).Order("created_at DESC").Find(&shares).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch shares"})
	}

	// Enrich: resolve names for shared_with_id
	type EnrichedShare struct {
		database.BoardShare
		SharedWithName  string `json:"shared_with_name"`
		SharedWithEmail string `json:"shared_with_email,omitempty"`
	}

	enriched := make([]EnrichedShare, 0, len(shares))
	for _, s := range shares {
		es := EnrichedShare{BoardShare: s}
		switch s.SharedWithType {
		case "user":
			var u database.User
			if err := h.DB.First(&u, "id = ?", s.SharedWithID).Error; err == nil {
				es.SharedWithName = u.FullName
				es.SharedWithEmail = u.Email
			}
		case "team":
			var t database.Team
			if err := h.DB.First(&t, "id = ?", s.SharedWithID).Error; err == nil {
				es.SharedWithName = t.Name
			}
		case "project":
			var p database.Project
			if err := h.DB.First(&p, "id = ?", s.SharedWithID).Error; err == nil {
				es.SharedWithName = p.Name + " (All Members)"
			}
		}
		enriched = append(enriched, es)
	}

	return c.JSON(fiber.Map{"shares": enriched, "is_owner": board.CreatedByID == userID})
}

// RevokeShare removes a specific share
func (h *BoardsHandler) RevokeShare(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	boardID := c.Params("id")
	shareID := c.Params("shareId")

	var board database.Board
	if err := h.DB.Where("id = ? AND project_id = ?", boardID, project.ID).First(&board).Error; err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Board not found"})
	}

	// Only owner can revoke shares
	if board.CreatedByID != userID {
		return c.Status(403).JSON(fiber.Map{"error": "Only the board owner can manage shares"})
	}

	if err := h.DB.Where("id = ? AND board_id = ?", shareID, board.ID).Delete(&database.BoardShare{}).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to revoke share"})
	}

	return c.JSON(fiber.Map{"message": "Share revoked"})
}

// ListSharedBoards returns boards that others have shared with the current user
func (h *BoardsHandler) ListSharedBoards(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	environment := c.Query("environment", "live")

	// 1. Direct user shares
	var userShareBoardIDs []string
	h.DB.Model(&database.BoardShare{}).
		Joins("JOIN boards ON boards.id = board_shares.board_id").
		Where("board_shares.shared_with_type = 'user' AND board_shares.shared_with_id = ? AND boards.project_id = ? AND boards.environment = ?", userID, project.ID, environment).
		Pluck("board_shares.board_id", &userShareBoardIDs)

	// 2. Team shares — get user's teams first
	var teamIDs []string
	h.DB.Model(&database.TeamMember{}).Where("user_id = ?", userID).Pluck("team_id", &teamIDs)

	var teamShareBoardIDs []string
	if len(teamIDs) > 0 {
		h.DB.Model(&database.BoardShare{}).
			Joins("JOIN boards ON boards.id = board_shares.board_id").
			Where("board_shares.shared_with_type = 'team' AND board_shares.shared_with_id IN ? AND boards.project_id = ? AND boards.environment = ?", teamIDs, project.ID, environment).
			Pluck("board_shares.board_id", &teamShareBoardIDs)
	}

	// 3. Project-wide shares
	var projectShareBoardIDs []string
	h.DB.Model(&database.BoardShare{}).
		Joins("JOIN boards ON boards.id = board_shares.board_id").
		Where("board_shares.shared_with_type = 'project' AND board_shares.shared_with_id = ? AND boards.project_id = ? AND boards.environment = ?", project.ID, project.ID, environment).
		Pluck("board_shares.board_id", &projectShareBoardIDs)

	// Combine and deduplicate
	boardIDSet := make(map[string]bool)
	for _, id := range userShareBoardIDs {
		boardIDSet[id] = true
	}
	for _, id := range teamShareBoardIDs {
		boardIDSet[id] = true
	}
	for _, id := range projectShareBoardIDs {
		boardIDSet[id] = true
	}

	// Remove own boards
	var uniqueIDs []string
	for id := range boardIDSet {
		uniqueIDs = append(uniqueIDs, id)
	}

	if len(uniqueIDs) == 0 {
		return c.JSON(fiber.Map{"boards": []interface{}{}})
	}

	var sharedBoards []database.Board
	if err := h.DB.Preload("Widgets").Preload("CreatedBy").
		Where("id IN ? AND created_by_id != ?", uniqueIDs, userID).
		Order("created_at DESC").
		Find(&sharedBoards).Error; err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to fetch shared boards"})
	}

	// Add permission info to each board
	type BoardWithPermission struct {
		database.Board
		Permission string `json:"permission"`
		SharedVia  string `json:"shared_via"` // "user", "team", "project"
	}

	result := make([]BoardWithPermission, 0, len(sharedBoards))
	for _, b := range sharedBoards {
		bwp := BoardWithPermission{Board: b}
		bwp.Permission = h.getBoardPermissionLevel(userID, &b)

		// Determine how it was shared
		for _, id := range userShareBoardIDs {
			if id == b.ID {
				bwp.SharedVia = "user"
				break
			}
		}
		if bwp.SharedVia == "" {
			for _, id := range teamShareBoardIDs {
				if id == b.ID {
					bwp.SharedVia = "team"
					break
				}
			}
		}
		if bwp.SharedVia == "" {
			bwp.SharedVia = "project"
		}

		result = append(result, bwp)
	}

	return c.JSON(fiber.Map{"boards": result})
}

// GetBoardPermission returns the current user's permission level for a board
func (h *BoardsHandler) GetBoardPermission(c *fiber.Ctx) error {
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

	permission := "none"
	if board.IsSystem {
		permission = "editor"
	} else if board.CreatedByID == userID {
		permission = "owner"
	} else {
		perm := h.getBoardPermissionLevel(userID, &board)
		if perm != "" {
			permission = perm
		}
	}

	return c.JSON(fiber.Map{"permission": permission, "is_owner": board.CreatedByID == userID})
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

	// Check permission
	if !board.IsSystem {
		perm := h.getBoardPermissionLevel(userID, &board)
		if perm != "owner" && perm != "editor" {
			return c.Status(403).JSON(fiber.Map{"error": "You don't have permission to edit this board"})
		}
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

	// Check permission
	if !board.IsSystem {
		perm := h.getBoardPermissionLevel(userID, &board)
		if perm != "owner" && perm != "editor" {
			return c.Status(403).JSON(fiber.Map{"error": "You don't have permission to edit this board"})
		}
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

	// Check permission
	if !board.IsSystem {
		perm := h.getBoardPermissionLevel(userID, &board)
		if perm != "owner" && perm != "editor" {
			return c.Status(403).JSON(fiber.Map{"error": "You don't have permission to edit this board"})
		}
	}

	if err := h.DB.Where("id = ? AND board_id = ?", widgetID, board.ID).Delete(&database.BoardWidget{}).Error; err != nil {
		log.Println("Failed to delete widget:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to remove widget"})
	}

	return c.JSON(fiber.Map{"message": "Widget removed"})
}
