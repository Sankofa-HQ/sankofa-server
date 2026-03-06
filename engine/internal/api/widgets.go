package api

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

type WidgetsHandler struct {
	DB *gorm.DB
	CH driver.Conn
}

func NewWidgetsHandler(db *gorm.DB, ch driver.Conn) *WidgetsHandler {
	return &WidgetsHandler{DB: db, CH: ch}
}

func (h *WidgetsHandler) RegisterRoutes(router fiber.Router, authMiddleware fiber.Handler) {
	widgets := router.Group("/widgets", authMiddleware)
	widgets.Get("/kpi-summary", h.GetKPISummary)
	widgets.Get("/active-visitors", h.GetActiveVisitorsSeries)
	widgets.Get("/top-events", h.GetTopEvents)
	widgets.Get("/geo-breakdown", h.GetGeographicBreakdown)
	widgets.Get("/device-breakdown", h.GetDeviceBreakdown)
	widgets.Get("/top-users", h.GetTopUsers)
}

// GetKPISummary returns 4 aggregated metrics for the top KPI widget row
// (Active Users, Total Events, Avg Session Time, New Users) spanning the last 30 days
func (h *WidgetsHandler) GetKPISummary(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	environment := c.Query("environment", "live")

	// Default 30 days window
	now := time.Now().UTC()
	startDate := now.AddDate(0, 0, -30).Format("2006-01-02 15:04:05")
	endDate := now.Format("2006-01-02 15:04:05")

	// System events are ignored by default in the Dashboard KPI
	baseWhere := "project_id = ? AND environment = ? AND timestamp >= ? AND timestamp <= ? AND event_name NOT LIKE '$%'"

	// 1. Total Active Users (Count Distinct)
	var activeUsers uint64
	queryUsers := fmt.Sprintf(`SELECT COUNT(DISTINCT distinct_id) FROM events WHERE %s`, baseWhere)
	if err := h.CH.QueryRow(context.Background(), queryUsers, project.ID, environment, startDate, endDate).Scan(&activeUsers); err != nil {
		activeUsers = 0
	}

	// 2. Total Events
	var totalEvents uint64
	queryEvents := fmt.Sprintf(`SELECT COUNT(1) FROM events WHERE %s`, baseWhere)
	if err := h.CH.QueryRow(context.Background(), queryEvents, project.ID, environment, startDate, endDate).Scan(&totalEvents); err != nil {
		totalEvents = 0
	}

	// 3. New Users (First-time seen in this period)
	var newUsers uint64
	queryNew := `
		SELECT COUNT(DISTINCT distinct_id)
		FROM (
			SELECT distinct_id, MIN(timestamp) as first_seen
			FROM events
			WHERE project_id = ? AND environment = ? AND event_name NOT LIKE '$%'
			GROUP BY distinct_id
			HAVING first_seen >= ? AND first_seen <= ?
		)
	`
	if err := h.CH.QueryRow(context.Background(), queryNew, project.ID, environment, startDate, endDate).Scan(&newUsers); err != nil {
		newUsers = 0
	}

	// 4. Avg Session Time (in seconds)
	// We calculate the diff between max and min timestamp per session, then avg it
	var avgSessionFloat float64
	querySession := fmt.Sprintf(`
		SELECT avg(session_duration) FROM (
			SELECT mapContains(properties, '$session_id') ? properties['$session_id'] : 'unknown' as session_id,
				   dateDiff('second', MIN(timestamp), MAX(timestamp)) as session_duration
			FROM events
			WHERE %s AND mapContains(properties, '$session_id') AND properties['$session_id'] != 'unknown'
			GROUP BY session_id
			HAVING session_duration > 0
		)
	`, baseWhere)
	if err := h.CH.QueryRow(context.Background(), querySession, project.ID, environment, startDate, endDate).Scan(&avgSessionFloat); err != nil {
		avgSessionFloat = 0
	}

	// Format Session Time into string (e.g., "4m 32s")
	totalSeconds := int(avgSessionFloat)
	minutes := totalSeconds / 60
	seconds := totalSeconds % 60
	sessionTimeFmt := fmt.Sprintf("%dm %ds", minutes, seconds)

	return c.JSON(fiber.Map{
		"active_users":     activeUsers,
		"total_events":     totalEvents,
		"new_users":        newUsers,
		"avg_session_time": sessionTimeFmt,
		"period":           "30d",
	})
}

// GetActiveVisitorsSeries returns time-series data of DAU (Daily Active Users) for the last 30 days
func (h *WidgetsHandler) GetActiveVisitorsSeries(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	environment := c.Query("environment", "live")

	// Default 30 days window
	now := time.Now().UTC()
	startDate := now.AddDate(0, 0, -30).Format("2006-01-02 15:04:05")

	ctx := context.Background()

	query := `
		SELECT 
			toStartOfDay(timestamp) as day,
			uniqExact(distinct_id) as active_users
		FROM events
		WHERE project_id = ? 
		  AND environment = ?
		  AND timestamp >= ?
		  AND event_name NOT LIKE '$%'
		GROUP BY day
		ORDER BY day ASC
	`

	rows, err := h.CH.Query(ctx, query, project.ID, environment, startDate)
	if err != nil {
		fmt.Println("Error querying DAU time-series:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to query active users series"})
	}
	defer rows.Close()

	type TimeSeriesPoint struct {
		Date  string `json:"date"`
		Users uint64 `json:"users"`
	}

	var series []TimeSeriesPoint

	for rows.Next() {
		var day time.Time
		var users uint64
		if err := rows.Scan(&day, &users); err != nil {
			continue
		}
		series = append(series, TimeSeriesPoint{
			Date:  day.Format("2006-01-02"), // Format as YYYY-MM-DD
			Users: users,
		})
	}

	return c.JSON(fiber.Map{"series": series})
}

// GetTopEvents returns the top 10 most frequent custom events over the last 30 days
func (h *WidgetsHandler) GetTopEvents(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	environment := c.Query("environment", "live")

	// Default 30 days window
	now := time.Now().UTC()
	startDate := now.AddDate(0, 0, -30).Format("2006-01-02 15:04:05")

	ctx := context.Background()

	query := `
		SELECT 
			event_name,
			count(*) as total_occurrences,
			uniqExact(distinct_id) as unique_users
		FROM events
		WHERE project_id = ? 
		  AND environment = ?
		  AND timestamp >= ?
		  AND event_name NOT LIKE '$%'
		GROUP BY event_name
		ORDER BY total_occurrences DESC
		LIMIT 10
	`

	rows, err := h.CH.Query(ctx, query, project.ID, environment, startDate)
	if err != nil {
		fmt.Println("Error querying top events:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to query top events"})
	}
	defer rows.Close()

	type TopEvent struct {
		EventName   string `json:"event_name"`
		TotalVolume uint64 `json:"total_volume"`
		UniqueUsers uint64 `json:"unique_users"`
	}

	var events []TopEvent

	for rows.Next() {
		var name string
		var volume, users uint64
		if err := rows.Scan(&name, &volume, &users); err != nil {
			continue
		}
		events = append(events, TopEvent{
			EventName:   name,
			TotalVolume: volume,
			UniqueUsers: users,
		})
	}

	return c.JSON(fiber.Map{"events": events})
}

// GetGeographicBreakdown returns the top 5 countries by event volume over the last 30 days
func (h *WidgetsHandler) GetGeographicBreakdown(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	environment := c.Query("environment", "live")
	now := time.Now().UTC()
	startDate := now.AddDate(0, 0, -30).Format("2006-01-02 15:04:05")

	ctx := context.Background()

	// Using the geo.country property. If missing, it's grouped as empty string
	query := `
		SELECT 
			JSONExtractString(properties, '$geo', 'country') as country,
			count(*) as value
		FROM events
		WHERE project_id = ? 
		  AND environment = ?
		  AND timestamp >= ?
		  AND JSONHas(properties, '$geo') = 1
		GROUP BY country
		ORDER BY value DESC
		LIMIT 5
	`

	rows, err := h.CH.Query(ctx, query, project.ID, environment, startDate)
	if err != nil {
		fmt.Println("Error querying geographic breakdown:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to query geographic data"})
	}
	defer rows.Close()

	type BreakdownPoint struct {
		Label string `json:"label"`
		Value uint64 `json:"value"`
	}
	var data []BreakdownPoint

	for rows.Next() {
		var country string
		var value uint64
		if err := rows.Scan(&country, &value); err != nil {
			continue
		}
		if country == "" {
			country = "Unknown"
		}
		data = append(data, BreakdownPoint{
			Label: country,
			Value: value,
		})
	}

	return c.JSON(fiber.Map{"data": data})
}

// GetDeviceBreakdown returns the top OS usage breakdown over the last 30 days
func (h *WidgetsHandler) GetDeviceBreakdown(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	environment := c.Query("environment", "live")
	now := time.Now().UTC()
	startDate := now.AddDate(0, 0, -30).Format("2006-01-02 15:04:05")

	ctx := context.Background()

	query := `
		SELECT 
			JSONExtractString(properties, '$device', 'os') as os,
			count(*) as value
		FROM events
		WHERE project_id = ? 
		  AND environment = ?
		  AND timestamp >= ?
		  AND JSONHas(properties, '$device') = 1
		GROUP BY os
		ORDER BY value DESC
		LIMIT 5
	`

	rows, err := h.CH.Query(ctx, query, project.ID, environment, startDate)
	if err != nil {
		fmt.Println("Error querying device breakdown:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to query device data"})
	}
	defer rows.Close()

	type BreakdownPoint struct {
		Label string `json:"label"`
		Value uint64 `json:"value"`
	}
	var data []BreakdownPoint

	for rows.Next() {
		var os string
		var value uint64
		if err := rows.Scan(&os, &value); err != nil {
			continue
		}
		if os == "" {
			os = "Unknown"
		}
		data = append(data, BreakdownPoint{
			Label: os,
			Value: value,
		})
	}

	return c.JSON(fiber.Map{"data": data})
}

// GetTopUsers returns the top 5 most active distinct_ids over the last 30 days
func (h *WidgetsHandler) GetTopUsers(c *fiber.Ctx) error {
	userID := c.Locals("user_id").(string)
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	environment := c.Query("environment", "live")
	now := time.Now().UTC()
	startDate := now.AddDate(0, 0, -30).Format("2006-01-02 15:04:05")

	ctx := context.Background()

	query := `
		SELECT 
			distinct_id,
			count(*) as total_events
		FROM events
		WHERE project_id = ? 
		  AND environment = ?
		  AND timestamp >= ?
		  AND event_name NOT LIKE '$%'
		GROUP BY distinct_id
		ORDER BY total_events DESC
		LIMIT 5
	`

	rows, err := h.CH.Query(ctx, query, project.ID, environment, startDate)
	if err != nil {
		fmt.Println("Error querying top users:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to query top users"})
	}
	defer rows.Close()

	type TopUser struct {
		DistinctID  string `json:"distinct_id"`
		TotalEvents uint64 `json:"total_events"`
	}

	var users []TopUser

	for rows.Next() {
		var id string
		var count uint64
		if err := rows.Scan(&id, &count); err != nil {
			continue
		}
		users = append(users, TopUser{
			DistinctID:  id,
			TotalEvents: count,
		})
	}

	return c.JSON(fiber.Map{"users": users})
}
