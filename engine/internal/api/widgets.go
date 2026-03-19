package api

import (
	"context"
	"fmt"
	"time"

	"sankofa/engine/internal/database"
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

func (h *WidgetsHandler) RegisterRoutes(router fiber.Router, middlewares ...fiber.Handler) {
	w := router.Group("/widgets", middlewares...)
	w.Get("/kpi-summary", h.GetKPISummary)
	w.Get("/active-visitors", h.GetActiveVisitorsSeries)
	w.Get("/top-events", h.GetTopEvents)
	w.Get("/geo-breakdown", h.GetGeographicBreakdown)
	w.Get("/device-breakdown", h.GetDeviceBreakdown)
	w.Get("/browser-breakdown", h.GetBrowserBreakdown)
	w.Get("/platform-breakdown", h.GetPlatformBreakdown)
	w.Get("/top-users", h.GetTopUsers)
	w.Get("/active-users-today", h.GetActiveUsersToday)
}

// ─── Date Range Helper ───
// Reads "period" query param and returns (startDate, endDate, periodLabel).
// Supported values: 7d, 14d, 30d, 90d, today, yesterday, this_week, this_month
// Defaults to 30d if not specified.
func getWidgetDateRange(c *fiber.Ctx) (string, string, string) {
	period := c.Query("period", "30d")
	now := time.Now().UTC()

	var start, end time.Time
	end = now

	switch period {
	case "today":
		start = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	case "yesterday":
		yesterday := now.AddDate(0, 0, -1)
		start = time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)
		end = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	case "this_week":
		weekday := int(now.Weekday())
		if weekday == 0 {
			weekday = 7
		}
		start = time.Date(now.Year(), now.Month(), now.Day()-weekday+1, 0, 0, 0, 0, time.UTC)
	case "this_month":
		start = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	case "7d":
		start = now.AddDate(0, 0, -7)
	case "14d":
		start = now.AddDate(0, 0, -14)
	case "90d":
		start = now.AddDate(0, 0, -90)
	default: // "30d"
		period = "30d"
		start = now.AddDate(0, 0, -30)
	}

	return start.Format("2006-01-02 15:04:05"), end.Format("2006-01-02 15:04:05"), period
}

func (h *WidgetsHandler) GetActiveUsersToday(c *fiber.Ctx) error {
	// Extract project from context (populated by middleware)
	project, ok := c.Locals("project").(database.Project)
	if !ok {
		// Fallback to manual check if middleware wasn't used
		userID, okU := c.Locals("user_id").(string)
		if !okU || userID == "" {
			return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
		}
		var err error
		p, err := getProjectFromContext(c, h.DB, userID)
		if err != nil || p == nil {
			return err
		}
		project = *p
	}

	environment := c.Query("environment", "live")

	// This widget always compares today vs yesterday, regardless of date range
	now := time.Now().UTC()
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).Format("2006-01-02 15:04:05")
	yesterdayStart := now.AddDate(0, 0, -1)
	yesterdayStartFmt := time.Date(yesterdayStart.Year(), yesterdayStart.Month(), yesterdayStart.Day(), 0, 0, 0, 0, time.UTC).Format("2006-01-02 15:04:05")

	baseWhere := "project_id = ? AND environment = ? AND event_name NOT LIKE '$%'"

	var todayCount uint64
	queryToday := fmt.Sprintf(`SELECT uniqExact(distinct_id) FROM events WHERE %s AND timestamp >= ?`, baseWhere)
	if err := h.CH.QueryRow(context.Background(), queryToday, project.ID, environment, todayStart).Scan(&todayCount); err != nil {
		todayCount = 0
	}

	var yesterdayCount uint64
	queryYesterday := fmt.Sprintf(`SELECT uniqExact(distinct_id) FROM events WHERE %s AND timestamp >= ? AND timestamp < ?`, baseWhere)
	if err := h.CH.QueryRow(context.Background(), queryYesterday, project.ID, environment, yesterdayStartFmt, todayStart).Scan(&yesterdayCount); err != nil {
		yesterdayCount = 0
	}

	var pctChange float64
	if yesterdayCount > 0 {
		pctChange = (float64(todayCount) - float64(yesterdayCount)) / float64(yesterdayCount) * 100
	} else if todayCount > 0 {
		pctChange = 100
	}

	return c.JSON(fiber.Map{
		"today":             todayCount,
		"yesterday":         yesterdayCount,
		"percentage_change": pctChange,
	})
}

// GetKPISummary returns 4 aggregated metrics for the top KPI widget row
// (Active Users, Total Events, Avg Session Time, New Users) for the selected period
func (h *WidgetsHandler) GetKPISummary(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok || userID == "" {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	environment := c.Query("environment", "live")
	startDate, endDate, period := getWidgetDateRange(c)

	// System events are ignored by default in the Dashboard KPI
	baseWhere := "project_id = ? AND environment = ? AND timestamp >= ? AND timestamp <= ? AND event_name NOT LIKE '$%'"

	// 1. Total Active Users (Count Distinct from events)
	var activeUsers uint64
	queryUsers := fmt.Sprintf(`SELECT COUNT(DISTINCT distinct_id) FROM events WHERE %s`, baseWhere)
	if err := h.CH.QueryRow(context.Background(), queryUsers, project.ID, environment, startDate, endDate).Scan(&activeUsers); err != nil {
		activeUsers = 0
	}

	// 1b. Identified Users (from persons table - users with profiles)
	var identifiedUsers uint64
	queryIdentified := `SELECT COUNT(DISTINCT distinct_id) FROM persons WHERE project_id = ? AND environment = ?`
	if err := h.CH.QueryRow(context.Background(), queryIdentified, project.ID, environment).Scan(&identifiedUsers); err != nil {
		identifiedUsers = 0
	}

	// Calculate identification rate
	var identificationRate float64
	if activeUsers > 0 {
		identificationRate = float64(identifiedUsers) / float64(activeUsers) * 100
		if identificationRate > 100 {
			identificationRate = 100
		}
	}

	// 2. Total Events (deduplicated same way as events page: LIMIT 1 BY distinct_id, event_name, timestamp)
	var totalEvents uint64
	queryEvents := fmt.Sprintf(`SELECT count() FROM (SELECT distinct_id, event_name, timestamp FROM events WHERE %s LIMIT 1 BY distinct_id, event_name, timestamp)`, baseWhere)
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
			SELECT session_id,
				   dateDiff('second', MIN(timestamp), MAX(timestamp)) as session_duration
			FROM events
			WHERE %s AND session_id != '' AND session_id != 'unknown'
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
		"active_users":        activeUsers,
		"identified_users":    identifiedUsers,
		"identification_rate": identificationRate,
		"total_events":        totalEvents,
		"new_users":           newUsers,
		"avg_session_time":    sessionTimeFmt,
		"period":              period,
	})
}

// GetActiveVisitorsSeries returns time-series data of DAU for the selected period
func (h *WidgetsHandler) GetActiveVisitorsSeries(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok || userID == "" {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	environment := c.Query("environment", "live")
	startDate, _, _ := getWidgetDateRange(c)

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

// GetTopEvents returns the top 10 most frequent custom events for the selected period
func (h *WidgetsHandler) GetTopEvents(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok || userID == "" {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	environment := c.Query("environment", "live")
	startDate, _, _ := getWidgetDateRange(c)

	ctx := context.Background()

	query := `
		SELECT 
			event_name,
			count() as total_volume,
			uniqExact(distinct_id) as unique_users
		FROM events
		WHERE project_id = ? 
		  AND environment = ?
		  AND timestamp >= ?
		  AND event_name NOT LIKE '$%'
		GROUP BY event_name
		ORDER BY total_volume DESC
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
		DisplayName string `json:"display_name"`
		TotalVolume uint64 `json:"total_volume"`
		UniqueUsers uint64 `json:"unique_users"`
	}
	var events []TopEvent

	// 1. Fetch from ClickHouse
	for rows.Next() {
		var eventName string
		var totalVolume uint64
		var uniqueUsers uint64
		if err := rows.Scan(&eventName, &totalVolume, &uniqueUsers); err != nil {
			continue
		}
		events = append(events, TopEvent{
			EventName:   eventName,
			DisplayName: eventName, // Default to raw name
			TotalVolume: totalVolume,
			UniqueUsers: uniqueUsers,
		})
	}

	// 2. Enrich with Lexicon Display Names from SQL
	if len(events) > 0 {
		var names []string
		for _, e := range events {
			names = append(names, e.EventName)
		}

		var lexiconEntries []database.LexiconEvent
		err := h.DB.Where("project_id = ? AND environment = ? AND name IN ?", project.ID, environment, names).Find(&lexiconEntries).Error
		if err == nil && len(lexiconEntries) > 0 {
			nameMap := make(map[string]string)
			for _, entry := range lexiconEntries {
				if entry.DisplayName != "" {
					nameMap[entry.Name] = entry.DisplayName
				}
			}
			for i := range events {
				if disp, ok := nameMap[events[i].EventName]; ok {
					events[i].DisplayName = disp
				}
			}
		}
	}

	return c.JSON(fiber.Map{"events": events})
}

// GetGeographicBreakdown returns the top 5 countries by event volume for the selected period
func (h *WidgetsHandler) GetGeographicBreakdown(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok || userID == "" {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	environment := c.Query("environment", "live")
	startDate, _, _ := getWidgetDateRange(c)

	ctx := context.Background()

	// Using the geo.country property. If missing, it's grouped as empty string
	query := `
		SELECT 
			if(country != '', country, 'Unknown') as geo_country,
			uniqExact(distinct_id) as value
		FROM events
		WHERE project_id = ? 
		  AND environment = ?
		  AND timestamp >= ?
		  AND event_name NOT LIKE '$%'
		GROUP BY geo_country
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

// GetDeviceBreakdown returns the top OS usage breakdown for the selected period
func (h *WidgetsHandler) GetDeviceBreakdown(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok || userID == "" {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	environment := c.Query("environment", "live")
	startDate, _, _ := getWidgetDateRange(c)

	ctx := context.Background()

	query := `
		SELECT 
			if(os != '', os, 'Unknown') as os_name,
			uniqExact(distinct_id) as value
		FROM events
		WHERE project_id = ? 
		  AND environment = ?
		  AND timestamp >= ?
		  AND event_name NOT LIKE '$%'
		GROUP BY os_name
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
		var osName string
		var value uint64
		if err := rows.Scan(&osName, &value); err != nil {
			continue
		}
		if osName == "" {
			osName = "Unknown"
		}
		data = append(data, BreakdownPoint{
			Label: osName,
			Value: value,
		})
	}

	return c.JSON(fiber.Map{"data": data})
}

// GetBrowserBreakdown returns the top browser breakdown for the selected period
func (h *WidgetsHandler) GetBrowserBreakdown(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok || userID == "" {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	environment := c.Query("environment", "live")
	startDate, _, _ := getWidgetDateRange(c)

	ctx := context.Background()

	query := `
		SELECT 
			if(browser != '', browser, 'Unknown') as browser,
			uniqExact(distinct_id) as value
		FROM events
		WHERE project_id = ? 
		  AND environment = ?
		  AND timestamp >= ?
		  AND event_name NOT LIKE '$%'
		GROUP BY browser
		ORDER BY value DESC
		LIMIT 5
	`

	rows, err := h.CH.Query(ctx, query, project.ID, environment, startDate)
	if err != nil {
		fmt.Println("Error querying browser breakdown:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to query browser data"})
	}
	defer rows.Close()

	type BreakdownPoint struct {
		Label string `json:"label"`
		Value uint64 `json:"value"`
	}
	var data []BreakdownPoint

	for rows.Next() {
		var browser string
		var value uint64
		if err := rows.Scan(&browser, &value); err != nil {
			continue
		}
		if browser == "" {
			browser = "Unknown"
		}
		data = append(data, BreakdownPoint{
			Label: browser,
			Value: value,
		})
	}

	return c.JSON(fiber.Map{"data": data})
}

// GetPlatformBreakdown returns the SDK lib/platform breakdown for the selected period
func (h *WidgetsHandler) GetPlatformBreakdown(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok || userID == "" {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	environment := c.Query("environment", "live")
	startDate, _, _ := getWidgetDateRange(c)

	ctx := context.Background()

	query := `
		SELECT 
			if(mapContains(default_properties, '$lib'), default_properties['$lib'],
			   if(mapContains(properties, '$lib'), properties['$lib'], 'Unknown')
			) as platform,
			uniqExact(distinct_id) as value
		FROM events
		WHERE project_id = ? 
		  AND environment = ?
		  AND timestamp >= ?
		  AND event_name NOT LIKE '$%'
		GROUP BY platform
		ORDER BY value DESC
		LIMIT 5
	`

	rows, err := h.CH.Query(ctx, query, project.ID, environment, startDate)
	if err != nil {
		fmt.Println("Error querying platform breakdown:", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to query platform data"})
	}
	defer rows.Close()

	type BreakdownPoint struct {
		Label string `json:"label"`
		Value uint64 `json:"value"`
	}
	var data []BreakdownPoint

	for rows.Next() {
		var platform string
		var value uint64
		if err := rows.Scan(&platform, &value); err != nil {
			continue
		}
		if platform == "" {
			platform = "Unknown"
		}
		data = append(data, BreakdownPoint{
			Label: platform,
			Value: value,
		})
	}

	return c.JSON(fiber.Map{"data": data})
}

// GetTopUsers returns the top 5 most active distinct_ids for the selected period
func (h *WidgetsHandler) GetTopUsers(c *fiber.Ctx) error {
	userID, ok := c.Locals("user_id").(string)
	if !ok || userID == "" {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}
	project, err := getProjectFromContext(c, h.DB, userID)
	if err != nil {
		return err
	}

	environment := c.Query("environment", "live")
	startDate, _, _ := getWidgetDateRange(c)

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
		Avatar      string `json:"avatar"`
		Name        string `json:"name"`
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

	// 2. Fetch avatars and names for these top users
	if len(users) > 0 {
		userIDs := make([]string, len(users))
		for i, u := range users {
			userIDs[i] = u.DistinctID
		}

		queryProps := `
			SELECT 
				distinct_id, 
				argMax(properties['$avatar'], last_seen) as avatar,
				argMax(properties['$name'], last_seen) as name
			FROM persons 
			WHERE project_id = ? AND environment = ? AND distinct_id IN (?)
			GROUP BY distinct_id
		`
		propRows, err := h.CH.Query(ctx, queryProps, project.ID, environment, userIDs)
		if err == nil {
			defer propRows.Close()
			avatarMap := make(map[string]string)
			nameMap := make(map[string]string)
			for propRows.Next() {
				var id, avatar, name string
				if err := propRows.Scan(&id, &avatar, &name); err == nil {
					avatarMap[id] = avatar
					nameMap[id] = name
				}
			}

			for i := range users {
				if avatar, ok := avatarMap[users[i].DistinctID]; ok {
					users[i].Avatar = avatar
				}
				if name, ok := nameMap[users[i].DistinctID]; ok {
					users[i].Name = name
				}
			}
		}
	}

	return c.JSON(fiber.Map{"users": users})
}
