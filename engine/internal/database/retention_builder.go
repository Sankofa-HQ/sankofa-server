package database

import (
	"fmt"
	"strings"

	"sankofa/engine/internal/models"
)

// BuildRetentionQuery constructs a ClickHouse SQL query using the retention() aggregate function.
func BuildRetentionQuery(req models.RetentionRequest) (string, []any) {
	var args []any

	// ── 1. Determine the time bucketing function ──
	timeBucket := "toStartOfDay"
	switch req.Interval {
	case "week":
		timeBucket = "toStartOfWeek"
	case "month":
		timeBucket = "toStartOfMonth"
	case "hour":
		timeBucket = "toStartOfHour"
	default:
		timeBucket = "toStartOfDay"
	}

	// ── 2. Build global WHERE clause ──
	whereStmt := "project_id = ?"
	args = append(args, req.ProjectID)

	if len(req.GlobalFilters) > 0 {
		globalFilterStmt, globalFilterArgs := buildFilterConds(req.GlobalFilters)
		if globalFilterStmt != "" {
			whereStmt += " AND " + globalFilterStmt
			args = append(args, globalFilterArgs...)
		}
	}

	// ── 3. Build breakdown selects ──
	var breakdownSelects []string
	var breakdownAliases []string
	for i, bd := range req.Breakdowns {
		alias := fmt.Sprintf("breakdown_%d", i)
		var expandedKeys []string
		if i < len(req.ExpandedBreakdowns) && len(req.ExpandedBreakdowns[i]) > 0 {
			expandedKeys = req.ExpandedBreakdowns[i]
		} else {
			expandedKeys = []string{bd} // fallback
		}
		breakdownSelects = append(breakdownSelects, fmt.Sprintf("%s AS %s", BuildBreakdownSQL(expandedKeys), alias))
		breakdownAliases = append(breakdownAliases, alias)
	}

	// ── 4. Build Event MATCH conditions ──
	var startCond string
	if len(req.ExpandedStartEvent) > 1 {
		startEscaped := make([]string, len(req.ExpandedStartEvent))
		for i, ev := range req.ExpandedStartEvent {
			startEscaped[i] = fmt.Sprintf("'%s'", escapeString(ev))
		}
		startCond = fmt.Sprintf("event_name IN (%s)", strings.Join(startEscaped, ", "))
	} else {
		startName := req.StartEvent
		if len(req.ExpandedStartEvent) == 1 {
			startName = req.ExpandedStartEvent[0]
		}
		startCond = fmt.Sprintf("event_name = '%s'", escapeString(startName))
	}

	var returnCond string
	if len(req.ExpandedReturnEvent) > 1 {
		returnEscaped := make([]string, len(req.ExpandedReturnEvent))
		for i, ev := range req.ExpandedReturnEvent {
			returnEscaped[i] = fmt.Sprintf("'%s'", escapeString(ev))
		}
		returnCond = fmt.Sprintf("event_name IN (%s)", strings.Join(returnEscaped, ", "))
	} else {
		returnName := req.ReturnEvent
		if len(req.ExpandedReturnEvent) == 1 {
			returnName = req.ExpandedReturnEvent[0]
		}
		returnCond = fmt.Sprintf("event_name = '%s'", escapeString(returnName))
	}

	// ── 5. Build retention() argument conditions ──
	// ClickHouse syntax: retention(cond1, cond2, cond3...)
	// cond1 is "Did they do the start event?"
	// cond2 is "Did they do the return event 1 interval later?"
	// cond3 is "Did they do the return event 2 intervals later?"
	var retentionConds []string

	// Example interval mapping to ClickHouse dateDiff units
	chIntervalUnit := "day"
	switch req.Interval {
	case "week":
		chIntervalUnit = "week"
	case "month":
		chIntervalUnit = "month"
	case "hour":
		chIntervalUnit = "hour"
	}

	// Condition 1: Did the start event occur?
	// (Note: in the inner query we'll define 'start_date' for the distinct_id)
	retentionConds = append(retentionConds, startCond)

	// Conditions 2...N: Return event occurred exactly N intervals after start_date
	// Since we aggregate over the users, the inner query checks events per user.
	// ClickHouse's retention() evaluates if each condition was met *at least once* for the row (grouped by user).
	for i := 1; i <= req.TimeWindow; i++ {
		// Event must be the RETURN event, AND the time diff from start must be EXACTLY `i` intervals.
		// dateDiff returns the difference between boundaries.
		// Wait, ClickHouse retention() evaluates these sequentially over time automatically if sorted,
		// but typically you give it the boolean conditions.
		// A standard way in ClickHouse is:
		// cond1 = (event_name = 'Start')
		// cond2 = (event_name = 'Return' AND dateDiff('day', min_start_date_for_user, timestamp) = 1)

		cond := fmt.Sprintf("(%s AND dateDiff('%s', user_start_date, timestamp) = %d)", returnCond, chIntervalUnit, i)
		retentionConds = append(retentionConds, cond)
	}

	// Wait, to use `user_start_date` inside retention(), we need an inner query that calculates
	// min(timestamp) AS user_start_date grouped by distinct_id.
	// Let's structure the queries:

	// INNER QUERY: Get user start dates
	innerWhereStmt := whereStmt + " AND " + startCond
	var innerArgs []any
	innerArgs = append(innerArgs, args...)

	innerQuery := fmt.Sprintf(`SELECT distinct_id, min(%s(timestamp)) AS user_start_date 
        FROM events 
        WHERE %s 
        GROUP BY distinct_id`, timeBucket, innerWhereStmt)

	// MIDDLE QUERY: Join events with user_start_dates and evaluate conditions
	// We need breakdowns here too!
	midSelects := []string{
		"e.distinct_id",
		"u.user_start_date",
		"e.timestamp",
		startCond + " AS cond_start",
	}

	for i := 1; i <= req.TimeWindow; i++ {
		midSelects = append(midSelects, fmt.Sprintf("(%s AND dateDiff('%s', u.user_start_date, e.timestamp) = %d) AS cond_%d", returnCond, chIntervalUnit, i, i))
	}

	// Also pull breakdowns if any
	// Note: Breakdowns usually apply to the properties of the START event.
	// To do this accurately, we should pull breakdowns from the inner query where we find the start event!

	// LET'S REWRITE to ensure accurate property extraction for breakdowns from the start event.
	innerSelectsWithBreakdowns := []string{"distinct_id", fmt.Sprintf("min(%s(timestamp)) AS user_start_date", timeBucket)}
	innerGroupBys := []string{"distinct_id"}

	for _, alias := range breakdownAliases {
		innerGroupBys = append(innerGroupBys, alias)
	}

	if len(breakdownSelects) > 0 {
		innerSelectsWithBreakdowns = append(innerSelectsWithBreakdowns, breakdownSelects...)
	}

	innerQuery = fmt.Sprintf(`SELECT %s
        FROM events 
        WHERE %s 
        GROUP BY %s`, strings.Join(innerSelectsWithBreakdowns, ", "), innerWhereStmt, strings.Join(innerGroupBys, ", "))

	// Better Middle Query using retention()
	// Actually, retention() aggregates boolean arrays.
	// retention(cond1, cond2...) -> [1, 1, 0]
	// If grouped by (user_start_date(cohort), breakdown), we get the cohort tables.

	// Let's just do it directly with a Join inside:
	// SELECT cohort_date, retention(cond1, cond2...) as r FROM (
	//   SELECT distinct_id, user_start_date AS cohort_date, e.timestamp, e.event_name ...
	//   FROM events e JOIN (SELECT distinct_id, min(...) user_start_date, breakdowns... FROM events WHERE start_event) u
	//   ON e.distinct_id = u.distinct_id
	// ) GROUP BY cohort_date

	// However, building this efficiently:
	midJoinArgs := append(innerArgs, args...) // inner args for left side, global args for right side

	midQuery := fmt.Sprintf(`SELECT
    u.user_start_date AS cohort_date,
    %s
    retention(
        %s,
        %s
    ) AS retention_array
FROM events e
INNER JOIN (
    %s
) u ON e.distinct_id = u.distinct_id
WHERE %s`,
		strings.Join(append(breakdownAliases, ""), ",\n    "),
		"e.event_name = '"+escapeString(req.StartEvent)+"'", // Simplified start cond syntax here since we joined on those users anyway.
		strings.Join(retentionConds[1:], ",\n        "),
		innerQuery,
		strings.ReplaceAll(whereStmt, "project_id", "e.project_id"),
	)

	// Since we mapped breakdowns to u.breakdown_N, we need to select them properly in midQuery
	var midSelectBreakdowns []string
	for _, alias := range breakdownAliases {
		midSelectBreakdowns = append(midSelectBreakdowns, fmt.Sprintf("u.%s AS %s", alias, alias))
	}
	midBreakdownStr := ""
	if len(midSelectBreakdowns) > 0 {
		midBreakdownStr = strings.Join(midSelectBreakdowns, ",\n    ") + ","
	}

	midGroupBys := []string{"e.distinct_id", "u.user_start_date"}
	for _, alias := range breakdownAliases {
		midGroupBys = append(midGroupBys, "u."+alias)
	}

	// Apply correct event condition names to middle query based on startCond definition above
	startCondMid := strings.ReplaceAll(startCond, "event_name", "e.event_name")
	returnCondMid := strings.ReplaceAll(returnCond, "event_name", "e.event_name")

	var finalRetentionConds []string
	finalRetentionConds = append(finalRetentionConds, startCondMid)
	for i := 1; i <= req.TimeWindow; i++ {
		finalRetentionConds = append(finalRetentionConds, fmt.Sprintf("(%s AND dateDiff('%s', u.user_start_date, %s(e.timestamp)) = %d)", returnCondMid, chIntervalUnit, timeBucket, i))
	}

	midQuery = fmt.Sprintf(`SELECT
    u.user_start_date AS cohort_date,
    %s
    retention(
        %s
    ) AS ret_arr
FROM events e
INNER JOIN (
    %s
) u ON e.distinct_id = u.distinct_id
WHERE %s
GROUP BY %s`,
		midBreakdownStr,
		strings.Join(finalRetentionConds, ",\n        "),
		innerQuery,
		strings.ReplaceAll(whereStmt, "project_id = ?", "e.project_id = ?"),
		strings.Join(midGroupBys, ", "),
	)

	// Outer Query: sum the arrays across the cohort to get total retained users.
	var outerSelects []string
	outerSelects = append(outerSelects, "cohort_date")
	outerSelects = append(outerSelects, breakdownAliases...)

	var outerGroupBys []string
	outerGroupBys = append(outerGroupBys, "cohort_date")
	outerGroupBys = append(outerGroupBys, breakdownAliases...)

	var orderBys []string
	orderBys = append(orderBys, "cohort_date ASC")
	orderBys = append(orderBys, breakdownAliases...)

	finalQuery := fmt.Sprintf(`SELECT
    %s,
    sumArray(ret_arr) AS retention_array
FROM (
%s
)
GROUP BY %s
ORDER BY %s`,
		strings.Join(outerSelects, ",\n    "),
		midQuery,
		strings.Join(outerGroupBys, ", "),
		strings.Join(orderBys, ", "),
	)

	return finalQuery, midJoinArgs
}
