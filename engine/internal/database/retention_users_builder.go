package database

import (
	"fmt"
	"strings"

	"sankofa/engine/internal/models"
)

// BuildRetentionUsersQuery returns a query that yields distinct_ids for users who
// belong to a particular cohort (cohortDate = bucketed start date) and were either
// retained (action="retained") or dropped off (action="dropped") at period `periodIdx`.
//
// periodIdx 0 → all users who did the start event in that cohort bucket.
// periodIdx N → users who retained (i.e. did return event N intervals later).
// action="dropped" + periodIdx N → users who did NOT retain on period N.
func BuildRetentionUsersQuery(req models.RetentionRequest, cohortDate string, periodIdx int, action string) (string, []any) {
	var args []any

	timeBucket := "toStartOfDay"
	chIntervalUnit := "day"
	switch req.Interval {
	case "week":
		timeBucket = "toStartOfWeek"
		chIntervalUnit = "week"
	case "month":
		timeBucket = "toStartOfMonth"
		chIntervalUnit = "month"
	case "hour":
		timeBucket = "toStartOfHour"
		chIntervalUnit = "hour"
	}

	// WHERE clause for project + global filters
	whereStmt := "project_id = ? AND environment = ?"
	args = append(args, req.ProjectID, req.Environment)
	if len(req.GlobalFilters) > 0 {
		gfStmt, gfArgs := buildFilterConds(req.GlobalFilters)
		if gfStmt != "" {
			whereStmt += " AND " + gfStmt
			args = append(args, gfArgs...)
		}
	}

	// Build start/return event conditions
	startCond := buildEventCond(req.ExpandedStartEvent, req.StartEvent)
	returnCond := buildEventCond(req.ExpandedReturnEvent, req.ReturnEvent)

	// Inner query: all users who did the start event in the full date range, grouped to their start bucket
	innerWhereStmt := whereStmt + " AND " + startCond
	innerArgs := make([]any, len(args))
	copy(innerArgs, args)
	if !req.GlobalDateRange.Start.IsZero() && !req.GlobalDateRange.End.IsZero() {
		innerWhereStmt += " AND timestamp >= ? AND timestamp <= ?"
		innerArgs = append(innerArgs, req.GlobalDateRange.Start, req.GlobalDateRange.End)
	}

	innerQuery := fmt.Sprintf(
		`SELECT distinct_id, min(%s(timestamp)) AS user_start_date
		 FROM events
		 WHERE %s
		 GROUP BY distinct_id`,
		timeBucket, innerWhereStmt,
	)

	// We need the full event range for the outer join (not just the cohort date range)
	outerWhere := strings.ReplaceAll(whereStmt, "project_id = ?", "e.project_id = ?")

	// All join args = innerArgs (for the inner query) + outer args (for the outer WHERE)
	joinArgs := append(innerArgs, args...)

	// Filter to the specific cohort date
	// parseDateTimeBestEffort handles full timestamp or ISO strings properly.
	cohortFilter := fmt.Sprintf("%s(u.user_start_date) = %s(parseDateTimeBestEffort('%s'))", timeBucket, timeBucket, cohortDate)

	if periodIdx == 0 {
		// Users who did the start event in this cohort bucket
		q := fmt.Sprintf(
			`SELECT DISTINCT u.distinct_id AS distinct_id
			 FROM events e
			 INNER JOIN (%s) u ON e.distinct_id = u.distinct_id
			 WHERE %s AND %s`,
			innerQuery,
			outerWhere,
			cohortFilter,
		)
		return q, joinArgs
	}

	// Period N: users retained = did return event exactly N intervals after their start date
	retainedSubQ := fmt.Sprintf(
		`SELECT DISTINCT u.distinct_id AS distinct_id
		 FROM events e
		 INNER JOIN (%s) u ON e.distinct_id = u.distinct_id
		 WHERE %s AND %s AND %s AND dateDiff('%s', u.user_start_date, %s(e.timestamp)) = %d`,
		innerQuery,
		outerWhere,
		cohortFilter,
		strings.ReplaceAll(returnCond, "event_name", "e.event_name"),
		chIntervalUnit,
		timeBucket,
		periodIdx,
	)

	if action == "retained" {
		return retainedSubQ, joinArgs
	}

	// Dropped = in cohort but NOT in retained
	startedSubQ := fmt.Sprintf(
		`SELECT DISTINCT u.distinct_id AS distinct_id
		 FROM events e
		 INNER JOIN (%s) u ON e.distinct_id = u.distinct_id
		 WHERE %s AND %s`,
		innerQuery,
		outerWhere,
		cohortFilter,
	)

	droppedQ := fmt.Sprintf(
		`SELECT distinct_id FROM (%s) WHERE distinct_id NOT IN (%s)`,
		startedSubQ,
		retainedSubQ,
	)

	// args for dropped = joinArgs (started) + joinArgs (retained) 
	droppedArgs := append([]any{}, joinArgs...)
	droppedArgs = append(droppedArgs, joinArgs...)
	return droppedQ, droppedArgs
}

func buildEventCond(expanded []string, fallback string) string {
	names := expanded
	if len(names) == 0 {
		names = []string{fallback}
	}
	if len(names) == 1 {
		return fmt.Sprintf("event_name = '%s'", escapeString(names[0]))
	}
	escaped := make([]string, len(names))
	for i, n := range names {
		escaped[i] = fmt.Sprintf("'%s'", escapeString(n))
	}
	return fmt.Sprintf("event_name IN (%s)", strings.Join(escaped, ", "))
}
