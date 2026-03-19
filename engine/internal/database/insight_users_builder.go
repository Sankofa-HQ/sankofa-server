package database

import (
	"fmt"
	"strings"
	"time"

	"sankofa/engine/internal/models"
)

// BuildInsightUsersQuery constructs a ClickHouse SQL query that returns distinct_ids 
// for a specific metric segment and time bucket of an Insight.
func BuildInsightUsersQuery(req models.InsightRequest, targetMetric int, targetTimeBucket time.Time, segment string) (string, []any) {
	var args []any

	if targetMetric < 0 || targetMetric >= len(req.Metrics) {
		return "", nil
	}
	metric := req.Metrics[targetMetric]

	// ── Determine the time bucketing function ──
	timeBucketFunc := "toStartOfDay"
	switch req.Interval {
	case "week":
		timeBucketFunc = "toStartOfWeek"
	case "month":
		timeBucketFunc = "toStartOfMonth"
	case "hour":
		timeBucketFunc = "toStartOfHour"
	default:
		timeBucketFunc = "toStartOfDay"
	}

	// ── Build basic WHERE clause ──
	whereStmt := "project_id = ? AND environment = ?"
	args = append(args, req.ProjectID, req.Environment)

	// Filter by the time bucket
	whereStmt += fmt.Sprintf(" AND %s(timestamp, '%s') = ?", timeBucketFunc, req.Timezone)
	args = append(args, targetTimeBucket)

	// Global filters
	if len(req.GlobalFilters) > 0 {
		globalFilterStmt, globalFilterArgs := buildFilterConds(req.GlobalFilters)
		if globalFilterStmt != "" {
			whereStmt += " AND " + globalFilterStmt
			args = append(args, globalFilterArgs...)
		}
	}

	// Metric event matching
	var eventCond string
	if len(metric.ExpandedEvents) > 1 {
		var escaped []string
		for _, ev := range metric.ExpandedEvents {
			escaped = append(escaped, fmt.Sprintf("'%s'", escapeString(ev)))
		}
		eventCond = fmt.Sprintf("event_name IN (%s)", strings.Join(escaped, ", "))
	} else {
		eventName := metric.EventName
		if len(metric.ExpandedEvents) == 1 {
			eventName = metric.ExpandedEvents[0]
		}
		eventCond = fmt.Sprintf("event_name = '%s'", escapeString(eventName))
	}
	whereStmt += " AND " + eventCond

	// Per-metric filters
	if len(metric.Filters) > 0 {
		filterStmt, filterArgs := buildFilterConds(metric.Filters)
		if filterStmt != "" {
			whereStmt += " AND " + filterStmt
			args = append(args, filterArgs...)
		}
	}

	// Breakdown / Segment filtering
	if segment != "" && segment != "Overall" && len(req.Breakdowns) > 0 {
		var bdParts []string
		for i, bd := range req.Breakdowns {
			var expandedKeys []string
			if i < len(req.ExpandedBreakdowns) && len(req.ExpandedBreakdowns[i]) > 0 {
				expandedKeys = req.ExpandedBreakdowns[i]
			} else {
				expandedKeys = []string{bd}
			}
			bdParts = append(bdParts, BuildBreakdownSQL(expandedKeys))
		}
		// ClickHouse concat with ' · ' delimiter
		concatExpr := "concat(" + strings.Join(bdParts, ", ' · '") + ")"
		whereStmt += fmt.Sprintf(" AND %s = ?", concatExpr)
		args = append(args, segment)
	}

	query := fmt.Sprintf(`SELECT DISTINCT distinct_id FROM events WHERE %s LIMIT 1000`, whereStmt)

	return query, args
}
