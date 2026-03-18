package database

import (
	"fmt"
	"strings"

	"sankofa/engine/internal/models"
)

// BuildInsightQuery constructs a ClickHouse SQL query for an Insight time-series.
// It produces one result set with: time_bucket, metric_index, breakdown_0..N, value
//
// Each metric produces its own row set keyed by metric_index, allowing the frontend
// to render multiple series on a single chart.
func BuildInsightQuery(req models.InsightRequest) (string, []any) {
	var args []any

	// ── Determine the time bucketing function ──
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

	// ── Build global WHERE clause ──
	whereStmt := "project_id = ? AND environment = ?"
	args = append(args, req.ProjectID, req.Environment)

	if !req.GlobalDateRange.Start.IsZero() && !req.GlobalDateRange.End.IsZero() {
		whereStmt += " AND timestamp >= ? AND timestamp <= ?"
		args = append(args, req.GlobalDateRange.Start.UTC(), req.GlobalDateRange.End.UTC())
	}

	if len(req.GlobalFilters) > 0 {
		globalFilterStmt, globalFilterArgs := buildFilterConds(req.GlobalFilters)
		if globalFilterStmt != "" {
			whereStmt += " AND " + globalFilterStmt
			args = append(args, globalFilterArgs...)
		}
	}

	// ── Build breakdown selects ──
	var breakdownSelectExprs []string
	var breakdownAliases []string
	for i, bd := range req.Breakdowns {
		alias := fmt.Sprintf("breakdown_%d", i)
		var expandedKeys []string
		if i < len(req.ExpandedBreakdowns) && len(req.ExpandedBreakdowns[i]) > 0 {
			expandedKeys = req.ExpandedBreakdowns[i]
		} else {
			expandedKeys = []string{bd}
		}
		breakdownSelectExprs = append(breakdownSelectExprs, fmt.Sprintf("%s AS %s", BuildBreakdownSQL(expandedKeys), alias))
		breakdownAliases = append(breakdownAliases, alias)
	}

	// ── Build one sub-query per metric, then UNION ALL ──
	var metricQueries []string
	// We collect all args for all sub-queries in order
	var allArgs []any

	for mi, metric := range req.Metrics {
		// Start each metric's args with a fresh copy of the base (global) args
		var metricArgs []any
		metricArgs = append(metricArgs, args...)

		// Determine event match clause
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

		// Build per-metric filter conditions
		metricWhere := whereStmt + " AND " + eventCond
		if len(metric.Filters) > 0 {
			filterStmt, filterArgs := buildFilterConds(metric.Filters)
			if filterStmt != "" {
				metricWhere += " AND " + filterStmt
				metricArgs = append(metricArgs, filterArgs...)
			}
		}

		// Determine the aggregation expression
		var aggExpr string
		switch metric.MathType {
		case "uniques":
			aggExpr = "uniqExact(distinct_id)"
		case "totals":
			aggExpr = "count(*)"
		case "property_sum":
			if metric.MathProperty != "" {
				aggExpr = fmt.Sprintf("sum(toFloat64OrZero(%s))", BuildPropertyExtractionSQL(metric.MathProperty))
			} else {
				aggExpr = "count(*)" // Fallback
			}
		case "property_avg":
			if metric.MathProperty != "" {
				aggExpr = fmt.Sprintf("avg(toFloat64OrZero(%s))", BuildPropertyExtractionSQL(metric.MathProperty))
			} else {
				aggExpr = "count(*)" // Fallback
			}
		default:
			aggExpr = "uniqExact(distinct_id)" // Default to unique users
		}

		// Build SELECT columns
		selectCols := []string{
			fmt.Sprintf("%s(timestamp, '%s') AS time_bucket", timeBucket, req.Timezone),
			fmt.Sprintf("%d AS metric_index", mi),
		}
		selectCols = append(selectCols, breakdownSelectExprs...)
		selectCols = append(selectCols, fmt.Sprintf("%s AS value", aggExpr))

		// Build GROUP BY
		groupByCols := []string{"time_bucket", "metric_index"}
		groupByCols = append(groupByCols, breakdownAliases...)

		subQ := fmt.Sprintf(`SELECT
    %s
FROM events
WHERE %s
GROUP BY %s`,
			strings.Join(selectCols, ",\n    "),
			metricWhere,
			strings.Join(groupByCols, ", "),
		)
		metricQueries = append(metricQueries, subQ)

		// Append this metric's args to the unified args list
		allArgs = append(allArgs, metricArgs...)
	}

	// ── Final UNION ALL query ──
	orderByCols := []string{"time_bucket ASC", "metric_index ASC"}
	orderByCols = append(orderByCols, breakdownAliases...)

	finalQuery := fmt.Sprintf(`SELECT * FROM (
%s
)
ORDER BY %s`,
		strings.Join(metricQueries, "\nUNION ALL\n"),
		strings.Join(orderByCols, ", "),
	)

	return finalQuery, allArgs
}
