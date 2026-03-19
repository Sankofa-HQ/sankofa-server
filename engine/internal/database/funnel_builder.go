package database

import (
	"fmt"
	"strings"

	"sankofa/engine/internal/models"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// BuildBreakdownSQL generates the SQL expression for a breakdown column.
// If only one key is provided it delegates to BuildPropertyExtractionSQL.
// If multiple keys are provided (merged/virtual property) it uses COALESCE so the
// first non-empty child value is used as the segment label.
func BuildBreakdownSQL(expandedKeys []string) string {
	if len(expandedKeys) == 0 {
		return "''"
	}
	if len(expandedKeys) == 1 {
		return BuildPropertyExtractionSQL(expandedKeys[0])
	}
	// multiple children — COALESCE over all of them, picking the first non-empty
	parts := make([]string, 0, len(expandedKeys))
	for _, k := range expandedKeys {
		parts = append(parts, "nullIf("+BuildPropertyExtractionSQL(k)+", '')")
	}
	return "coalesce(" + strings.Join(parts, ", ") + ", '')"
}

// BuildPropertyExtractionSQL standardizes how properties are queried, handling frontend prefixes.
func BuildPropertyExtractionSQL(key string) string {
	cleanKey := key
	if strings.HasPrefix(key, "default_") {
		cleanKey = key[8:]
		return fmt.Sprintf("default_properties['%s']", escapeString(cleanKey))
	}
	if strings.HasPrefix(key, "prop_") {
		cleanKey = key[5:]
	}
	return fmt.Sprintf("mapUpdate(default_properties, properties)['%s']", escapeString(cleanKey))
}

// BuildWindowFunnelQuery constructs a ClickHouse SQL query using the windowFunnel function.
func BuildWindowFunnelQuery(req models.FunnelRequest, defaultWindowSeconds int) (string, []any) {
	var args []any

	// Helper to extract properties based on existing conventions (mapUpdate for defaults fallback)
	extractProp := func(key string) string {
		return BuildPropertyExtractionSQL(key)
	}

	// 1. Build breakdowns — use ExpandedBreakdowns for merged/virtual properties
	var breakdownSelects []string
	var breakdownAliases []string
	for i, bp := range req.Breakdowns {
		alias := fmt.Sprintf("breakdown_%d", i)
		var expandedKeys []string
		if i < len(req.ExpandedBreakdowns) && len(req.ExpandedBreakdowns[i]) > 0 {
			expandedKeys = req.ExpandedBreakdowns[i]
		} else {
			expandedKeys = []string{bp}
		}
		breakdownSelects = append(breakdownSelects, fmt.Sprintf("%s AS %s", BuildBreakdownSQL(expandedKeys), alias))
		breakdownAliases = append(breakdownAliases, alias)
	}

	// 2. Build step conditions (booleans)
	var stepMatchExprs []string
	var stepMatchAliases []string
	var stepFilterArgs []any

	for i, step := range req.Steps {
		alias := fmt.Sprintf("step_%d_match", i+1)
		var cond string
		if len(step.ExpandedEvents) > 1 {
			var escaped []string
			for _, ev := range step.ExpandedEvents {
				escaped = append(escaped, fmt.Sprintf("'%s'", escapeString(ev)))
			}
			cond = fmt.Sprintf("event_name IN (%s)", strings.Join(escaped, ", "))
		} else {
			eventName := step.EventName
			if len(step.ExpandedEvents) == 1 {
				eventName = step.ExpandedEvents[0]
			}
			cond = fmt.Sprintf("event_name = '%s'", escapeString(eventName))
		}

		if len(step.Filters) > 0 {
			filterStmt, fArgs := buildFilterConds(step.Filters)
			if filterStmt != "" {
				cond = "(" + cond + " AND " + filterStmt + ")"
				stepFilterArgs = append(stepFilterArgs, fArgs...)
			}
		}

		stepMatchExprs = append(stepMatchExprs, fmt.Sprintf("%s AS %s", cond, alias))
		stepMatchAliases = append(stepMatchAliases, alias)
	}

	modeArgs := ", 'strict_deduplication'"
	if req.OrderMode == "strict" {
		modeArgs = ", 'strict_order', 'strict_deduplication'"
	}

	windowFunnelCall := fmt.Sprintf("windowFunnel(%d%s)(\n                timestamp,\n                %s\n            )", defaultWindowSeconds, modeArgs, strings.Join(stepMatchAliases, ",\n                "))

	// 3. Build WHERE clause for the innermost (raw events) query
	baseWhere := "project_id = ? AND environment = ?"
	var baseArgs []any
	baseArgs = append(baseArgs, req.ProjectID, req.Environment)

	if !req.GlobalDateRange.Start.IsZero() && !req.GlobalDateRange.End.IsZero() {
		baseWhere += " AND timestamp >= ? AND timestamp <= ?"
		baseArgs = append(baseArgs, req.GlobalDateRange.Start.UTC(), req.GlobalDateRange.End.UTC())
	}

	if len(req.GlobalFilters) > 0 {
		globalFilterStmt, globalFilterArgs := buildFilterConds(req.GlobalFilters)
		if globalFilterStmt != "" {
			baseWhere += " AND " + globalFilterStmt
			baseArgs = append(baseArgs, globalFilterArgs...)
		}
	}

	// Inner Query: Extract raw events and calculate booleans once per row
	// This avoids doubling placeholders in the final query
	rawSelects := []string{"distinct_id", "timestamp"}
	if len(breakdownSelects) > 0 {
		rawSelects = append(rawSelects, breakdownSelects...)
	}
	rawSelects = append(rawSelects, stepMatchExprs...)
	for _, hc := range req.HoldConstants {
		rawSelects = append(rawSelects, extractProp(hc))
	}

	rawArgs := append(stepFilterArgs, baseArgs...)

	rawEventsQuery := fmt.Sprintf(`SELECT 
            %s
        FROM events
        WHERE %s`, strings.Join(rawSelects, ",\n            "), baseWhere)

	// 4. Build GROUP BY for aggregate query
	groupBys := make([]string, 0)
	if len(breakdownAliases) > 0 {
		groupBys = append(groupBys, breakdownAliases...)
	}
	groupBys = append(groupBys, "distinct_id")

	for _, hc := range req.HoldConstants {
		groupBys = append(groupBys, extractProp(hc))
	}

	// Innermost aggregate select
	innerSelects := make([]string, 0)
	if len(breakdownAliases) > 0 {
		innerSelects = append(innerSelects, breakdownAliases...)
	}
	innerSelects = append(innerSelects, "distinct_id")
	innerSelects = append(innerSelects, windowFunnelCall+" AS level")

	for i, alias := range stepMatchAliases {
		innerSelects = append(innerSelects, fmt.Sprintf("minIf(timestamp, %s) AS step_%d_time", alias, i+1))
	}

	innerQuery := fmt.Sprintf(`SELECT 
            %s
        FROM (
            %s
        )
        GROUP BY %s`, strings.Join(innerSelects, ",\n            "), rawEventsQuery, strings.Join(groupBys, ", "))

	args = rawArgs

	// Middle query (handles MAX(level) when there are hold constants)
	midSelects := make([]string, 0)
	midGroupBys := make([]string, 0)
	if len(breakdownAliases) > 0 {
		midSelects = append(midSelects, breakdownAliases...)
		midGroupBys = append(midGroupBys, breakdownAliases...)
	}
	midSelects = append(midSelects, "distinct_id", "MAX(level) as level")
	for i := range stepMatchAliases {
		midSelects = append(midSelects, fmt.Sprintf("min(step_%d_time) as step_%d_time", i+1, i+1))
	}
	midGroupBys = append(midGroupBys, "distinct_id")

	midQuery := fmt.Sprintf(`SELECT %s
    FROM (
        %s
    )
    GROUP BY %s`, strings.Join(midSelects, ", "), innerQuery, strings.Join(midGroupBys, ", "))

	// Outer query
	outerSelects := make([]string, 0)
	outerGroupBys := make([]string, 0)

	for _, alias := range breakdownAliases {
		outerSelects = append(outerSelects, alias)
		outerGroupBys = append(outerGroupBys, alias)
	}

	outerSelects = append(outerSelects, "arrayJoin(range(1, toUInt32(level) + 1)) AS funnel_level", "count(DISTINCT distinct_id) as users_at_level")
	outerGroupBys = append(outerGroupBys, "funnel_level")

	outerOrderBy := make([]string, 0)
	if len(breakdownAliases) > 0 {
		outerOrderBy = append(outerOrderBy, breakdownAliases...)
	}
	outerOrderBy = append(outerOrderBy, "funnel_level ASC")

	finalQuery := fmt.Sprintf(`SELECT 
    %s
FROM (
    %s
)
WHERE level > 0
GROUP BY %s
ORDER BY %s`, strings.Join(outerSelects, ",\n    "), midQuery, strings.Join(outerGroupBys, ", "), strings.Join(outerOrderBy, ", "))

	// Build times array: [0, step2-step1, step3-step2...]
	var timeDiffs []string
	timeDiffs = append(timeDiffs, "0")
	for i := 1; i < len(stepMatchAliases); i++ {
		timeDiffs = append(timeDiffs, fmt.Sprintf("dateDiff('second', step_%d_time, step_%d_time)", i, i+1))
	}
	timesArrayStr := fmt.Sprintf("[%s] AS times_array", strings.Join(timeDiffs, ", "))

	// Replace "funnel_level" back to "level" in outer result for frontend compatibility
	// To be very precise:
	finalQuery = fmt.Sprintf(`SELECT 
    %s
FROM (
    SELECT 
        %s
    FROM (
        %s
    )
    WHERE level > 0
)
GROUP BY %s
ORDER BY %s`,
		strings.Join(append(breakdownAliases, "funnel_level AS level", "count(DISTINCT distinct_id) as users_at_level", "avg(if(time_to_reach > 0, time_to_reach, NULL)) as avg_time_to_next"), ",\n    "),
		strings.Join(append(breakdownAliases, "arrayJoin(range(1, toUInt32(level) + 1)) AS funnel_level", "distinct_id", timesArrayStr, "times_array[funnel_level] AS time_to_reach"), ",\n        "),
		midQuery,
		strings.Join(append(breakdownAliases, "funnel_level"), ", "),
		strings.Join(append(breakdownAliases, "funnel_level ASC"), ", "),
	)

	return finalQuery, args
}

// buildFilterConds returns a combined SQL condition string and arguments for the given filters.
// If a filter has ExpandedProperties set, it generates an OR across all expanded keys.
func buildFilterConds(filters []models.Filter) (string, []any) {
	var conds []string
	var args []any
	for _, f := range filters {
		// Determine which property keys to query — either expanded (for virtual/merged) or just the one
		propKeys := f.ExpandedProperties
		if len(propKeys) == 0 {
			propKeys = []string{f.Property}
		}

		var orParts []string
		for _, pk := range propKeys {
			extractedVal := BuildPropertyExtractionSQL(pk)

			switch f.Operator {
			case "eq":
				if len(f.Values) > 0 {
					orParts = append(orParts, fmt.Sprintf("%s = ?", extractedVal))
					args = append(args, f.Values[0])
				}
			case "neq":
				if len(f.Values) > 0 {
					orParts = append(orParts, fmt.Sprintf("%s != ?", extractedVal))
					args = append(args, f.Values[0])
				}
			case "in":
				if len(f.Values) > 0 {
					placeholders := make([]string, len(f.Values))
					for i, v := range f.Values {
						placeholders[i] = "?"
						args = append(args, v)
					}
					orParts = append(orParts, fmt.Sprintf("%s IN (%s)", extractedVal, strings.Join(placeholders, ", ")))
				}
			case "contains":
				if len(f.Values) > 0 {
					orParts = append(orParts, fmt.Sprintf("position(%s, ?) > 0", extractedVal))
					args = append(args, f.Values[0])
				}
			}
		}

		if len(orParts) == 1 {
			conds = append(conds, orParts[0])
		} else if len(orParts) > 1 {
			conds = append(conds, "("+strings.Join(orParts, " OR ")+")")
		}
	}
	return strings.Join(conds, " AND "), args
}

// buildFilterCondsNamed returns a combined SQL condition string and clickhouse.Named arguments
// specifically for sequenceMatch where ? placeholders cause conflicts.
func buildFilterCondsNamed(filters []models.Filter, prefix string) (string, []any) {
	var conds []string
	var args []any

	for i, f := range filters {
		propKeys := f.ExpandedProperties
		if len(propKeys) == 0 {
			propKeys = []string{f.Property}
		}

		var orParts []string
		for k, pk := range propKeys {
			extractedVal := BuildPropertyExtractionSQL(pk)
			paramName := fmt.Sprintf("%s_f%d_k%d", prefix, i, k)

			switch f.Operator {
			case "eq":
				if len(f.Values) > 0 {
					orParts = append(orParts, fmt.Sprintf("%s = @%s", extractedVal, paramName))
					args = append(args, clickhouse.Named(paramName, f.Values[0]))
				}
			case "neq":
				if len(f.Values) > 0 {
					orParts = append(orParts, fmt.Sprintf("%s != @%s", extractedVal, paramName))
					args = append(args, clickhouse.Named(paramName, f.Values[0]))
				}
			case "in":
				if len(f.Values) > 0 {
					placeholders := make([]string, len(f.Values))
					for j, v := range f.Values {
						pName := fmt.Sprintf("%s_v%d", paramName, j)
						placeholders[j] = "@" + pName
						args = append(args, clickhouse.Named(pName, v))
					}
					orParts = append(orParts, fmt.Sprintf("%s IN (%s)", extractedVal, strings.Join(placeholders, ", ")))
				}
			case "contains":
				if len(f.Values) > 0 {
					orParts = append(orParts, fmt.Sprintf("position(%s, @%s) > 0", extractedVal, paramName))
					args = append(args, clickhouse.Named(paramName, f.Values[0]))
				}
			}
		}

		if len(orParts) == 1 {
			conds = append(conds, orParts[0])
		} else if len(orParts) > 1 {
			conds = append(conds, "("+strings.Join(orParts, " OR ")+")")
		}
	}
	return strings.Join(conds, " AND "), args
}

// BuildSequenceMatchQuery constructs an advanced funnel query using sequenceMatch.
func BuildSequenceMatchQuery(req models.FunnelRequest, windowSeconds int) (string, []any) {
	var args []any

	extractProp := func(key string) string {
		return BuildPropertyExtractionSQL(key)
	}

	// 1. Breakdowns — use ExpandedBreakdowns for merged/virtual properties
	var breakdownSelects []string
	var breakdownAliases []string
	for i, bp := range req.Breakdowns {
		alias := fmt.Sprintf("breakdown_%d", i)
		var expandedKeys []string
		if i < len(req.ExpandedBreakdowns) && len(req.ExpandedBreakdowns[i]) > 0 {
			expandedKeys = req.ExpandedBreakdowns[i]
		} else {
			expandedKeys = []string{bp}
		}
		breakdownSelects = append(breakdownSelects, fmt.Sprintf("%s AS %s", BuildBreakdownSQL(expandedKeys), alias))
		breakdownAliases = append(breakdownAliases, alias)
	}

	// 2. Build inner condition statements
	var condSelects []string
	var condArgs []any
	eventCount := 0

	for i, step := range req.Steps {
		condName := fmt.Sprintf("cond_%d", i+1)

		var condExpr string
		if len(step.ExpandedEvents) > 1 {
			var escaped []string
			for _, ev := range step.ExpandedEvents {
				escaped = append(escaped, fmt.Sprintf("'%s'", escapeString(ev)))
			}
			condExpr = fmt.Sprintf("event_name IN (%s)", strings.Join(escaped, ", "))
		} else {
			eventName := step.EventName
			if len(step.ExpandedEvents) == 1 {
				eventName = step.ExpandedEvents[0]
			}
			condExpr = fmt.Sprintf("event_name = '%s'", escapeString(eventName))
		}

		if len(step.Filters) > 0 {
			filterStmt, filterArgs := buildFilterCondsNamed(step.Filters, fmt.Sprintf("step%d", i))
			if filterStmt != "" {
				condExpr = condExpr + " AND " + filterStmt
				condArgs = append(condArgs, filterArgs...)
			}
		}
		condSelects = append(condSelects, fmt.Sprintf("(%s) AS %s", condExpr, condName))
		if step.Type == "event" || step.Type == "" {
			eventCount++
		}
	}

	// 3. Inner WHERE
	whereStmt := "project_id = @project_id AND environment = @environment"
	var whereArgs []any
	whereArgs = append(whereArgs, clickhouse.Named("project_id", req.ProjectID), clickhouse.Named("environment", req.Environment))

	if !req.GlobalDateRange.Start.IsZero() && !req.GlobalDateRange.End.IsZero() {
		whereStmt += " AND timestamp >= @start_time AND timestamp <= @end_time"
		whereArgs = append(whereArgs, clickhouse.Named("start_time", req.GlobalDateRange.Start.UTC()), clickhouse.Named("end_time", req.GlobalDateRange.End.UTC()))
	}

	if len(req.GlobalFilters) > 0 {
		globalFilterStmt, globalFilterArgs := buildFilterCondsNamed(req.GlobalFilters, "global")
		if globalFilterStmt != "" {
			whereStmt += " AND " + globalFilterStmt
			whereArgs = append(whereArgs, globalFilterArgs...)
		}
	}

	// 4. Inner Query
	innerSelects := make([]string, 0)
	innerSelects = append(innerSelects, "distinct_id", "timestamp")
	if len(breakdownSelects) > 0 {
		innerSelects = append(innerSelects, breakdownSelects...)
	}
	innerSelects = append(innerSelects, condSelects...)

	args = append(args, condArgs...)
	args = append(args, whereArgs...)

	innerQuery := fmt.Sprintf(`SELECT 
        %s
    FROM events
    WHERE %s`, strings.Join(innerSelects, ",\n        "), whereStmt)

	// 5. Middle Query (SequenceMatch Evaluations)
	var condRefs []string
	for i := range req.Steps {
		condRefs = append(condRefs, fmt.Sprintf("cond_%d", i+1))
	}
	condRefsStr := strings.Join(condRefs, ", ")

	var levelChecks []string
	for targetMax := 1; targetMax <= eventCount; targetMax++ {
		pattern := buildSequencePattern(req.Steps, targetMax, req.OrderMode, windowSeconds)
		levelChecks = append(levelChecks, fmt.Sprintf("sequenceMatch('%s')(timestamp, %s) AS level_%d", pattern, condRefsStr, targetMax))
	}

	midGroupBys := make([]string, 0)
	midGroupBys = append(midGroupBys, "distinct_id")
	if len(breakdownAliases) > 0 {
		midGroupBys = append(midGroupBys, breakdownAliases...)
	}
	// Inject HoldConstants into the middle query correctly
	var holdAliases []string
	for _, hc := range req.HoldConstants {
		alias := fmt.Sprintf("hold_%s", sanitizeKeyForAlias(hc))
		holdAliases = append(holdAliases, alias)
	}

	midSelects := make([]string, 0)
	midSelects = append(midSelects, "distinct_id")
	if len(breakdownAliases) > 0 {
		midSelects = append(midSelects, breakdownAliases...)
	}
	if len(holdAliases) > 0 {
		midSelects = append(midSelects, holdAliases...)
	}
	midSelects = append(midSelects, levelChecks...)
	for i := range req.Steps {
		midSelects = append(midSelects, fmt.Sprintf("minIf(timestamp, cond_%d) AS step_%d_time", i+1, i+1))
	}

	midQuery := fmt.Sprintf(`SELECT 
    %s
FROM (
    %s
)
GROUP BY %s`, strings.Join(midSelects, ",\n    "), innerQuery, strings.Join(append(append([]string{"distinct_id"}, breakdownAliases...), holdAliases...), ", "))

	// 6. Max Level Resolution Query
	// Convert level_1, level_2 into a single max_level
	var multiIfParts []string
	for i := eventCount; i >= 1; i-- {
		multiIfParts = append(multiIfParts, fmt.Sprintf("level_%d > 0", i), fmt.Sprintf("%d", i))
	}
	multiIfParts = append(multiIfParts, "0")
	maxLevelExpr := fmt.Sprintf("multiIf(%s)", strings.Join(multiIfParts, ", "))

	maxLevelSelects := make([]string, 0)
	maxLevelSelects = append(maxLevelSelects, "distinct_id")
	if len(breakdownAliases) > 0 {
		maxLevelSelects = append(maxLevelSelects, breakdownAliases...)
	}
	maxLevelSelects = append(maxLevelSelects, fmt.Sprintf("%s AS max_level", maxLevelExpr))
	for i := range req.Steps {
		maxLevelSelects = append(maxLevelSelects, fmt.Sprintf("step_%d_time", i+1))
	}
	for _, holdAlias := range holdAliases {
		maxLevelSelects = append(maxLevelSelects, holdAlias)
	}

	maxLevelQuery := fmt.Sprintf(`SELECT
    %s
FROM (
    %s
)`, strings.Join(maxLevelSelects, ",\n    "), midQuery)

	// 7. Outer Query (Count by Level)
	// We do an additional MAX aggregation if holding properties are present to ensure distinct_id goes to their highest reached level across all values of the held property
	var aggSelects []string
	aggSelects = append(aggSelects, "distinct_id")
	if len(breakdownAliases) > 0 {
		aggSelects = append(aggSelects, breakdownAliases...)
	}
	aggSelects = append(aggSelects, "max(max_level) AS final_level")
	for i := range req.Steps {
		aggSelects = append(aggSelects, fmt.Sprintf("min(step_%d_time) as step_%d_time", i+1, i+1))
	}

	aggGroupBys := append([]string{"distinct_id"}, breakdownAliases...)

	aggQuery := fmt.Sprintf(`SELECT %s
FROM (
    %s
)
GROUP BY %s`, strings.Join(aggSelects, ", "), maxLevelQuery, strings.Join(aggGroupBys, ", "))

	outerSelects := make([]string, 0)
	if len(breakdownAliases) > 0 {
		outerSelects = append(outerSelects, breakdownAliases...)
	}
	outerSelects = append(outerSelects, "final_level AS level", "count(DISTINCT distinct_id) AS users_at_level")

	outerGroupBys := append(breakdownAliases, "final_level")
	outerOrderBys := append(breakdownAliases, "final_level ASC")

	finalQuery := fmt.Sprintf(`SELECT 
    %s
FROM (
    %s
)
WHERE final_level > 0
GROUP BY %s
ORDER BY %s`, strings.Join(outerSelects, ",\n    "), aggQuery, strings.Join(outerGroupBys, ", "), strings.Join(outerOrderBys, ", "))

	finalQuery = fmt.Sprintf(`SELECT 
    %s
FROM (
    SELECT 
        %s
    FROM (
        %s
    )
    WHERE final_level > 0
)
GROUP BY %s
ORDER BY %s`,
		strings.Join(append(breakdownAliases, "funnel_level AS level", "count(DISTINCT distinct_id) as users_at_level"), ",\n    "),
		strings.Join(append(breakdownAliases, "arrayJoin(range(1, toUInt32(final_level) + 1)) AS funnel_level", "distinct_id"), ",\n        "),
		aggQuery,
		strings.Join(append(breakdownAliases, "funnel_level"), ", "),
		strings.Join(append(breakdownAliases, "funnel_level ASC"), ", "),
	)
	var innerSelectsFixed []string
	innerSelectsFixed = append(innerSelectsFixed, "distinct_id", "timestamp")
	if len(breakdownSelects) > 0 {
		innerSelectsFixed = append(innerSelectsFixed, breakdownSelects...)
	}
	for i, hc := range req.HoldConstants {
		innerSelectsFixed = append(innerSelectsFixed, fmt.Sprintf("%s AS %s", extractProp(hc), holdAliases[i]))
	}
	innerSelectsFixed = append(innerSelectsFixed, condSelects...)

	// Replace the innerQuery string
	innerQuery = fmt.Sprintf(`SELECT 
        %s
    FROM events
    WHERE %s`, strings.Join(innerSelectsFixed, ",\n        "), whereStmt)

	// Re-compose midQuery
	midQuery = fmt.Sprintf(`SELECT 
    %s
FROM (
    %s
)
GROUP BY %s`, strings.Join(midSelects, ",\n    "), innerQuery, strings.Join(append(append([]string{"distinct_id"}, breakdownAliases...), holdAliases...), ", "))

	// Re-compose maxLevelQuery
	maxLevelQuery = fmt.Sprintf(`SELECT
    %s
FROM (
    %s
)`, strings.Join(maxLevelSelects, ",\n    "), midQuery)

	// Re-compose aggQuery
	aggQuery = fmt.Sprintf(`SELECT %s
FROM (
    %s
)
GROUP BY %s`, strings.Join(aggSelects, ", "), maxLevelQuery, strings.Join(aggGroupBys, ", "))

	var timeDiffs []string
	timeDiffs = append(timeDiffs, "0")
	for i := 1; i < len(req.Steps); i++ {
		timeDiffs = append(timeDiffs, fmt.Sprintf("dateDiff('second', step_%d_time, step_%d_time)", i, i+1))
	}
	timesArrayStr := fmt.Sprintf("[%s] AS times_array", strings.Join(timeDiffs, ", "))

	// Re-compose finalQuery completely
	finalQuery = fmt.Sprintf(`SELECT 
    %s
FROM (
    SELECT 
        %s
    FROM (
        %s
    )
    WHERE final_level > 0
)
GROUP BY %s
ORDER BY %s`,
		strings.Join(append(breakdownAliases, "funnel_level AS level", "count(DISTINCT distinct_id) as users_at_level", "avg(if(time_to_reach > 0, time_to_reach, NULL)) as avg_time_to_next"), ",\n    "),
		strings.Join(append(breakdownAliases, "arrayJoin(range(1, toUInt32(final_level) + 1)) AS funnel_level", "distinct_id", timesArrayStr, "times_array[funnel_level] AS time_to_reach"), ",\n        "),
		aggQuery,
		strings.Join(append(breakdownAliases, "funnel_level"), ", "),
		strings.Join(append(breakdownAliases, "funnel_level ASC"), ", "),
	)

	return finalQuery, args
}

// buildSequencePattern generates the Regex-like patterns for ClickHouse sequenceMatch
func buildSequencePattern(steps []models.FunnelStep, targetEventIndex int, orderMode string, globalWindow int) string {
	var pattern string
	eventCount := 0

	var transitionPattern string

	for i, step := range steps {
		condRef := fmt.Sprintf("(?%d)", i+1)

		if step.Type == "event" || step.Type == "" {
			eventCount++
			if eventCount == 1 {
				pattern = condRef
			} else {
				if orderMode == "loose" || orderMode == "" {
					if transitionPattern != "" {
						pattern += ".*" + transitionPattern + ".*"
					} else {
						pattern += ".*"
					}
				} else {
					pattern += transitionPattern
				}

				if step.TimeToNext != nil && *step.TimeToNext > 0 {
					pattern += fmt.Sprintf("(?t<=%d)", *step.TimeToNext)
				} else if globalWindow > 0 {
					pattern += fmt.Sprintf("(?t<=%d)", globalWindow)
				}
				pattern += condRef
			}

			transitionPattern = ""

			if eventCount == targetEventIndex {
				return pattern
			}
		} else if step.Type == "exclusion" {
			if transitionPattern != "" {
				transitionPattern += ".*"
			}
			transitionPattern += fmt.Sprintf("~%s", condRef)
		}
	}
	return pattern
}

func sanitizeKeyForAlias(key string) string {
	s := strings.ToLower(key)
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")
	return s
}

func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
