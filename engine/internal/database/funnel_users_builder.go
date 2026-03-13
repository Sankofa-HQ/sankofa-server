package database

import (
	"fmt"
	"strings"

	"sankofa/engine/internal/models"
	"github.com/ClickHouse/clickhouse-go/v2"
)

// BuildFunnelUsersQuery builds a query that returns distinct_ids for users 
// who dropped off or converted at a specific step.
func BuildFunnelUsersQuery(req models.FunnelRequest, defaultWindowSeconds int, targetStep int, action string, segment string) (string, []any) {
	if models.RequiresSequenceMatch(req) {
		return BuildSequenceMatchUsersQuery(req, defaultWindowSeconds, targetStep, action, segment)
	}
	return BuildWindowFunnelUsersQuery(req, defaultWindowSeconds, targetStep, action, segment)
}

func BuildWindowFunnelUsersQuery(req models.FunnelRequest, defaultWindowSeconds int, targetStep int, action string, segment string) (string, []any) {
	var args []any

	extractProp := func(key string) string {
		return BuildPropertyExtractionSQL(key)
	}

	var breakdownAliases []string
	for i, bp := range req.Breakdowns {
		alias := fmt.Sprintf("breakdown_%d", i)
		var expandedKeys []string
		if i < len(req.ExpandedBreakdowns) && len(req.ExpandedBreakdowns[i]) > 0 {
			expandedKeys = req.ExpandedBreakdowns[i]
		} else {
			expandedKeys = []string{bp}
		}
		breakdownAliases = append(breakdownAliases, fmt.Sprintf("%s AS %s", BuildBreakdownSQL(expandedKeys), alias))
	}

	var conditions []string
	for _, step := range req.Steps {
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
			filterStmt, filterArgs := buildFilterConds(step.Filters)
			if filterStmt != "" {
				cond = cond + " AND " + filterStmt
				args = append(args, filterArgs...)
			}
		}
		conditions = append(conditions, cond)
	}

	modeArgs := ", 'strict_deduplication'"
	if req.OrderMode == "strict" {
		modeArgs = ", 'strict_order', 'strict_deduplication'"
	}

	windowFunnelCall := fmt.Sprintf("windowFunnel(%d%s)(\n                timestamp,\n                %s\n            )", defaultWindowSeconds, modeArgs, strings.Join(conditions, ",\n                "))

	whereStmt := "project_id = ?"
	args = append(args, req.ProjectID)

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

	groupBys := make([]string, 0)
	for i := range breakdownAliases {
		groupBys = append(groupBys, fmt.Sprintf("breakdown_%d", i))
	}
	groupBys = append(groupBys, "distinct_id")

	for _, hc := range req.HoldConstants {
		groupBys = append(groupBys, extractProp(hc))
	}

	innerSelects := make([]string, 0)
	innerSelects = append(innerSelects, breakdownAliases...)
	innerSelects = append(innerSelects, "distinct_id")
	innerSelects = append(innerSelects, windowFunnelCall+" AS level")

	innerQuery := fmt.Sprintf(`SELECT 
            %s
        FROM events
        WHERE %s
        GROUP BY %s`, strings.Join(innerSelects, ",\n            "), whereStmt, strings.Join(groupBys, ", "))

	midSelects := make([]string, 0)
	midGroupBys := make([]string, 0)
	for i := range breakdownAliases {
		alias := fmt.Sprintf("breakdown_%d", i)
		midSelects = append(midSelects, alias)
		midGroupBys = append(midGroupBys, alias)
	}
	midSelects = append(midSelects, "distinct_id", "MAX(level) as level")
	midGroupBys = append(midGroupBys, "distinct_id")

	midQuery := fmt.Sprintf(`SELECT %s
    FROM (
        %s
    )
    GROUP BY %s`, strings.Join(midSelects, ", "), innerQuery, strings.Join(midGroupBys, ", "))

	// Now filter the midQuery based on action and targetStep
	var outerWhere []string
	if action == "converted" {
		outerWhere = append(outerWhere, fmt.Sprintf("level >= %d", targetStep))
	} else if action == "dropoff" {
		outerWhere = append(outerWhere, fmt.Sprintf("level = %d", targetStep-1))
	}

	// Filter by segment string if we have breakdowns
	if segment != "" && segment != "Overall" && len(req.Breakdowns) > 0 {
		// segment string is like "MacOS · 10.15". We match the breakdowns concat.
		// For simplicity, we can reconstruct the breakdown concat.
		var bdParts []string
		for i := range req.Breakdowns {
			bdParts = append(bdParts, fmt.Sprintf("breakdown_%d", i))
		}
		// ClickHouse concat with ' · ' delimiter
		concatExpr := "concat(" + strings.Join(bdParts, ", ' · '") + ")"
		outerWhere = append(outerWhere, fmt.Sprintf("%s = ?", concatExpr))
		args = append(args, segment)
	}

	whereClause := ""
	if len(outerWhere) > 0 {
		whereClause = "WHERE " + strings.Join(outerWhere, " AND ")
	}

	finalQuery := fmt.Sprintf(`SELECT DISTINCT distinct_id FROM (%s) %s`, midQuery, whereClause)

	return finalQuery, args
}

func BuildSequenceMatchUsersQuery(req models.FunnelRequest, defaultWindowSeconds int, targetStep int, action string, segment string) (string, []any) {
	var args []any

	extractProp := func(key string) string {
		return BuildPropertyExtractionSQL(key)
	}

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

	whereStmt := "project_id = @project_id"
	var whereArgs []any
	whereArgs = append(whereArgs, clickhouse.Named("project_id", req.ProjectID))

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

	innerSelects := make([]string, 0)
	innerSelects = append(innerSelects, "distinct_id", "timestamp")
	if len(breakdownSelects) > 0 {
		innerSelects = append(innerSelects, breakdownSelects...)
	}
	
	// Hold constants
	var holdAliases []string
	for _, hc := range req.HoldConstants {
		alias := fmt.Sprintf("hold_%s", sanitizeKeyForAlias(hc))
		holdAliases = append(holdAliases, alias)
		innerSelects = append(innerSelects, fmt.Sprintf("%s AS %s", extractProp(hc), alias))
	}

	innerSelects = append(innerSelects, condSelects...)

	args = append(args, condArgs...)
	args = append(args, whereArgs...)

	innerQuery := fmt.Sprintf(`SELECT 
        %s
    FROM events
    WHERE %s`, strings.Join(innerSelects, ",\n        "), whereStmt)

	var condRefsStr string
	var condRefs []string
	for i := range req.Steps {
		condRefs = append(condRefs, fmt.Sprintf("cond_%d", i+1))
	}
	condRefsStr = strings.Join(condRefs, ", ")

	var levelChecks []string
	for targetMax := 1; targetMax <= eventCount; targetMax++ {
		pattern := buildSequencePattern(req.Steps, targetMax, req.OrderMode, defaultWindowSeconds)
		levelChecks = append(levelChecks, fmt.Sprintf("sequenceMatch('%s')(timestamp, %s) AS level_%d", pattern, condRefsStr, targetMax))
	}

	midGroupBys := make([]string, 0)
	midGroupBys = append(midGroupBys, "distinct_id")
	if len(breakdownAliases) > 0 {
		midGroupBys = append(midGroupBys, breakdownAliases...)
	}
	if len(holdAliases) > 0 {
		midGroupBys = append(midGroupBys, holdAliases...)
	}

	midSelects := make([]string, 0)
	midSelects = append(midSelects, midGroupBys...)
	midSelects = append(midSelects, levelChecks...)

	midQuery := fmt.Sprintf(`SELECT 
    %s
FROM (
    %s
)
GROUP BY %s`, strings.Join(midSelects, ",\n    "), innerQuery, strings.Join(midGroupBys, ", "))

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
	for _, holdAlias := range holdAliases {
		maxLevelSelects = append(maxLevelSelects, holdAlias)
	}

	maxLevelQuery := fmt.Sprintf(`SELECT
    %s
FROM (
    %s
)`, strings.Join(maxLevelSelects, ",\n    "), midQuery)

	aggSelects := make([]string, 0)
	aggSelects = append(aggSelects, "distinct_id")
	if len(breakdownAliases) > 0 {
		aggSelects = append(aggSelects, breakdownAliases...)
	}
	aggSelects = append(aggSelects, "max(max_level) AS final_level")

	aggGroupBys := append([]string{"distinct_id"}, breakdownAliases...)

	aggQuery := fmt.Sprintf(`SELECT %s
FROM (
    %s
)
GROUP BY %s`, strings.Join(aggSelects, ", "), maxLevelQuery, strings.Join(aggGroupBys, ", "))

	var outerWhere []string
	if action == "converted" {
		outerWhere = append(outerWhere, fmt.Sprintf("final_level >= %d", targetStep))
	} else if action == "dropoff" {
		outerWhere = append(outerWhere, fmt.Sprintf("final_level = %d", targetStep-1))
	}

	if segment != "" && segment != "Overall" && len(req.Breakdowns) > 0 {
		var bdParts []string
		for i := range req.Breakdowns {
			bdParts = append(bdParts, fmt.Sprintf("breakdown_%d", i))
		}
		concatExpr := "concat(" + strings.Join(bdParts, ", ' · '") + ")"
		outerWhere = append(outerWhere, fmt.Sprintf("%s = @segment", concatExpr))
		args = append(args, clickhouse.Named("segment", segment))
	}

	whereClause := ""
	if len(outerWhere) > 0 {
		whereClause = "WHERE " + strings.Join(outerWhere, " AND ")
	}

	finalQuery := fmt.Sprintf(`SELECT DISTINCT distinct_id FROM (%s) %s`, aggQuery, whereClause)

	return finalQuery, args
}
