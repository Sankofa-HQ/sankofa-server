package database

import (
	"fmt"

	"sankofa/engine/internal/models"
)

// BuildFlowQuery generates a ClickHouse query to extract user flows starting from a specific event.
// It uses groupArray and arraySlice to trace user paths.
func BuildFlowQuery(req models.FlowRequest) (string, []any) {
	var whereArgs []any

	whereStmt := "project_id = ?"
	whereArgs = append(whereArgs, req.ProjectID)

	if !req.GlobalDateRange.Start.IsZero() && !req.GlobalDateRange.End.IsZero() {
		whereStmt += " AND timestamp >= ? AND timestamp <= ?"
		whereArgs = append(whereArgs, req.GlobalDateRange.Start.UTC(), req.GlobalDateRange.End.UTC())
	}

	if len(req.GlobalFilters) > 0 {
		globalFilterStmt, globalFilterArgs := buildFilterConds(req.GlobalFilters)
		if globalFilterStmt != "" {
			whereStmt += " AND " + globalFilterStmt
			whereArgs = append(whereArgs, globalFilterArgs...)
		}
	}

	if len(req.HiddenEvents) > 0 {
		var qMarks string
		for i, ev := range req.HiddenEvents {
			if i > 0 {
				qMarks += ", "
			}
			qMarks += "?"
			whereArgs = append(whereArgs, ev)
		}
		whereStmt += fmt.Sprintf(" AND event_name NOT IN (%s)", qMarks)
	}

	eventNameProj := "event_name"
	var finalArgs []any

	if req.BreakdownEvent != "" && req.BreakdownProperty != "" {
		eventNameProj = `if(event_name == ?, event_name || ' (' || coalesce(nullIf(JSONExtractString(properties, ?), ''), 'Other') || ')', event_name)`
		finalArgs = append(finalArgs, req.BreakdownEvent, req.BreakdownProperty)
	}

	finalArgs = append(finalArgs, whereArgs...)

	stepsBefore := req.StepsBefore
	if stepsBefore < 0 {
		stepsBefore = 0
	}
	stepsAfter := req.StepsAfter
	if stepsAfter <= 0 {
		stepsAfter = 5 // default forward path depth
	}

	// Bind parameters for the `indexOf` array matching, `arraySlice` depth, and aliasing the initial node.
	var pathsCTE string
	if req.EndEvent != "" {
		pathsCTE = `
paths AS (
    SELECT
        actor_id,
        indexOf(arrayMap(x -> has(CAST(? AS Array(String)), x), event_sequence), 1) AS start_idx,
        indexOf(event_sequence, ?, start_idx) AS end_idx,
        GREATEST(1, start_idx - ?) AS slice_start,
        end_idx - slice_start + 1 + ? AS slice_len,
        arraySlice(event_sequence, slice_start, slice_len) AS path,
        start_idx - slice_start AS anchor_offset
    FROM session_events
    WHERE start_idx > 0 AND end_idx > 0
)`
		finalArgs = append(finalArgs, req.StartEventExpanded, req.EndEvent, stepsBefore, stepsAfter)
	} else {
		pathsCTE = `
paths AS (
    SELECT
        actor_id,
        indexOf(arrayMap(x -> has(CAST(? AS Array(String)), x), event_sequence), 1) AS start_idx,
        GREATEST(1, start_idx - ?) AS slice_start,
        ? + 1 + (start_idx - slice_start) AS slice_len,
        arraySlice(event_sequence, slice_start, slice_len) AS path,
        start_idx - slice_start AS anchor_offset
    FROM session_events
    WHERE start_idx > 0
)`
		finalArgs = append(finalArgs, req.StartEventExpanded, stepsBefore, stepsAfter)
	}

	// Edges alias args
	finalArgs = append(finalArgs, req.StartEvent, req.StartEvent)

	query := fmt.Sprintf(`
WITH session_events AS (
    SELECT
        if(empty(session_id), distinct_id, session_id) AS actor_id,
        groupArray(event_name) AS event_sequence
    FROM (
        SELECT session_id, distinct_id, %s as event_name
        FROM events
        WHERE %s
        ORDER BY timestamp ASC
    )
    GROUP BY actor_id
),
%s,
edges AS (
    SELECT
        path[i] as raw_source,
        path[i+1] as raw_target,
        if(i - (anchor_offset + 1) = 0, CAST(? AS String), path[i]) || ' (Step ' || toString(i - (anchor_offset + 1)) || ')' as source,
        if(i + 1 - (anchor_offset + 1) = 0, CAST(? AS String), path[i+1]) || ' (Step ' || toString(i + 1 - (anchor_offset + 1)) || ')' as target,
        i - (anchor_offset + 1) as step_level
    FROM paths
    ARRAY JOIN arrayEnumerate(path) AS i
    WHERE i < length(path) AND path[i+1] != ''
)
SELECT source, target, count() as value, step_level
FROM edges
GROUP BY source, target, step_level
HAVING value > 0
ORDER BY step_level ASC, value DESC
LIMIT 1000
    `, eventNameProj, whereStmt, pathsCTE)

	return query, finalArgs
}
