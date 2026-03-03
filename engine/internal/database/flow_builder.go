package database

import (
	"fmt"

	"sankofa/engine/internal/models"
)

// BuildFlowQuery generates a ClickHouse query to extract user flows starting from a specific event.
// It uses groupArray and arraySlice to trace user paths.
func BuildFlowQuery(req models.FlowRequest) (string, []any) {
	var args []any

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

	maxDepth := req.MaxDepth
	if maxDepth <= 0 {
		maxDepth = 5 // default to 5 steps deep
	}

	// Bind parameters for the `indexOf` array matching, `arraySlice` depth, and aliasing the initial node.
	// Since we pass an array to the prepared statement, we CAST to Array(String).
	args = append(args, req.StartEventExpanded, maxDepth, req.StartEvent)

	query := fmt.Sprintf(`
WITH session_events AS (
    SELECT
        if(empty(session_id), distinct_id, session_id) AS actor_id,
        groupArray(event_name) AS event_sequence
    FROM (
        SELECT session_id, distinct_id, event_name
        FROM events
        WHERE %s
        ORDER BY timestamp ASC
    )
    GROUP BY actor_id
),
paths AS (
    SELECT
        actor_id,
        indexOf(arrayMap(x -> has(CAST(? AS Array(String)), x), event_sequence), 1) AS start_idx,
        arraySlice(event_sequence, start_idx, ?) AS path
    FROM session_events
    WHERE start_idx > 0
),
edges AS (
    SELECT
        path[i] as raw_source,
        path[i+1] as raw_target,
        if(i=1, CAST(? AS String), path[i]) || ' (Step ' || toString(i-1) || ')' as source,
        path[i+1] || ' (Step ' || toString(i) || ')' as target,
        i as step_level
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
    `, whereStmt)

	return query, args
}
