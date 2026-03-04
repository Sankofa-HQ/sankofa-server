package database

import (
	"fmt"
	"strings"

	"sankofa/engine/internal/models"
)

// BuildFlowQuery generates a ClickHouse query to extract user flows.
// It supports both legacy single-event mode (start_event/end_event) and
// multi-step sequential chaining (Steps[]).
func BuildFlowQuery(req models.FlowRequest) (string, []any) {

	// ── Decide mode: multi-step (Steps[]) vs legacy (start_event/end_event) ──
	if len(req.Steps) > 0 && req.Steps[0].EventName != "" {
		return buildMultiStepFlowQuery(req)
	}
	return buildLegacyFlowQuery(req)
}

// ════════════════════════════════════════════════════════════════════════════
// MULTI-STEP SEQUENTIAL CHAIN (new architecture)
//
// Strategy: one shared CTE finds all sequential anchors (idx_A, idx_B, idx_C).
// Then per-step UNION ALL queries each extract [anchor - before, anchor + after]
// with EXACT step counts and step-prefixed column labels.
// ════════════════════════════════════════════════════════════════════════════

func buildMultiStepFlowQuery(req models.FlowRequest) (string, []any) {
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

	var finalArgs []any
	finalArgs = append(finalArgs, whereArgs...)

	steps := req.Steps
	stepCount := len(steps)
	stepLetters := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

	// ── Build the chained indexOf columns for each step ──
	var idxColumns []string
	var pathsArgs []any

	for i, step := range steps {
		letter := string(stepLetters[i])
		eventName := step.EventName

		if i == 0 {
			// First step: use virtual event expansion
			expanded := req.StartEventExpanded
			if len(expanded) == 0 {
				expanded = []string{eventName}
			}
			idxColumns = append(idxColumns,
				fmt.Sprintf("        indexOf(arrayMap(x -> has(CAST(? AS Array(String)), x), event_sequence), 1) AS idx_%s", letter))
			pathsArgs = append(pathsArgs, expanded)
		} else {
			prevLetter := string(stepLetters[i-1])
			// Relative indexOf with false-positive guard
			idxColumns = append(idxColumns,
				fmt.Sprintf("        indexOf(arraySlice(event_sequence, idx_%s + 1), ?) AS rel_%s", prevLetter, letter))
			pathsArgs = append(pathsArgs, eventName)
			idxColumns = append(idxColumns,
				fmt.Sprintf("        if(idx_%s > 0 AND rel_%s > 0, rel_%s + idx_%s, 0) AS idx_%s", prevLetter, letter, letter, prevLetter, letter))
		}
	}

	// WHERE: all anchors must be found sequentially
	lastLetter := string(stepLetters[stepCount-1])
	whereFilter := fmt.Sprintf("idx_%s > 0", lastLetter)

	finalArgs = append(finalArgs, pathsArgs...)

	// ── Compute per-step zone boundaries ──
	type stepBounds struct {
		letter string
		before int
		after  int
	}
	var bounds []stepBounds
	for i := range steps {
		b := steps[i].StepsBefore
		if b < 0 {
			b = 0
		}
		a := steps[i].StepsAfter
		if a <= 0 {
			a = 3
		}
		bounds = append(bounds, stepBounds{letter: string(stepLetters[i]), before: b, after: a})
	}

	firstLetter := bounds[0].letter
	lastLetter = bounds[stepCount-1].letter

	// ── Build the multiIf label expression ──
	// Two-phase priority:
	//   1. ANCHOR checks first: idx_X always → 'X0' (anchors can never be stolen)
	//   2. ZONE checks in REVERSE order: later steps win for overlapping positions
	// This prevents A's after-zone from swallowing B's anchor when steps are close.
	var labelParts []string

	// Phase 1: Anchor positions always get their own step label
	for _, bd := range bounds {
		l := bd.letter
		labelParts = append(labelParts, fmt.Sprintf(
			"(slice_start + pos - 1) = idx_%s, '%s0'", l, l))
	}

	// Phase 2: Zone checks in REVERSE order (last step first → later steps win overlaps)
	for i := len(bounds) - 1; i >= 0; i-- {
		bd := bounds[i]
		l := bd.letter
		labelParts = append(labelParts, fmt.Sprintf(
			"(slice_start + pos - 1) >= GREATEST(1, idx_%s - %d) AND (slice_start + pos - 1) <= idx_%s + %d, "+
				"concat('%s', if((slice_start + pos - 1 - idx_%s) > 0, '+', ''), "+
				"toString(slice_start + pos - 1 - idx_%s))",
			l, bd.before, l, bd.after, l, l, l))
	}

	multiIfExpr := "multiIf(" + strings.Join(labelParts, ", ") + ", '')"

	// ── Build paths CTE: one continuous slice with labels array ──
	pathsCTE := fmt.Sprintf(`
paths AS (
    SELECT
        actor_id, path, labels,
        arrayFilter(pos -> labels[pos] != '', arrayEnumerate(path)) AS lbl_idx
    FROM (
        SELECT
            actor_id,
%s,
            GREATEST(1, idx_%s - %d) AS slice_start,
            idx_%s + %d - GREATEST(1, idx_%s - %d) + 1 AS slice_len,
            arraySlice(event_sequence, slice_start, slice_len) AS path,
            arrayMap(pos -> %s, arrayEnumerate(arraySlice(event_sequence, slice_start, slice_len))) AS labels
        FROM session_events
        WHERE %s
    )
)`,
		strings.Join(idxColumns, ",\n"),
		firstLetter, bounds[0].before,
		lastLetter, bounds[stepCount-1].after, firstLetter, bounds[0].before,
		multiIfExpr,
		whereFilter)

	finalArgs = append(finalArgs, pathsArgs...)

	// ── Edges: pair consecutive labeled positions ──
	// lbl_idx has the 1-indexed positions in path that have non-empty labels.
	// Pairing lbl_idx[k] with lbl_idx[k+1] creates:
	//   - Within-zone edges (adjacent labeled positions in same step zone)
	//   - Bridge edges (last labeled pos of zone A → first labeled pos of zone B)
	edgesCTE := `
edges AS (
    SELECT
        path[lbl_idx[k]] || ' (' || labels[lbl_idx[k]] || ')' as source,
        path[lbl_idx[k+1]] || ' (' || labels[lbl_idx[k+1]] || ')' as target,
        toUInt32(k) as pos
    FROM paths
    ARRAY JOIN arrayEnumerate(lbl_idx) AS k
    WHERE k < length(lbl_idx)
      AND path[lbl_idx[k]] != ''
      AND path[lbl_idx[k+1]] != ''
)`

	query := fmt.Sprintf(`
WITH session_events AS (
    SELECT
        if(empty(session_id), distinct_id, session_id) AS actor_id,
        groupArray(event_name) AS event_sequence
    FROM (
        SELECT session_id, distinct_id, event_name as event_name
        FROM events
        WHERE %s
        ORDER BY timestamp ASC
    )
    GROUP BY actor_id
),
%s,
%s
SELECT source, target, count() as value, toUInt32(0) as step_level
FROM edges
GROUP BY source, target
HAVING value > 0
ORDER BY value DESC
LIMIT 1000
    `, whereStmt, pathsCTE, edgesCTE)

	return query, finalArgs
}

// ════════════════════════════════════════════════════════════════════════════
// LEGACY SINGLE-EVENT MODE (backward compatibility)
// ════════════════════════════════════════════════════════════════════════════

func buildLegacyFlowQuery(req models.FlowRequest) (string, []any) {
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

	var hasSysConversion bool
	var propertyBreakdown string

	for _, bd := range req.Breakdowns {
		if bd == "sys_conversion" {
			hasSysConversion = true
		} else if bd != "" {
			propertyBreakdown = bd
		}
	}

	if propertyBreakdown != "" {
		eventNameProj = `if(event_name == ?, event_name || ' (' || coalesce(nullIf(JSONExtractString(properties, ?), ''), 'Other') || ')', event_name)`
		finalArgs = append(finalArgs, req.StartEvent, propertyBreakdown)
	}

	finalArgs = append(finalArgs, whereArgs...)

	stepsBefore := req.StepsBefore
	if stepsBefore < 0 {
		stepsBefore = 0
	}
	stepsAfter := req.StepsAfter
	if stepsAfter <= 0 {
		stepsAfter = 5
	}

	// NOTE: ClickHouse indexOf() only takes 2 args (array, element).
	// To search from a start position, we use: indexOf(arraySlice(arr, pos), elem) + pos - 1
	var pathsCTE string
	if req.EndEvent != "" && hasSysConversion {
		pathsCTE = `
paths AS (
    SELECT
        actor_id,
        indexOf(arrayMap(x -> has(CAST(? AS Array(String)), x), event_sequence), 1) AS start_idx,
        if(start_idx > 0, indexOf(arraySlice(event_sequence, start_idx), ?) + start_idx - 1, 0) AS real_end_idx,
        if(real_end_idx > 0, 'Converted', 'Dropped Off') as conversion_status,
        GREATEST(1, start_idx - ?) AS slice_start,
        if(real_end_idx > 0, real_end_idx - slice_start + 1 + ?, ? + 1 + (start_idx - slice_start)) AS slice_len,
        arraySlice(event_sequence, slice_start, slice_len) AS path,
        start_idx - slice_start AS anchor_offset
    FROM session_events
    WHERE start_idx > 0
)`
		finalArgs = append(finalArgs, req.StartEventExpanded, req.EndEvent, stepsBefore, stepsAfter, stepsAfter)
	} else if req.EndEvent != "" {
		pathsCTE = `
paths AS (
    SELECT
        actor_id,
        indexOf(arrayMap(x -> has(CAST(? AS Array(String)), x), event_sequence), 1) AS start_idx,
        if(start_idx > 0, indexOf(arraySlice(event_sequence, start_idx), ?) + start_idx - 1, 0) AS end_idx,
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

	var edgesSelect string
	if hasSysConversion && req.EndEvent != "" {
		edgesSelect = `
        if(i - (anchor_offset + 1) = 0, CAST(? AS String), path[i]) || ' (Step ' || toString(i - (anchor_offset + 1)) || ') - ' || conversion_status as source,
        if(i + 1 - (anchor_offset + 1) = 0, CAST(? AS String), path[i+1]) || ' (Step ' || toString(i + 1 - (anchor_offset + 1)) || ') - ' || conversion_status as target,`
	} else {
		edgesSelect = `
        if(i - (anchor_offset + 1) = 0, CAST(? AS String), path[i]) || ' (Step ' || toString(i - (anchor_offset + 1)) || ')' as source,
        if(i + 1 - (anchor_offset + 1) = 0, CAST(? AS String), path[i+1]) || ' (Step ' || toString(i + 1 - (anchor_offset + 1)) || ')' as target,`
	}
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
        %s
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
    `, eventNameProj, whereStmt, pathsCTE, edgesSelect)

	return query, finalArgs
}
