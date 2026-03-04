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
	// Given steps A, B, C, the logic is:
	//   idx_a = indexOf(arrayMap(...), 1)   — find Step A anywhere
	//   rel_b = indexOf(arraySlice(event_sequence, idx_a + 1), stepB_event)
	//   idx_b = if(idx_a > 0 AND rel_b > 0, rel_b + idx_a, 0)  — Step B AFTER A
	//   rel_c = indexOf(arraySlice(event_sequence, idx_b + 1), stepC_event)
	//   idx_c = if(idx_b > 0 AND rel_c > 0, rel_c + idx_b, 0)  — Step C AFTER B
	//
	// Then we slice from (idx_a - steps_before_A) to (last_anchor + steps_after_last)

	var idxColumns []string
	var pathsArgs []any

	for i, step := range steps {
		letter := string(stepLetters[i])
		eventName := step.EventName

		if i == 0 {
			// First step: use virtual event expansion (find using arrayMap + has)
			expanded := req.StartEventExpanded
			if len(expanded) == 0 {
				expanded = []string{eventName}
			}
			idxColumns = append(idxColumns,
				fmt.Sprintf("        indexOf(arrayMap(x -> has(CAST(? AS Array(String)), x), event_sequence), 1) AS idx_%s", letter))
			pathsArgs = append(pathsArgs, expanded)
		} else {
			prevLetter := string(stepLetters[i-1])
			// Relative indexOf on sub-array starting after the previous anchor
			idxColumns = append(idxColumns,
				fmt.Sprintf("        indexOf(arraySlice(event_sequence, idx_%s + 1), ?) AS rel_%s", prevLetter, letter))
			pathsArgs = append(pathsArgs, eventName)

			// Absolute index with false-positive guard: if rel is 0, indexOf returned nothing
			idxColumns = append(idxColumns,
				fmt.Sprintf("        if(idx_%s > 0 AND rel_%s > 0, rel_%s + idx_%s, 0) AS idx_%s", prevLetter, letter, letter, prevLetter, letter))
		}
	}

	// ── Build the path slice ──
	// slice_start = max(1, idx_A - steps_before_A)
	// slice_end   = idx_LAST + steps_after_LAST   (or idx_A + steps_after_A if only 1 step)
	firstLetter := string(stepLetters[0])
	lastLetter := string(stepLetters[stepCount-1])
	stepsBefore := steps[0].StepsBefore
	if stepsBefore < 0 {
		stepsBefore = 0
	}
	stepsAfterLast := steps[stepCount-1].StepsAfter
	if stepsAfterLast <= 0 {
		stepsAfterLast = 3
	}

	// The WHERE clause: at minimum the first step must exist.
	// For multi-step, all anchors must be found sequentially.
	whereFilter := fmt.Sprintf("idx_%s > 0", firstLetter)
	if stepCount > 1 {
		whereFilter = fmt.Sprintf("idx_%s > 0", lastLetter) // If last anchor exists, all prior anchors exist too
	}

	pathsArgs = append(pathsArgs, stepsBefore, stepsAfterLast)

	// ── Build anchor_offsets: for each step, compute its relative position in the slice ──
	// anchor_offset_A = idx_A - slice_start
	// anchor_offset_B = idx_B - slice_start, etc.
	var anchorOffsetCols []string
	for i := range steps {
		letter := string(stepLetters[i])
		anchorOffsetCols = append(anchorOffsetCols,
			fmt.Sprintf("        idx_%s - slice_start AS anchor_%s", letter, letter))
	}

	pathsCTE := fmt.Sprintf(`
paths AS (
    SELECT
        actor_id,
%s,
        GREATEST(1, idx_%s - ?) AS slice_start,
        idx_%s + ? - slice_start + 1 AS slice_len,
        arraySlice(event_sequence, slice_start, slice_len) AS path,
%s
    FROM session_events
    WHERE %s
)`, strings.Join(idxColumns, ",\n"), firstLetter, lastLetter, strings.Join(anchorOffsetCols, ",\n"), whereFilter)

	finalArgs = append(finalArgs, pathsArgs...)

	// ── Build edges SELECT with step-prefixed column labels ──
	// For each position i in the path, determine which step group it belongs to
	// by comparing i to the anchor offsets. The label prefix is the letter of the
	// nearest anchor (e.g., "A-1", "A", "A+1", "B-1", "B", "B+1")
	//
	// Logic: build a CASE WHEN chain that assigns each i to a step segment.
	// For step X with anchor_X, the columns before it belong to "X-N" and after to "X+N".

	// Build the step label expression
	// For simplicity and correctness: compute step_label as letter + offset from nearest anchor
	var stepLabelParts []string
	for i := range steps {
		letter := string(stepLetters[i])
		if i == stepCount-1 {
			// Last step: everything at or after this anchor
			stepLabelParts = append(stepLabelParts,
				fmt.Sprintf("concat('%s', if(%%s - (anchor_%s + 1) >= 0, '+', ''), toString(%%s - (anchor_%s + 1)))", letter, letter, letter))
		} else {
			nextLetter := string(stepLetters[i+1])
			if i == 0 {
				// First step: from start until midpoint before next anchor
				stepLabelParts = append(stepLabelParts,
					fmt.Sprintf("if(%%s <= (anchor_%s + anchor_%s) / 2, concat('%s', if(%%s - (anchor_%s + 1) >= 0, '+', ''), toString(%%s - (anchor_%s + 1))), %%s)",
						letter, nextLetter, letter, letter, letter))
			} else {
				prevLetter := string(stepLetters[i-1])
				stepLabelParts = append(stepLabelParts,
					fmt.Sprintf("if(%%s > (anchor_%s + anchor_%s) / 2 AND %%s <= (anchor_%s + anchor_%s) / 2, concat('%s', if(%%s - (anchor_%s + 1) >= 0, '+', ''), toString(%%s - (anchor_%s + 1))), %%s)",
						prevLetter, letter, letter, nextLetter, letter, letter, letter))
			}
		}
	}

	// Actually, the step label logic with CASE WHEN is getting too complex for dynamic sprintf.
	// Let me use a cleaner approach: for each position i, find the nearest anchor offset and label it.
	// The simplest robust approach: use the FIRST anchor offset system where i maps to a step segment.

	// SIMPLEST APPROACH: Just use a single anchor_offset like before for now,
	// BUT prefix the step letter to each node based on which step segment it falls into.
	// For the initial multi-step release, we'll build the column labels using a
	// multiIf() chain that assigns step letters based on position relative to anchors.

	var multiIfParts []string
	for i := range steps {
		letter := string(stepLetters[i])
		if i < stepCount-1 {
			nextLetter := string(stepLetters[i+1])
			// Midpoint between this anchor and next anchor determines boundary
			multiIfParts = append(multiIfParts,
				fmt.Sprintf("%%s < (anchor_%s + anchor_%s + 2) / 2, concat('%s', if(%%s - (anchor_%s + 1) = 0, '', if(%%s - (anchor_%s + 1) > 0, '+', '')), toString(%%s - (anchor_%s + 1)))",
					letter, nextLetter, letter, letter, letter, letter))
		}
	}
	// Last step (default case)
	lastStepLabel := fmt.Sprintf("concat('%s', if(%%s - (anchor_%s + 1) = 0, '', if(%%s - (anchor_%s + 1) > 0, '+', '')), toString(%%s - (anchor_%s + 1)))",
		string(stepLetters[stepCount-1]), lastLetter, lastLetter, lastLetter)

	var stepLabelExpr string
	if len(multiIfParts) > 0 {
		// Build: multiIf(cond1, val1, cond2, val2, ..., defaultVal)
		stepLabelExpr = "multiIf(" + strings.Join(multiIfParts, ", ") + ", " + lastStepLabel + ")"
	} else {
		// Single step
		stepLabelExpr = lastStepLabel
	}

	// The edge expression uses position `i` for source and `i+1` for target
	// We need to substitute %s with the actual position variable
	sourceLabelExpr := substitutePos(stepLabelExpr, "i")
	targetLabelExpr := substitutePos(stepLabelExpr, "(i + 1)")

	// First step's anchor event name for aliasing
	firstEventName := steps[0].EventName
	finalArgs = append(finalArgs, firstEventName, firstEventName)

	edgesSelect := fmt.Sprintf(`
        path[i] || ' (' || %s || ')' as source,
        path[i+1] || ' (' || %s || ')' as target,`, sourceLabelExpr, targetLabelExpr)

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
edges AS (
    SELECT
        path[i] as raw_source,
        path[i+1] as raw_target,
        %s
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
    `, whereStmt, pathsCTE, edgesSelect)

	return query, finalArgs
}

// substitutePos replaces all %s placeholders in a multiIf expression with the given position variable.
func substitutePos(expr string, posVar string) string {
	return strings.ReplaceAll(expr, "%s", posVar)
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
