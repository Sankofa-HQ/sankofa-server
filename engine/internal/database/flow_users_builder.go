package database

import (
	"fmt"
	"strings"

	"sankofa/engine/internal/models"
)

// BuildFlowUsersQuery generates a ClickHouse query to extract distinct users
// that passed through a specific node or link in a flow.
func BuildFlowUsersQuery(req models.FlowRequest, nodeID string) (string, []any) {
	originalQuery, originalArgs := BuildFlowQuery(req)

	withStart := strings.Index(originalQuery, "WITH ")
	selectStart := strings.LastIndex(originalQuery, "SELECT source, target")

	if withStart == -1 || selectStart == -1 {
		return "SELECT distinct(distinct_id) FROM events WHERE 1=0", []any{}
	}

	ctes := originalQuery[withStart+5 : selectStart]

	isMultiStep := strings.Contains(ctes, "lbl_idx")
	var hasSysConversion bool
	for _, bd := range req.Breakdowns {
		if bd == "sys_conversion" {
			hasSysConversion = true
			break
		}
	}

	// Determine what we're searching for
	isLink := strings.HasPrefix(nodeID, "LINK::")
	var sourceID, targetID string
	if isLink {
		parts := strings.Split(strings.TrimPrefix(nodeID, "LINK::"), "::")
		if len(parts) >= 2 {
			sourceID = parts[0]
			targetID = parts[1]
		}
	} else {
		sourceID = nodeID
	}

	searchSource := sourceID
	searchTarget := targetID
	if strings.Contains(searchSource, " - Converted") {
		searchSource = strings.ReplaceAll(searchSource, " - Converted", "")
	} else if strings.Contains(searchSource, " - Did Not Convert") {
		searchSource = strings.ReplaceAll(searchSource, " - Did Not Convert", "")
	}
	if strings.Contains(searchTarget, " - Converted") {
		searchTarget = strings.ReplaceAll(searchTarget, " - Converted", "")
	} else if strings.Contains(searchTarget, " - Did Not Convert") {
		searchTarget = strings.ReplaceAll(searchTarget, " - Did Not Convert", "")
	}

	var query string

	if isMultiStep {
		query = fmt.Sprintf(`
WITH %s
SELECT DISTINCT actor_id as distinct_id
FROM paths
ARRAY JOIN arrayEnumerate(lbl_idx) AS k
WHERE k < length(lbl_idx) AND path[lbl_idx[k]] != ''
`, ctes)

		sourceFormat := "path[lbl_idx[k]] || ' (' || labels[lbl_idx[k]] || ')'"
		targetFormat := "path[lbl_idx[k+1]] || ' (' || labels[lbl_idx[k+1]] || ')'"
		
		if hasSysConversion {
			sourceFormat += " || ' - ' || conversion_status"
			targetFormat += " || ' - ' || conversion_status"
		}

		if isLink {
			if targetID == "Dropoff-end" {
				// User reached source, but had no next step
				query += fmt.Sprintf(`  AND %s = ? AND k = length(lbl_idx) `, sourceFormat)
				originalArgs = append(originalArgs, sourceID)
			} else {
				// User went from source to target
				query += fmt.Sprintf(`  AND %s = ? AND (k+1 <= length(lbl_idx) AND path[lbl_idx[k+1]] != '' AND %s = ?) `, sourceFormat, targetFormat)
				originalArgs = append(originalArgs, sourceID, targetID)
			}
		} else if sourceID == "Dropoff-end" {
			// All dropoffs (path ended here without going further)
			query += fmt.Sprintf(`  AND k = length(lbl_idx) `)
		} else {
			// Normal node search
			query += fmt.Sprintf(`  AND (
        %s = ?
        OR
        (k+1 <= length(lbl_idx) AND path[lbl_idx[k+1]] != '' AND %s = ?)
    )`, sourceFormat, targetFormat)
			originalArgs = append(originalArgs, sourceID, sourceID)
		}

	} else {
		// Legacy mode
		query = fmt.Sprintf(`
WITH %s
SELECT DISTINCT actor_id as distinct_id
FROM paths
ARRAY JOIN arrayEnumerate(path) AS i
WHERE i < length(path) AND path[i] != ''
`, ctes)

		sourceFormat := "if(i - (anchor_offset + 1) = 0, path[i], path[i]) || ' (Step ' || toString(i - (anchor_offset + 1)) || ')'"
		targetFormat := "if(i + 1 - (anchor_offset + 1) = 0, path[i+1], path[i+1]) || ' (Step ' || toString(i + 1 - (anchor_offset + 1)) || ')'"

		if hasSysConversion {
			sourceFormat += " || ' - ' || conversion_status"
			targetFormat += " || ' - ' || conversion_status"
		}

		if isLink {
			if targetID == "Dropoff-end" {
				query += fmt.Sprintf(`  AND %s = ? AND i = length(path) `, sourceFormat)
				originalArgs = append(originalArgs, sourceID)
			} else {
				query += fmt.Sprintf(`  AND %s = ? AND (i+1 <= length(path) AND path[i+1] != '' AND %s = ?) `, sourceFormat, targetFormat)
				originalArgs = append(originalArgs, sourceID, targetID)
			}
		} else if sourceID == "Dropoff-end" {
			query += fmt.Sprintf(`  AND i = length(path) `)
		} else {
			query += fmt.Sprintf(`  AND (
        %s = ?
        OR
        (i+1 <= length(path) AND path[i+1] != '' AND %s = ?)
    )`, sourceFormat, targetFormat)
			originalArgs = append(originalArgs, sourceID, sourceID)
		}
	}

	return query, originalArgs
}
