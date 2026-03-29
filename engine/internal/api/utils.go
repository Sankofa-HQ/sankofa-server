package api

import (
	"fmt"
	"sankofa/engine/internal/database"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

// ParseTimeRangeSql parses a time range string and returns an SQL condition and its arguments.
// Supported formats:
// - "all" -> ""
// - "30d", "24h" (legacy) -> "col >= now() - INTERVAL ? SECOND"
// - "last_30_days", "last_24_hours" -> "col >= now() - INTERVAL ? DAY" (etc)
// - "since_2024-01-01" -> "col >= parseDateTimeBestEffort(?)"
// - "fixed_2024-01-01_2024-02-01" -> "col >= parseDateTimeBestEffort(?) AND col <= parseDateTimeBestEffort(?)"
func ParseTimeRangeSql(colName string, timeStr string) (string, []interface{}) {
	if timeStr == "all" || timeStr == "" {
		return "", nil
	}

	if strings.HasPrefix(timeStr, "last_") {
		parts := strings.Split(timeStr, "_")
		if len(parts) == 3 {
			val, err := strconv.Atoi(parts[1])
			if err == nil {
				unit := strings.ToUpper(parts[2])
				// Standardize ClickHouse interval units
				switch unit {
				case "HOURS", "HOUR":
					unit = "HOUR"
				case "DAYS", "DAY":
					unit = "DAY"
				case "WEEKS", "WEEK":
					unit = "WEEK"
				case "MONTHS", "MONTH":
					unit = "MONTH"
				case "YEARS", "YEAR":
					unit = "YEAR"
				default:
					unit = "DAY"
				}
				return fmt.Sprintf("%s >= now() - INTERVAL ? %s", colName, unit), []interface{}{val}
			}
		}
	}

	if strings.HasPrefix(timeStr, "since_") {
		startStr := strings.TrimPrefix(timeStr, "since_")
		return fmt.Sprintf("%s >= parseDateTimeBestEffort(?)", colName), []interface{}{startStr}
	}

	if strings.HasPrefix(timeStr, "fixed_") {
		rangeStr := strings.TrimPrefix(timeStr, "fixed_")
		parts := strings.Split(rangeStr, "_")
		if len(parts) >= 2 {
			// handle case where ISO dates might have underscores (they usually don't though, usually T or spaces)
			// But splitting by standard `YYYY-MM-DDTHH:mm:ss.000Z_YYYY-MM...`
			// So parts[0] is start, everything after first `_` is end if we just use strings.SplitN
			// Let's use SplitN
			parts = strings.SplitN(rangeStr, "_", 2)
			startStr := parts[0]
			endStr := parts[1]
			return fmt.Sprintf("%s >= parseDateTimeBestEffort(?) AND %s <= parseDateTimeBestEffort(?)", colName, colName), []interface{}{startStr, endStr}
		}
	}

	// Legacy fallback (e.g. "30d", "24h", "12m")
	var timeSeconds int64 = 0
	val, err := strconv.Atoi(timeStr[:len(timeStr)-1])
	if err == nil {
		unit := timeStr[len(timeStr)-1]
		switch unit {
		case 'h':
			timeSeconds = int64(val) * 3600
		case 'd':
			timeSeconds = int64(val) * 86400
		case 'm': // Could be minutes or months, legacy usually meant months for "12m", or minutes?
			if val == 6 || val == 12 {
				timeSeconds = int64(val) * 86400 * 30 // Approx months
			} else {
				timeSeconds = int64(val) * 60 // Minutes
			}
		}
		if timeSeconds > 0 {
			return fmt.Sprintf("%s >= now() - INTERVAL ? SECOND", colName), []interface{}{timeSeconds}
		}
	}

	return "", nil
}

// ExpandVirtualEventNames takes a slice of requested event names,
// looks them up in Lexicon, and replaces any virtual events with their child event names.
func ExpandVirtualEventNames(db *gorm.DB, projectID string, environment string, eventNames []string) []string {
	if len(eventNames) == 0 {
		return eventNames
	}

	// Fetch all virtual events matching these names
	var virtualEvents []database.LexiconEvent
	db.Where("project_id = ? AND environment = ? AND is_virtual = ? AND name IN ?", projectID, environment, true, eventNames).Find(&virtualEvents)

	if len(virtualEvents) == 0 {
		return eventNames // No translation needed
	}

	virtualIDs := []string{}
	virtualNameMap := make(map[string]bool)
	for _, ve := range virtualEvents {
		virtualIDs = append(virtualIDs, ve.ID)
		virtualNameMap[ve.Name] = true
	}

	// Fetch children
	var children []database.LexiconEvent
	db.Where("project_id = ? AND environment = ? AND merged_into_id IN ?", projectID, environment, virtualIDs).Find(&children)

	expandedNames := []string{}

	// Keep original names that aren't virtual
	for _, reqName := range eventNames {
		if !virtualNameMap[reqName] {
			expandedNames = append(expandedNames, reqName)
		}
	}

	// Add children names
	for _, child := range children {
		expandedNames = append(expandedNames, child.Name)
	}

	return expandedNames
}

// ApplyVirtualEventNames checks a list of raw events. If any event is merged into a Virtual Event,
// it replaces the event's EventName with the Virtual Event's DisplayName so the UI shows the merged identity.
func ApplyVirtualEventNames(db *gorm.DB, projectID string, environment string, events []Event) {
	if len(events) == 0 {
		return
	}

	rawNameSet := make(map[string]bool)
	for _, e := range events {
		rawNameSet[e.EventName] = true
	}

	var rawNames []string
	for n := range rawNameSet {
		rawNames = append(rawNames, n)
	}

	var children []database.LexiconEvent
	db.Where("project_id = ? AND environment = ? AND name IN ?", projectID, environment, rawNames).Find(&children)

	if len(children) == 0 {
		return
	}

	var parentIDs []string
	for _, c := range children {
		if c.MergedIntoID != nil && *c.MergedIntoID != "" {
			parentIDs = append(parentIDs, *c.MergedIntoID)
		}
	}

	if len(parentIDs) == 0 {
		return
	}

	var parents []database.LexiconEvent
	db.Where("project_id = ? AND environment = ? AND id IN ?", projectID, environment, parentIDs).Find(&parents)

	// Map parent ID → DisplayName (fall back to Name if DisplayName is empty)
	parentDisplayMap := make(map[string]string)
	for _, p := range parents {
		if p.DisplayName != "" {
			parentDisplayMap[p.ID] = p.DisplayName
		} else {
			parentDisplayMap[p.ID] = p.Name
		}
	}

	rawToVirtual := make(map[string]string)
	for _, c := range children {
		if c.MergedIntoID != nil {
			if displayName, ok := parentDisplayMap[*c.MergedIntoID]; ok {
				rawToVirtual[c.Name] = displayName
			}
		}
	}

	for i := range events {
		if virtualName, ok := rawToVirtual[events[i].EventName]; ok {
			events[i].EventName = virtualName
		}
	}
}

