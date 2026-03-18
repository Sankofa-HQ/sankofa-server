package models

import "time"

type FunnelRequest struct {
	ProjectID          string            `json:"project_id"`
	Environment        string            `json:"environment"` // "live" or "test"
	Timezone           string            `json:"timezone"` // Added
	GlobalDateRange    DateRange         `json:"global_date_range"`
	WindowValue        int               `json:"window_value"`
	WindowUnit         string            `json:"window_unit"` // "seconds", "minutes", "hours", "days", "weeks", "months", "sessions"
	OrderMode          string            `json:"order_mode"`  // "loose" or "strict"
	HoldConstants      []string          `json:"hold_constants"`
	GlobalFilters      []Filter          `json:"global_filters"`
	Breakdowns         []string          `json:"breakdowns"`
	ColorOverrides     map[string]string `json:"color_overrides,omitempty"`
	ExpandedBreakdowns [][]string        `json:"-"` // Internal: expanded child keys for each breakdown (set by pre-processing)
	Steps              []FunnelStep      `json:"steps"`
}

type DateRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type Filter struct {
	Property           string   `json:"property"`
	Operator           string   `json:"operator"` // "eq", "neq", "in", "contains"
	Values             []string `json:"values"`
	ExpandedProperties []string `json:"-"` // Internal: set by pre-processing for virtual/merged properties
}

type FunnelStep struct {
	ID             int      `json:"id"`
	Type           string   `json:"type"` // "event", "exclusion"
	EventName      string   `json:"event_name"`
	ExpandedEvents []string `json:"-"`
	Filters        []Filter `json:"filters"`
	TimeToNext     *int     `json:"time_to_next,omitempty"` // Seconds. Pointer so it can be null.
}

func RequiresSequenceMatch(req FunnelRequest) bool {
	for _, step := range req.Steps {
		if step.Type == "exclusion" || step.TimeToNext != nil {
			return true
		}
	}
	return false
}
