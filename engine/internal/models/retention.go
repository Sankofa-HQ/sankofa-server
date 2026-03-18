package models

// RetentionRequest is the JSON body sent by the frontend Retention query builder.
type RetentionRequest struct {
	ProjectID           string            `json:"project_id"`
	Environment         string            `json:"environment"` // "live" or "test"
	Timezone            string            `json:"timezone"`
	StartEvent          string            `json:"start_event"`
	ExpandedStartEvent  []string          `json:"-"`
	ReturnEvent         string            `json:"return_event"`
	ExpandedReturnEvent []string          `json:"-"`
	GlobalDateRange     DateRange         `json:"global_date_range"`
	TimeWindow          int               `json:"time_window"` // e.g., 7 (intervals)
	Interval            string            `json:"interval"`    // "day", "week", "month", "hour"
	GlobalFilters       []Filter          `json:"global_filters"`
	Breakdowns          []string          `json:"breakdowns"`
	ColorOverrides      map[string]string `json:"color_overrides,omitempty"`
	ExpandedBreakdowns  [][]string        `json:"-"` // Internal: expanded child keys for each breakdown
}
