package models

// InsightMetric represents a single metric/series the user wants to analyze.
type InsightMetric struct {
	EventName      string   `json:"event_name"`
	ExpandedEvents []string `json:"-"`                       // Internal: set by pre-processing for virtual/merged events
	MathType       string   `json:"math_type"`               // "uniques", "totals", "property_sum", "property_avg"
	MathProperty   string   `json:"math_property,omitempty"` // Required when math_type is property_sum or property_avg
	Filters        []Filter `json:"filters"`                 // Per-metric inline filters
}

// InsightRequest is the JSON body sent by the frontend Insights query builder.
type InsightRequest struct {
	ProjectID          string          `json:"project_id"`
	GlobalDateRange    DateRange       `json:"global_date_range"`
	Interval           string          `json:"interval"` // "day", "week", "month"
	Metrics            []InsightMetric `json:"metrics"`
	GlobalFilters      []Filter        `json:"global_filters"`
	Breakdowns         []string        `json:"breakdowns"`
	ExpandedBreakdowns [][]string      `json:"-"` // Internal: expanded child keys for each breakdown
}
