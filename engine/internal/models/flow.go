package models

// FlowStep represents a single step/anchor in a multi-step flow query.
type FlowStep struct {
	ID          int      `json:"id"`
	EventName   string   `json:"event_name"`
	StepsBefore int      `json:"steps_before"`
	StepsAfter  int      `json:"steps_after"`
	Filters     []Filter `json:"filters,omitempty"`
}

// FlowRequest specifies the parameters for querying user flows.
type FlowRequest struct {
	ProjectID       string    `json:"project_id"`
	Environment     string    `json:"environment"` // "live" or "test"
	GlobalDateRange DateRange `json:"global_date_range"`

	// ── Legacy flat fields (backward compat with old frontend) ──
	StartEvent         string   `json:"start_event"`
	StartEventExpanded []string `json:"-"`
	StepsBefore        int      `json:"steps_before"`
	StepsAfter         int      `json:"steps_after"`
	EndEvent           string   `json:"end_event,omitempty"`
	EndEventExpanded   []string `json:"-"`

	// ── New multi-step fields ──
	Steps []FlowStep `json:"steps,omitempty"`

	Breakdowns    []string `json:"breakdowns,omitempty"`
	GlobalFilters []Filter `json:"global_filters"`
	HiddenEvents  []string `json:"hidden_events,omitempty"`
	MaxRows       int      `json:"max_rows,omitempty"`
}

// FlowNode represents a node in the Sankey diagram for @nivo/sankey
type FlowNode struct {
	ID    string `json:"id"`
	Name  string `json:"name,omitempty"`
	Level int    `json:"level,omitempty"`
}

// FlowLink represents a directed link between two nodes in the Sankey diagram
type FlowLink struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Value  int    `json:"value"`
}

// FlowResult contains the final structure expected by the frontend's Sankey diagram
type FlowResult struct {
	Nodes []FlowNode `json:"nodes"`
	Links []FlowLink `json:"links"`
}
