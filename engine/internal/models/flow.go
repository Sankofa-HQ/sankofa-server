package models

// FlowRequest specifies the parameters for querying user flows.
type FlowRequest struct {
	ProjectID          string    `json:"project_id"`
	GlobalDateRange    DateRange `json:"global_date_range"`
	StartEvent         string    `json:"start_event"` // The starting event to branch out from
	StartEventExpanded []string  `json:"-"`           // Internal: expanded list of events for virtual events
	MaxDepth           int       `json:"max_depth"`   // How many steps deep to calculate
	GlobalFilters      []Filter  `json:"global_filters"`
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
