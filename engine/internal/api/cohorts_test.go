package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildCohortSQL(t *testing.T) {
	tests := []struct {
		name      string
		jsonRules string
		wantSQL   string
	}{
		{
			name: "Simple Event Filter",
			jsonRules: `{
				"logic": "AND",
				"filters": [
					{
						"type": "event",
						"event_name": "Checkout",
						"timeframe_days": 30,
						"operator": ">=",
						"value": 3
					}
				]
			}`,
			wantSQL: "event_name = ? AND timestamp >= now() - INTERVAL ? DAY GROUP BY distinct_id HAVING count() >= ?",
		},
		{
			name: "User Property Filter",
			jsonRules: `{
				"logic": "AND",
				"filters": [
					{
						"type": "user_property",
						"property_name": "country",
						"operator": "==",
						"value": "Ghana"
					}
				]
			}`,
			wantSQL: "properties[?] = ?",
		},
		{
			name: "AND Logic",
			jsonRules: `{
				"logic": "AND",
				"filters": [
					{
						"type": "event",
						"event_name": "Signup",
						"operator": ">",
						"value": 0
					},
					{
						"type": "user_property",
						"property_name": "plan",
						"operator": "==",
						"value": "Pro"
					}
				]
			}`,
			wantSQL: "INTERSECT",
		},
		{
			name: "OR Logic",
			jsonRules: `{
				"logic": "OR",
				"filters": [
					{
						"type": "user_property",
						"property_name": "country",
						"operator": "==",
						"value": "US"
					},
					{
						"type": "user_property",
						"property_name": "country",
						"operator": "==",
						"value": "UK"
					}
				]
			}`,
			wantSQL: "UNION DISTINCT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ast CohortAST
			if err := json.Unmarshal([]byte(tt.jsonRules), &ast); err != nil {
				t.Fatalf("Failed to unmarshal JSON: %v", err)
			}

			gotSQL, args := BuildCohortSQL("1", "live", ast)

			normGot := strings.Join(strings.Fields(gotSQL), " ")
			normWant := strings.Join(strings.Fields(tt.wantSQL), " ")

			// Basic verification
			if !strings.Contains(normGot, normWant) {
				t.Errorf("BuildCohortSQL() = %v, want substring %v", normGot, normWant)
			}

			// Check args count
			expectedArgs := 0
			for _, f := range ast.Filters {
				if f.Type == "event" {
					expectedArgs += 4 // projectID, eventName, days, value
				} else if f.Type == "user_property" {
					expectedArgs += 3 // projectID, key, value
				}
			}
			if len(args) != expectedArgs {
				t.Errorf("Args count = %d, want %d", len(args), expectedArgs)
			}
		})
	}
}
