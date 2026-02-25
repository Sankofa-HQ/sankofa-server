package database

import (
	"strings"
	"testing"

	"sankofa/engine/internal/models"
)

func TestBuildWindowFunnelQuery_Basic(t *testing.T) {
	req := models.FunnelRequest{
		ProjectID: "proj_123",
		Steps: []models.FunnelStep{
			{EventName: "sign up"},
			{EventName: "login"},
		},
	}

	query, args := BuildWindowFunnelQuery(req, 604800)

	if !strings.Contains(query, "windowFunnel(604800)") {
		t.Errorf("Expected query to contain windowFunnel(604800)")
	}
	if !strings.Contains(query, "event_name = 'sign up'") {
		t.Errorf("Expected query to contain step 1")
	}
	if len(args) != 1 || args[0] != "proj_123" {
		t.Errorf("Expected exactly 1 argument (project_id), got %d", len(args))
	}
}

func TestBuildSequenceMatchQuery_Simple(t *testing.T) {
	req := models.FunnelRequest{
		ProjectID: "proj_123",
		OrderMode: "strict",
		Steps: []models.FunnelStep{
			{Type: "event", EventName: "sign up"},
			{Type: "event", EventName: "login"},
		},
	}

	query, args := BuildSequenceMatchQuery(req, 604800)

	if !strings.Contains(query, "sequenceMatch('(?1)')") {
		t.Errorf("Expected level 1 sequenceMatch")
	}
	if !strings.Contains(query, "sequenceMatch('(?1)(?2)')") {
		// Because strict mode means transition is empty
		t.Errorf("Expected level 2 sequenceMatch for strict mode")
	}
	// args length: step 1 cond (0), step 2 cond(0) + where (project_id) = 1
	if len(args) != 1 {
		t.Errorf("Expected 1 arg, got %d", len(args))
	}
}

func TestBuildSequenceMatchQuery_Exclusion(t *testing.T) {
	req := models.FunnelRequest{
		ProjectID: "proj_123",
		OrderMode: "loose",
		Steps: []models.FunnelStep{
			{Type: "event", EventName: "add_to_cart"},
			{Type: "exclusion", EventName: "payment_failed"},
			{Type: "event", EventName: "checkout"},
		},
	}

	query, _ := BuildSequenceMatchQuery(req, 604800)

	if !strings.Contains(query, "sequenceMatch('(?1).*~(?2).*(?3)')") {
		t.Errorf("Expected sequenceMatch to generate exclusion pattern: (?1).*~(?2).*(?3). Got: %s", query)
	}
}

func TestBuildSequenceMatchQuery_TimeToNext(t *testing.T) {
	timeToNext := 600
	req := models.FunnelRequest{
		ProjectID: "proj_123",
		OrderMode: "loose",
		Steps: []models.FunnelStep{
			{Type: "event", EventName: "add_to_cart"},
			{Type: "event", EventName: "checkout", TimeToNext: &timeToNext},
		},
	}

	query, _ := BuildSequenceMatchQuery(req, 604800)

	if !strings.Contains(query, "sequenceMatch('(?1).*(?t<=600)(?2)')") {
		t.Errorf("Expected timeout syntax (?t<=600)")
	}
}
