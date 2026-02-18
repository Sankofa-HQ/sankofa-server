package database

import (
	"time"
)

// LexiconEvent represents a tracked event in the system.
type LexiconEvent struct {
	ID          uint        `gorm:"primaryKey" json:"id"`
	ProjectID   uint        `gorm:"not null;index;uniqueIndex:idx_project_event_name" json:"project_id"`
	Name        string      `gorm:"not null;index;uniqueIndex:idx_project_event_name" json:"name"` // The raw event name (e.g., "signup_completed")
	DisplayName string      `json:"display_name"`                                                  // Friendly name (e.g., "Sign Up Completed")
	Description string      `json:"description"`
	Hidden      bool        `gorm:"default:false" json:"hidden"`  // Hides from UI filters
	Dropped     bool        `gorm:"default:false" json:"dropped"` // Instructs ingestion to drop this event
	Tags        StringArray `gorm:"type:text" json:"tags"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
}

// LexiconEventProperty represents a property associated with an event.
type LexiconEventProperty struct {
	ID           uint        `gorm:"primaryKey" json:"id"`
	ProjectID    uint        `gorm:"not null;index;uniqueIndex:idx_project_event_prop_name" json:"project_id"`
	EventID      *uint       `gorm:"index;uniqueIndex:idx_project_event_prop_name" json:"event_id"`      // Null for global properties, set for event-specific
	Name         string      `gorm:"not null;index;uniqueIndex:idx_project_event_prop_name" json:"name"` // The raw property name (e.g., "browser")
	DisplayName  string      `json:"display_name"`
	Description  string      `json:"description"`
	ExampleValue string      `json:"example_value"` // Sample value for documentation
	Hidden       bool        `gorm:"default:false" json:"hidden"`
	Dropped      bool        `gorm:"default:false" json:"dropped"`
	Tags         StringArray `gorm:"type:text" json:"tags"`
	Type         string      `json:"type"` // string, number, boolean, date, object, list
	CreatedAt    time.Time   `json:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at"`
}

// LexiconProfileProperty represents a property associated with a User or Company profile.
type LexiconProfileProperty struct {
	ID           uint        `gorm:"primaryKey" json:"id"`
	ProjectID    uint        `gorm:"not null;index" json:"project_id"`
	EntityType   string      `gorm:"default:'User'" json:"entity_type"` // 'User' or 'Company'
	Name         string      `gorm:"not null;index" json:"name"`        // The raw property name (e.g., "email", "plan")
	DisplayName  string      `json:"display_name"`
	Description  string      `json:"description"`
	ExampleValue string      `json:"example_value"`
	Hidden       bool        `gorm:"default:false" json:"hidden"`
	Dropped      bool        `gorm:"default:false" json:"dropped"`
	Tags         StringArray `gorm:"type:text" json:"tags"`
	Type         string      `json:"type"`
	CreatedAt    time.Time   `json:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at"`
}
