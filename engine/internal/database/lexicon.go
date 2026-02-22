package database

import (
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"gorm.io/gorm"
)

// LexiconEvent represents a tracked event in the system.
type LexiconEvent struct {
	ID           string      `gorm:"primaryKey;type:varchar(32)" json:"id"`
	ProjectID    string      `gorm:"not null;index;uniqueIndex:idx_project_event_name;type:varchar(32)" json:"project_id"`
	Name         string      `gorm:"not null;index;uniqueIndex:idx_project_event_name" json:"name"` // The raw event name (e.g., "signup_completed")
	DisplayName  string      `json:"display_name"`                                                  // Friendly name (e.g., "Sign Up Completed")
	Description  string      `json:"description"`
	Hidden       bool        `gorm:"default:false" json:"hidden"`     // Hides from UI filters
	Dropped      bool        `gorm:"default:false" json:"dropped"`    // Instructs ingestion to drop this event
	IsVirtual    bool        `gorm:"default:false" json:"is_virtual"` // True if this is a merged/virtual event
	MergedIntoID *string     `gorm:"type:varchar(32)" json:"merged_into_id"`
	Tags         StringArray `gorm:"type:text" json:"tags"`
	CreatedAt    time.Time   `json:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at"`
}

func (e *LexiconEvent) BeforeCreate(tx *gorm.DB) (err error) {
	if e.ID == "" {
		id, err := gonanoid.New(21)
		if err != nil {
			return err
		}
		e.ID = "evt_" + id
	}
	return
}

// LexiconEventProperty represents a property associated with an event.
type LexiconEventProperty struct {
	ID           string      `gorm:"primaryKey;type:varchar(32)" json:"id"`
	ProjectID    string      `gorm:"not null;index;uniqueIndex:idx_project_event_prop_name;type:varchar(32)" json:"project_id"`
	EventID      *string     `gorm:"index;uniqueIndex:idx_project_event_prop_name;type:varchar(32)" json:"event_id"` // Null for global properties, set for event-specific
	Name         string      `gorm:"not null;index;uniqueIndex:idx_project_event_prop_name" json:"name"`             // The raw property name (e.g., "browser")
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

func (p *LexiconEventProperty) BeforeCreate(tx *gorm.DB) (err error) {
	if p.ID == "" {
		id, err := gonanoid.New(21)
		if err != nil {
			return err
		}
		p.ID = "prop_" + id
	}
	return
}

// LexiconProfileProperty represents a property associated with a User or Company profile.
type LexiconProfileProperty struct {
	ID           string      `gorm:"primaryKey;type:varchar(32)" json:"id"`
	ProjectID    string      `gorm:"not null;index;uniqueIndex:idx_project_profile_prop_name;type:varchar(32)" json:"project_id"`
	EntityType   string      `gorm:"default:'User';uniqueIndex:idx_project_profile_prop_name" json:"entity_type"` // 'User' or 'Company'
	Name         string      `gorm:"not null;index;uniqueIndex:idx_project_profile_prop_name" json:"name"`        // The raw property name (e.g., "email", "plan")
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

func (p *LexiconProfileProperty) BeforeCreate(tx *gorm.DB) (err error) {
	if p.ID == "" {
		id, err := gonanoid.New(21)
		if err != nil {
			return err
		}
		p.ID = "prof_" + id
	}
	return
}
