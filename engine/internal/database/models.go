package database

import (
	"encoding/json"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"gorm.io/gorm"
)

// User represents a login (email/password).
type User struct {
	ID               string    `gorm:"primaryKey;type:varchar(32)" json:"id"`
	Email            string    `gorm:"uniqueIndex;not null" json:"email"`
	PasswordHash     string    `json:"-"` // Not exposed in JSON
	FullName         string    `json:"full_name"`
	AvatarURL        string    `json:"avatar_url"`
	CurrentProjectID *string   `json:"current_project_id" gorm:"type:varchar(32)"` // Nullable
	CurrentProject   *Project  `json:"current_project" gorm:"foreignKey:CurrentProjectID"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func (u *User) BeforeCreate(tx *gorm.DB) (err error) {
	if u.ID == "" {
		id, err := gonanoid.New(21)
		if err != nil {
			return err
		}
		u.ID = "usr_" + id
	}
	return
}

// Organization is the billable entity.
type Organization struct {
	ID           string    `gorm:"primaryKey;type:varchar(32)" json:"id"`
	Name         string    `gorm:"not null" json:"name"`
	Slug         string    `gorm:"uniqueIndex;not null" json:"slug"`
	BillingEmail string    `json:"billing_email"`
	Plan         string    `gorm:"default:Free" json:"plan"` // Free, Pro, Enterprise
	CompanySize  string    `json:"company_size"`
	Industry     string    `json:"industry"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (o *Organization) BeforeCreate(tx *gorm.DB) (err error) {
	if o.ID == "" {
		id, err := gonanoid.New(21)
		if err != nil {
			return err
		}
		o.ID = "org_" + id
	}
	return
}

// Project is a container for events (e.g., Staging vs Prod).
type Project struct {
	ID                string        `gorm:"primaryKey;type:varchar(32)" json:"id"`
	OrganizationID    string        `gorm:"not null;index;type:varchar(32)" json:"organization_id"` // FK
	Organization      *Organization `json:"organization" gorm:"foreignKey:OrganizationID"`
	Name              string        `gorm:"not null" json:"name"`
	APIKey            string        `gorm:"uniqueIndex;not null" json:"api_key"` // sk_live_...
	TestAPIKey        string        `gorm:"uniqueIndex" json:"test_api_key"`     // sk_test_...
	Environment       string        `gorm:"default:'live'" json:"environment"`   // live, test
	Timezone          string        `gorm:"default:'Africa/Accra'" json:"timezone"`
	Region            string        `gorm:"default:'eu-west-1'" json:"region"`
	AutoApproveEvents bool          `gorm:"default:true" json:"auto_approve_events"` // If false, new events enter quarantine as 'pending'
	CreatedByID       string        `json:"created_by_id" gorm:"column:created_by_id;index;type:varchar(32)"`
	CreatedBy         *User         `json:"created_by" gorm:"foreignKey:CreatedByID"`
	CreatedAt         time.Time     `json:"created_at"`
	UpdatedAt         time.Time     `json:"updated_at"`
}

func (p *Project) BeforeCreate(tx *gorm.DB) (err error) {
	if p.ID == "" {
		id, err := gonanoid.New(21)
		if err != nil {
			return err
		}
		p.ID = "proj_" + id
	}
	return
}

// OrganizationMember links Users to Organizations.
type OrganizationMember struct {
	OrganizationID string        `gorm:"primaryKey;type:varchar(32)" json:"organization_id"`
	Organization   *Organization `json:"organization" gorm:"foreignKey:OrganizationID"`
	UserID         string        `gorm:"primaryKey;type:varchar(32)" json:"user_id"`
	Role           string        `gorm:"default:'Member'" json:"role"` // Owner, Member
	CreatedAt      time.Time     `json:"created_at"`
}

// OrganizationInvite stores pending invitations for unregistered users.
type OrganizationInvite struct {
	ID             string        `gorm:"primaryKey;type:varchar(32)" json:"id"`
	OrganizationID string        `gorm:"not null;index;type:varchar(32)" json:"organization_id"`
	Organization   *Organization `json:"organization" gorm:"foreignKey:OrganizationID"`
	Email          string        `gorm:"not null;index" json:"email"`
	Role           string        `gorm:"default:'Member'" json:"role"` // Org role (Owner, Member)
	Token          string        `gorm:"uniqueIndex;not null;type:varchar(64)" json:"token"`
	ProjectIDs     string        `gorm:"type:text" json:"project_ids"` // comma separated or JSON string
	TeamIDs        string        `gorm:"type:text" json:"team_ids"`    // comma separated or JSON string
	ProjectRole    string        `gorm:"default:'Viewer'" json:"project_role"`
	ExpiresAt      time.Time     `json:"expires_at"`
	Accepted       bool          `gorm:"default:false" json:"accepted"`
	CreatedAt      time.Time     `json:"created_at"`
}

func (oi *OrganizationInvite) BeforeCreate(tx *gorm.DB) (err error) {
	if oi.ID == "" {
		id, err := gonanoid.New(21)
		if err != nil {
			return err
		}
		oi.ID = "inv_" + id
	}
	if oi.Token == "" {
		token, err := gonanoid.New(32)
		if err != nil {
			return err
		}
		oi.Token = token
	}
	return
}

// ProjectMember controls access to Projects.
type ProjectMember struct {
	ProjectID string    `gorm:"primaryKey;type:varchar(32)" json:"project_id"`
	Project   *Project  `json:"project" gorm:"foreignKey:ProjectID"`
	UserID    string    `gorm:"primaryKey;type:varchar(32)" json:"user_id"`
	Role      string    `gorm:"default:'Viewer'" json:"role"` // Admin, Editor, Viewer
	CreatedAt time.Time `json:"created_at"`
}

// Team represents a group of users within an Organization.
type Team struct {
	ID             string    `gorm:"primaryKey;type:varchar(32)" json:"id"`
	OrganizationID string    `gorm:"not null;index;type:varchar(32)" json:"organization_id"`
	Name           string    `gorm:"not null" json:"name"`
	Description    string    `json:"description"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (t *Team) BeforeCreate(tx *gorm.DB) (err error) {
	if t.ID == "" {
		id, err := gonanoid.New(21)
		if err != nil {
			return err
		}
		t.ID = "team_" + id
	}
	return
}

// TeamMember links Users to Teams.
type TeamMember struct {
	TeamID    string    `gorm:"primaryKey;type:varchar(32)" json:"team_id"`
	UserID    string    `gorm:"primaryKey;type:varchar(32)" json:"user_id"`
	Role      string    `gorm:"default:'Member'" json:"role"` // Member, Lead
	CreatedAt time.Time `json:"created_at"`
}

// TeamProject links Teams into Projects (e.g. "Mobile Devs" -> "Mobile App - Staging").
type TeamProject struct {
	TeamID    string    `gorm:"primaryKey;type:varchar(32)" json:"team_id"`
	ProjectID string    `gorm:"primaryKey;type:varchar(32)" json:"project_id"`
	Role      string    `gorm:"default:'Viewer'" json:"role"` // Viewer, Editor, Admin
	CreatedAt time.Time `json:"created_at"`
}

// Plan defines the limits for an organization.
type Plan struct {
	Name         string    `gorm:"primaryKey" json:"name"` // Free, Pro, Enterprise
	EventLimit   int64     `json:"event_limit"`
	ProfileLimit int64     `json:"profile_limit"`
	ReplayLimit  int64     `json:"replay_limit"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Cohort represents a user group (Dynamic or Static)
type Cohort struct {
	ID          string    `gorm:"primaryKey;type:varchar(32)" json:"id"`
	ProjectID   string    `gorm:"not null;index;type:varchar(32)" json:"project_id"`
	Name        string    `gorm:"not null" json:"name"`
	Description string    `json:"description"`
	Type        string    `json:"type"` // "dynamic" or "static"
	Rules       []byte    `gorm:"type:text" json:"rules"`
	IsVisible   bool      `gorm:"default:true" json:"is_visible"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	CreatedByID string    `json:"created_by_id" gorm:"type:varchar(32)"`
	CreatedBy   *User     `json:"created_by" gorm:"foreignKey:CreatedByID"`
}

func (c *Cohort) BeforeCreate(tx *gorm.DB) (err error) {
	if c.ID == "" {
		id, err := gonanoid.New(21)
		if err != nil {
			return err
		}
		c.ID = "coh_" + id
	}
	return
}

// SavedFunnel represents a user-saved funnel query
type SavedFunnel struct {
	ID          string          `gorm:"primaryKey;type:varchar(32)" json:"id"`
	ProjectID   string          `gorm:"not null;index;type:varchar(32)" json:"project_id"`
	Name        string          `gorm:"not null" json:"name"`
	Description string          `json:"description"`
	QueryAST    json.RawMessage `gorm:"type:text" json:"query_ast"` // The FunnelsQueryAST
	IsPinned    bool            `gorm:"default:false" json:"is_pinned"`
	CreatedByID string          `json:"created_by_id" gorm:"type:varchar(32)"`
	CreatedBy   *User           `json:"created_by" gorm:"foreignKey:CreatedByID"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

func (s *SavedFunnel) BeforeCreate(tx *gorm.DB) (err error) {
	if s.ID == "" {
		id, err := gonanoid.New(21)
		if err != nil {
			return err
		}
		s.ID = "fun_" + id
	}
	return
}
