package database

import (
	"time"
)

// User represents a login (email/password).
type User struct {
	ID               uint      `gorm:"primaryKey" json:"id"`
	Email            string    `gorm:"uniqueIndex;not null" json:"email"`
	PasswordHash     string    `json:"-"` // Not exposed in JSON
	FullName         string    `json:"full_name"`
	AvatarURL        string    `json:"avatar_url"`
	CurrentProjectID *uint     `json:"current_project_id"` // Nullable
	CurrentProject   *Project  `json:"current_project" gorm:"foreignKey:CurrentProjectID"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// Organization is the billable entity.
type Organization struct {
	ID           uint      `gorm:"primaryKey" json:"id"`
	Name         string    `gorm:"not null" json:"name"`
	Slug         string    `gorm:"uniqueIndex;not null" json:"slug"`
	BillingEmail string    `json:"billing_email"`
	Plan         string    `gorm:"default:Free" json:"plan"` // Free, Pro, Enterprise
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Project is a container for events (e.g., Staging vs Prod).
type Project struct {
	ID             uint      `gorm:"primaryKey" json:"id"`
	OrganizationID uint      `gorm:"not null;index" json:"organization_id"` // FK
	Name           string    `gorm:"not null" json:"name"`
	APIKey         string    `gorm:"uniqueIndex;not null" json:"api_key"` // sk_live_...
	Timezone       string    `gorm:"default:'Africa/Accra'" json:"timezone"`
	Region         string    `gorm:"default:'eu-west-1'" json:"region"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// OrganizationMember links Users to Organizations.
type OrganizationMember struct {
	OrganizationID uint      `gorm:"primaryKey" json:"organization_id"`
	UserID         uint      `gorm:"primaryKey" json:"user_id"`
	Role           string    `gorm:"default:'Member'" json:"role"` // Owner, Member
	CreatedAt      time.Time `json:"created_at"`
}

// ProjectMember controls access to Projects.
type ProjectMember struct {
	ProjectID uint      `gorm:"primaryKey" json:"project_id"`
	UserID    uint      `gorm:"primaryKey" json:"user_id"`
	Role      string    `gorm:"default:'Viewer'" json:"role"` // Admin, Editor, Viewer
	CreatedAt time.Time `json:"created_at"`
}

// Team represents a group of users within an Organization.
type Team struct {
	ID             uint      `gorm:"primaryKey" json:"id"`
	OrganizationID uint      `gorm:"not null;index" json:"organization_id"`
	Name           string    `gorm:"not null" json:"name"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// TeamMember links Users to Teams.
type TeamMember struct {
	TeamID    uint      `gorm:"primaryKey" json:"team_id"`
	UserID    uint      `gorm:"primaryKey" json:"user_id"`
	Role      string    `gorm:"default:'Member'" json:"role"` // Member, Lead
	CreatedAt time.Time `json:"created_at"`
}

// TeamProject links Teams into Projects (e.g. "Mobile Devs" -> "Mobile App - Staging").
type TeamProject struct {
	TeamID    uint      `gorm:"primaryKey" json:"team_id"`
	ProjectID uint      `gorm:"primaryKey" json:"project_id"`
	Role      string    `gorm:"default:'Viewer'" json:"role"` // Viewer, Editor, Admin
	CreatedAt time.Time `json:"created_at"`
}
