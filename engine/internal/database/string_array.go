package database

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
)

// StringArray is a SQLite-compatible replacement for pq.StringArray.
// It stores string slices as JSON text in the database (e.g., `["tag1","tag2"]`).
// It also gracefully handles legacy formats like `{tag1,tag2}` from PostgreSQL.
type StringArray []string

// Value implements the driver.Valuer interface for writing to DB.
func (a StringArray) Value() (driver.Value, error) {
	if a == nil {
		return "[]", nil
	}
	b, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// Scan implements the sql.Scanner interface for reading from DB.
func (a *StringArray) Scan(value interface{}) error {
	if value == nil {
		*a = StringArray{}
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case string:
		bytes = []byte(v)
	case []byte:
		bytes = v
	default:
		return fmt.Errorf("StringArray.Scan: unsupported type %T", value)
	}

	s := strings.TrimSpace(string(bytes))

	// Handle empty
	if s == "" || s == "null" || s == "NULL" {
		*a = StringArray{}
		return nil
	}

	// Handle PostgreSQL array format: {tag1,tag2}
	if strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") {
		inner := s[1 : len(s)-1]
		if inner == "" {
			*a = StringArray{}
			return nil
		}
		parts := strings.Split(inner, ",")
		result := make(StringArray, len(parts))
		for i, p := range parts {
			result[i] = strings.TrimSpace(p)
		}
		*a = result
		return nil
	}

	// Handle JSON array format: ["tag1","tag2"]
	return json.Unmarshal(bytes, a)
}
