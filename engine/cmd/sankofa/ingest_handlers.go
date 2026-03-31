package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"sankofa/engine/internal/database"
	"sankofa/engine/internal/utils"

	"github.com/gofiber/fiber/v2"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/ua-parser/uap-go/uaparser"
	"gorm.io/gorm"
)

var uaParser *uaparser.Parser

func init() {
	uaParser = uaparser.NewFromSaved()
}

type analyticsBatchRequest struct {
	Operations []analyticsBatchOperation `json:"operations"`
}

type analyticsBatchOperation struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func newTrackIngestHandler(db *gorm.DB, eventStream chan<- AnalyticsEvent) fiber.Handler {
	return func(c *fiber.Ctx) error {
		project, environment, err := resolveProjectForAPIKey(db, c.Get("x-api-key"), c.Get("Origin"), normalizedClientIP(c.IP()))
		if err != nil {
			return err
		}

		var event AnalyticsEvent
		if err := c.BodyParser(&event); err != nil {
			return c.SendStatus(fiber.StatusBadRequest)
		}

		finalizeAnalyticsEvent(&event, project, environment, normalizedClientIP(c.IP()), c.Get("User-Agent"))

		// ⚡️ BATTERY PROTECTION: If we still don't have a DistinctID, 
		// we return 202 Accepted. This stops the SDK from retrying broken data.
		if isGarbageID(event.DistinctID) || event.DistinctID == "" {
			log.Printf("⚠️  Discarding unidentifiable event (Project: %s). Missing DistinctID.", project.ID)
			return c.Status(202).JSON(fiber.Map{"ok": true, "status": "discarded"})
		}

		if err := enqueueAnalyticsEvent(eventStream, event); err != nil {
			return err
		}

		return c.SendStatus(fiber.StatusOK)
	}
}

func newPeopleIngestHandler(db *gorm.DB, personStream chan<- PersonProfile) fiber.Handler {
	return func(c *fiber.Ctx) error {
		project, environment, err := resolveProjectForAPIKey(db, c.Get("x-api-key"), c.Get("Origin"), normalizedClientIP(c.IP()))
		if err != nil {
			return err
		}

		var person PersonProfile
		if err := c.BodyParser(&person); err != nil {
			return c.SendStatus(fiber.StatusBadRequest)
		}

		finalizePersonProfile(&person, project, environment, normalizedClientIP(c.IP()), c.Get("User-Agent"))

		// ⚡️ BATTERY PROTECTION
		if isGarbageID(person.DistinctID) || person.DistinctID == "" {
			log.Printf("⚠️  Discarding unidentifiable person (Project: %s). Missing DistinctID.", project.ID)
			return c.Status(202).JSON(fiber.Map{"ok": true, "status": "discarded"})
		}

		if err := enqueuePersonProfile(personStream, person); err != nil {
			return err
		}

		return c.SendStatus(fiber.StatusOK)
	}
}

func newAliasIngestHandler(db *gorm.DB, aliasStream chan<- PersonAlias) fiber.Handler {
	return func(c *fiber.Ctx) error {
		project, environment, err := resolveProjectForAPIKey(db, c.Get("x-api-key"), c.Get("Origin"), normalizedClientIP(c.IP()))
		if err != nil {
			return err
		}

		var alias PersonAlias
		if err := c.BodyParser(&alias); err != nil {
			return c.SendStatus(fiber.StatusBadRequest)
		}

		finalizePersonAlias(&alias, project, environment)

		// ⚡️ BATTERY PROTECTION
		if isGarbageID(alias.DistinctID) || alias.DistinctID == "" {
			log.Printf("⚠️  Discarding unidentifiable alias (Project: %s). Missing DistinctID.", project.ID)
			return c.Status(202).JSON(fiber.Map{"ok": true, "status": "discarded"})
		}

		if err := enqueuePersonAlias(aliasStream, alias); err != nil {
			return err
		}

		return c.SendStatus(fiber.StatusOK)
	}
}

func newBatchIngestHandler(
	db *gorm.DB,
	eventStream chan<- AnalyticsEvent,
	personStream chan<- PersonProfile,
	aliasStream chan<- PersonAlias,
) fiber.Handler {
	return func(c *fiber.Ctx) error {
		project, environment, err := resolveProjectForAPIKey(db, c.Get("x-api-key"), c.Get("Origin"), normalizedClientIP(c.IP()))
		if err != nil {
			return err
		}

		var request analyticsBatchRequest
		if err := json.Unmarshal(c.Body(), &request); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid batch payload"})
		}

		if len(request.Operations) == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "No operations provided"})
		}

		clientIP := normalizedClientIP(c.IP())
		events := make([]AnalyticsEvent, 0)
		people := make([]PersonProfile, 0)
		aliases := make([]PersonAlias, 0)

		for _, operation := range request.Operations {
			switch operation.Type {
			case "track":
				var event AnalyticsEvent
				if err := json.Unmarshal(operation.Payload, &event); err != nil {
					return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid track payload"})
				}
				finalizeAnalyticsEvent(&event, project, environment, clientIP, c.Get("User-Agent"))
				if isGarbageID(event.DistinctID) || event.DistinctID == "" { continue }
				events = append(events, event)
			case "people":
				var person PersonProfile
				if err := json.Unmarshal(operation.Payload, &person); err != nil {
					return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid people payload"})
				}
				finalizePersonProfile(&person, project, environment, clientIP, c.Get("User-Agent"))
				if isGarbageID(person.DistinctID) || person.DistinctID == "" { continue }
				people = append(people, person)
			case "alias":
				var alias PersonAlias
				if err := json.Unmarshal(operation.Payload, &alias); err != nil {
					return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid alias payload"})
				}
				finalizePersonAlias(&alias, project, environment)
				if isGarbageID(alias.DistinctID) || alias.DistinctID == "" { continue }
				aliases = append(aliases, alias)
			default:
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": fmt.Sprintf("Unsupported operation type: %s", operation.Type)})
			}
		}

		// ⚡️ BATTERY PROTECTION (Batch Level): If zero valid operations remain, 
		// return 202 to stop retries if the entire batch was 'garbage'.
		if len(events) == 0 && len(people) == 0 && len(aliases) == 0 && len(request.Operations) > 0 {
			return c.Status(202).JSON(fiber.Map{"ok": true, "status": "discarded_all"})
		}

		for _, alias := range aliases {
			if err := enqueuePersonAlias(aliasStream, alias); err != nil {
				return err
			}
		}

		for _, person := range people {
			if err := enqueuePersonProfile(personStream, person); err != nil {
				return err
			}
		}

		for _, event := range events {
			if err := enqueueAnalyticsEvent(eventStream, event); err != nil {
				return err
			}
		}

		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"ok":                  true,
			"project_id":          project.ID,
			"project_name":        project.Name,
			"environment":         environment,
			"operations_received": len(request.Operations),
			"events_received":     len(events),
			"people_received":     len(people),
			"aliases_received":    len(aliases),
		})
	}
}

func resolveProjectForAPIKey(db *gorm.DB, apiKey string, origin string, clientIP string) (database.Project, string, error) {
	if apiKey == "" {
		log.Println("⚠️ Missing API Key")
		return database.Project{}, "", fiber.NewError(fiber.StatusUnauthorized, "Missing API Key")
	}

	var project database.Project
	environment := ""
	if err := db.Where("api_key = ?", apiKey).First(&project).Error; err == nil {
		environment = "live"
	} else if err := db.Where("test_api_key = ?", apiKey).First(&project).Error; err == nil {
		environment = "test"
	} else {
		log.Printf("⚠️  Invalid API Key attempt: %s (IP: %s)", apiKey, clientIP)
		return database.Project{}, "", fiber.NewError(fiber.StatusForbidden, "Invalid API Key")
	}

	if origin != "" {
		if project.AuthorizedDomains != "" {
			authorized := false
			domains := strings.Split(project.AuthorizedDomains, ",")
			for _, d := range domains {
				if strings.TrimSpace(d) == origin {
					authorized = true
					break
				}
			}
			if !authorized {
				log.Printf("⚠️ Unauthorized Origin: %s for project: %s", origin, project.ID)
				return database.Project{}, "", fiber.NewError(fiber.StatusForbidden, "Unauthorized Origin")
			}
		}
	} else {
		if project.AuthorizedIPs != "" {
			authorized := false
			ips := strings.Split(project.AuthorizedIPs, ",")
			for _, ip := range ips {
				if strings.TrimSpace(ip) == clientIP {
					authorized = true
					break
				}
			}
			if !authorized {
				log.Printf("⚠️ Unauthorized IP: %s for project: %s", clientIP, project.ID)
				return database.Project{}, "", fiber.NewError(fiber.StatusForbidden, "Unauthorized IP Address")
			}
		}
	}

	return project, environment, nil
}

func finalizeAnalyticsEvent(event *AnalyticsEvent, project database.Project, environment string, clientIP string, userAgent string) {
	nanoID, _ := gonanoid.New(21)
	event.ID = "evt_" + nanoID
	event.TenantID = fmt.Sprint(project.ID)
	event.ProjectID = fmt.Sprint(project.ID)
	event.OrganizationID = fmt.Sprint(project.OrganizationID)
	event.Environment = environment
	
	// 🕒 PRESERVE CLIENT TIMESTAMP: If the SDK sent a timestamp, keep it. 
	// Otherwise, default to server time.
	if event.Timestamp.IsZero() {
		// Fallback check in properties (common in Segment/PostHog styles)
		if ts, ok := event.Properties["$timestamp"].(string); ok {
			event.Timestamp = parseTimestamp(ts)
		} else if ts, ok := event.Properties["$time"].(string); ok {
			event.Timestamp = parseTimestamp(ts)
		} else {
			event.Timestamp = time.Now()
		}
	}

	if event.Properties == nil {
		event.Properties = make(map[string]any)
	}
	if event.DefaultProperties == nil {
		event.DefaultProperties = make(map[string]any)
	}

	enrichWithGeoIP(clientIP, event.DefaultProperties)
	enrichWithUserAgent(userAgent, event)

	if sessionID, ok := event.Properties["$session_id"].(string); ok {
		event.SessionID = sessionID
	}

	if city, ok := event.DefaultProperties["$city"].(string); ok {
		event.City = city
	}
	if region, ok := event.DefaultProperties["$region"].(string); ok {
		event.Region = region
	}
	if country, ok := event.DefaultProperties["$country"].(string); ok {
		event.Country = country
	}
	// Note: OS, Browser, DeviceModel are set by enrichWithUserAgent
	if database.Store != nil {
		props := make(map[string]interface{}, len(event.Properties))
		for key, value := range event.Properties {
			props[key] = value
		}
		database.Store.TrackEvent(event.ProjectID, event.Environment, event.EventName, props)
	}
}

func finalizePersonProfile(profile *PersonProfile, project database.Project, environment string, clientIP string, userAgent string) {
	profile.TenantID = fmt.Sprint(project.ID)
	profile.ProjectID = fmt.Sprint(project.ID)
	profile.OrganizationID = fmt.Sprint(project.OrganizationID)
	profile.Environment = environment

	if profile.Timestamp.IsZero() {
		profile.Timestamp = time.Now()
	}

	if profile.Properties == nil {
		profile.Properties = make(map[string]any)
	}

	enrichWithGeoIP(clientIP, profile.Properties)

	if client := uaParser.Parse(userAgent); client != nil {
		if val, _ := profile.Properties["$os"].(string); val == "" || val == "Other" {
			profile.Properties["$os"] = client.Os.Family
		}
		if val, _ := profile.Properties["$browser"].(string); val == "" || val == "Other" {
			profile.Properties["$browser"] = client.UserAgent.Family
		}
		if val, _ := profile.Properties["$device_model"].(string); val == "" || val == "Other" {
			profile.Properties["$device_model"] = client.Device.Family
		}
	}
}

func finalizePersonAlias(alias *PersonAlias, project database.Project, environment string) {
	alias.TenantID = fmt.Sprint(project.ID)
	alias.ProjectID = fmt.Sprint(project.ID)
	alias.OrganizationID = fmt.Sprint(project.OrganizationID)
	alias.Environment = environment
	
	if alias.Timestamp.IsZero() {
		alias.Timestamp = time.Now()
	}
}

func enrichWithUserAgent(userAgent string, event *AnalyticsEvent) {
	client := uaParser.Parse(userAgent)
	if client == nil {
		return
	}

	if event.OS == "" || event.OS == "Other" {
		event.OS = client.Os.Family
		event.DefaultProperties["$os"] = event.OS
	}
	if event.Browser == "" || event.Browser == "Other" {
		event.Browser = client.UserAgent.Family
		event.DefaultProperties["$browser"] = event.Browser
	}
	if event.DeviceModel == "" || event.DeviceModel == "Other" {
		event.DeviceModel = client.Device.Family
		event.DefaultProperties["$device_model"] = event.DeviceModel
	}
}

func enrichWithGeoIP(clientIP string, properties map[string]any) {
	location := utils.LookupIP(clientIP)
	if location == nil {
		return
	}

	if location.City != "" {
		properties["$city"] = location.City
	}
	if location.Region != "" {
		properties["$region"] = location.Region
	}
	if location.Country != "" {
		properties["$country"] = location.Country
	}
	if location.Timezone != "" {
		properties["$timezone"] = location.Timezone
	}
}

func normalizedClientIP(clientIP string) string {
	if clientIP == "127.0.0.1" || clientIP == "::1" || strings.HasPrefix(clientIP, "192.168.") || strings.HasPrefix(clientIP, "10.") || strings.HasPrefix(clientIP, "172.") {
		return "143.105.209.117"
	}
	return clientIP
}

func isGarbageID(s string) bool {
	if s == "" || len(s) < 2 {
		return true
	}
	lower := strings.ToLower(s)
	return strings.Contains(lower, "gzip") ||
		strings.Contains(lower, "*/*") ||
		strings.Contains(lower, "deflate") ||
		strings.Contains(lower, "identity") ||
		strings.Contains(lower, "accept")
}

func enqueueAnalyticsEvent(eventStream chan<- AnalyticsEvent, event AnalyticsEvent) error {
	select {
	case eventStream <- event:
		return nil
	default:
		return fiber.NewError(fiber.StatusServiceUnavailable, "Buffer full")
	}
}

func enqueuePersonProfile(personStream chan<- PersonProfile, profile PersonProfile) error {
	select {
	case personStream <- profile:
		return nil
	default:
		return fiber.NewError(fiber.StatusServiceUnavailable, "Buffer full")
	}
}

func enqueuePersonAlias(aliasStream chan<- PersonAlias, alias PersonAlias) error {
	select {
	case aliasStream <- alias:
		return nil
	default:
		return fiber.NewError(fiber.StatusServiceUnavailable, "Buffer full")
	}
}

// 🕒 Robust parser for client timestamps (ISO8601, RFC3339 with/without fractional seconds)
func parseTimestamp(ts string) time.Time {
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999Z07:00",
		"2006-01-02 15:04:05.999",
		"2006-01-02 15:04:05",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, ts); err == nil {
			return t
		}
	}

	// Fallback for relative timestamps or bad formats 
	return time.Now()
}
