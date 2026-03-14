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
	"gorm.io/gorm"
)

type analyticsBatchRequest struct {
	Operations []analyticsBatchOperation `json:"operations"`
}

type analyticsBatchOperation struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func newTrackIngestHandler(db *gorm.DB, eventStream chan<- AnalyticsEvent) fiber.Handler {
	return func(c *fiber.Ctx) error {
		project, environment, err := resolveProjectForAPIKey(db, c.Get("x-api-key"))
		if err != nil {
			return err
		}

		var event AnalyticsEvent
		if err := c.BodyParser(&event); err != nil {
			return c.SendStatus(fiber.StatusBadRequest)
		}

		finalizeAnalyticsEvent(&event, project, environment, normalizedClientIP(c.IP()))
		if err := enqueueAnalyticsEvent(eventStream, event); err != nil {
			return err
		}

		return c.SendStatus(fiber.StatusOK)
	}
}

func newPeopleIngestHandler(db *gorm.DB, personStream chan<- PersonProfile) fiber.Handler {
	return func(c *fiber.Ctx) error {
		project, environment, err := resolveProjectForAPIKey(db, c.Get("x-api-key"))
		if err != nil {
			return err
		}

		var person PersonProfile
		if err := c.BodyParser(&person); err != nil {
			return c.SendStatus(fiber.StatusBadRequest)
		}

		finalizePersonProfile(&person, project, environment, normalizedClientIP(c.IP()))
		if err := enqueuePersonProfile(personStream, person); err != nil {
			return err
		}

		return c.SendStatus(fiber.StatusOK)
	}
}

func newAliasIngestHandler(db *gorm.DB, aliasStream chan<- PersonAlias) fiber.Handler {
	return func(c *fiber.Ctx) error {
		project, environment, err := resolveProjectForAPIKey(db, c.Get("x-api-key"))
		if err != nil {
			return err
		}

		var alias PersonAlias
		if err := c.BodyParser(&alias); err != nil {
			return c.SendStatus(fiber.StatusBadRequest)
		}

		finalizePersonAlias(&alias, project, environment)
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
		project, environment, err := resolveProjectForAPIKey(db, c.Get("x-api-key"))
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
				finalizeAnalyticsEvent(&event, project, environment, clientIP)
				events = append(events, event)
			case "people":
				var person PersonProfile
				if err := json.Unmarshal(operation.Payload, &person); err != nil {
					return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid people payload"})
				}
				finalizePersonProfile(&person, project, environment, clientIP)
				people = append(people, person)
			case "alias":
				var alias PersonAlias
				if err := json.Unmarshal(operation.Payload, &alias); err != nil {
					return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid alias payload"})
				}
				finalizePersonAlias(&alias, project, environment)
				aliases = append(aliases, alias)
			default:
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": fmt.Sprintf("Unsupported operation type: %s", operation.Type)})
			}
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

func resolveProjectForAPIKey(db *gorm.DB, apiKey string) (database.Project, string, error) {
	if apiKey == "" {
		log.Println("⚠️ Missing API Key")
		return database.Project{}, "", fiber.NewError(fiber.StatusUnauthorized, "Missing API Key")
	}

	var project database.Project
	if err := db.Where("api_key = ?", apiKey).First(&project).Error; err == nil {
		return project, "live", nil
	}
	if err := db.Where("test_api_key = ?", apiKey).First(&project).Error; err == nil {
		return project, "test", nil
	}

	return database.Project{}, "", fiber.NewError(fiber.StatusForbidden, "Invalid API Key")
}

func finalizeAnalyticsEvent(event *AnalyticsEvent, project database.Project, environment string, clientIP string) {
	nanoID, _ := gonanoid.New(21)
	event.ID = "evt_" + nanoID
	event.TenantID = fmt.Sprint(project.ID)
	event.ProjectID = fmt.Sprint(project.ID)
	event.OrganizationID = fmt.Sprint(project.OrganizationID)
	event.Environment = environment
	event.Timestamp = time.Now()

	if event.Properties == nil {
		event.Properties = make(map[string]string)
	}
	if event.DefaultProperties == nil {
		event.DefaultProperties = make(map[string]string)
	}

	enrichWithGeoIP(clientIP, event.DefaultProperties)

	if sessionID, ok := event.Properties["$session_id"]; ok {
		event.SessionID = sessionID
	}

	if city, ok := event.DefaultProperties["$city"]; ok {
		event.City = city
	}
	if region, ok := event.DefaultProperties["$region"]; ok {
		event.Region = region
	}
	if country, ok := event.DefaultProperties["$country"]; ok {
		event.Country = country
	}
	if os, ok := event.DefaultProperties["$os"]; ok {
		event.OS = os
	}
	if model, ok := event.DefaultProperties["$device_model"]; ok {
		event.DeviceModel = model
	}

	if database.Store != nil {
		props := make(map[string]interface{}, len(event.Properties))
		for key, value := range event.Properties {
			props[key] = value
		}
		database.Store.TrackEvent(event.ProjectID, event.EventName, props)
	}
}

func finalizePersonProfile(profile *PersonProfile, project database.Project, environment string, clientIP string) {
	profile.TenantID = fmt.Sprint(project.ID)
	profile.ProjectID = fmt.Sprint(project.ID)
	profile.OrganizationID = fmt.Sprint(project.OrganizationID)
	profile.Environment = environment
	profile.Timestamp = time.Now()

	if profile.Properties == nil {
		profile.Properties = make(map[string]string)
	}

	enrichWithGeoIP(clientIP, profile.Properties)
}

func finalizePersonAlias(alias *PersonAlias, project database.Project, environment string) {
	alias.TenantID = fmt.Sprint(project.ID)
	alias.ProjectID = fmt.Sprint(project.ID)
	alias.OrganizationID = fmt.Sprint(project.OrganizationID)
	alias.Environment = environment
	alias.Timestamp = time.Now()
}

func enrichWithGeoIP(clientIP string, properties map[string]string) {
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
