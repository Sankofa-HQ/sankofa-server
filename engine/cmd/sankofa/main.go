package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sankofa/engine/internal/api"
	"sankofa/engine/internal/database"
	"sankofa/engine/internal/middleware"
	"sankofa/engine/internal/utils"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/glebarez/sqlite"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/joho/godotenv"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

// --- CONFIG ---
var (
	BUFFER_SIZE          = 10000
	FLUSH_INTERVAL       = 2 * time.Second
	BATCH_SIZE           = 1000
	SQLITE_FILE          string
	CLICKHOUSE_ADDR      string
	CLICKHOUSE_USER      string
	CLICKHOUSE_PASS      string
	APP_PORT             string
	CORS_ALLOWED_ORIGINS string
	API_SECRET           string
	ADMIN_EMAIL          string
	ADMIN_PASSWORD       string
	ADMIN_ORG_NAME       string
	ADMIN_PROJECT_NAME   string
	GEOIP_DB_PATH        string
)

func loadConfig() {
	if err := godotenv.Load(); err != nil {
		if err := godotenv.Load("../../.env"); err != nil {
			log.Println("⚠️ No .env file found in current or ../../ directory, finding env vars in system")
		} else {
			log.Println("✅ Loaded .env from ../../.env")
		}
	} else {
		log.Println("✅ Loaded .env from current directory")
	}

	SQLITE_FILE = getEnv("SQLITE_FILE", "sankofa.db")
	CLICKHOUSE_ADDR = getEnv("CLICKHOUSE_ADDR", "127.0.0.1:9000")
	CLICKHOUSE_USER = getEnv("CLICKHOUSE_USER", "default")
	CLICKHOUSE_PASS = getEnv("CLICKHOUSE_PASSWORD", "")
	APP_PORT = getEnv("APP_PORT", "8080")
	CORS_ALLOWED_ORIGINS = getEnv("CORS_ALLOWED_ORIGINS", "http://localhost:3000")
	API_SECRET = getEnv("API_SECRET", "super-secret-key-change-me")
	ADMIN_EMAIL = getEnv("ADMIN_EMAIL", "admin@sankofa.dev")
	ADMIN_PASSWORD = getEnv("ADMIN_PASSWORD", "password")
	ADMIN_ORG_NAME = getEnv("ADMIN_ORG_NAME", "Sankofa Admin Org")
	ADMIN_PROJECT_NAME = getEnv("ADMIN_PROJECT_NAME", "Sankofa Internal")
	GEOIP_DB_PATH = getEnv("GEOIP_DB_PATH", "")
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// --- ANALYTICS MODELS (Still Internal for now) ---
type AnalyticsEvent struct {
	ID                string            `json:"id"`
	EventName         string            `json:"event_name"`
	DistinctID        string            `json:"distinct_id"`
	SessionID         string            `json:"-"`
	City              string            `json:"-"`
	Region            string            `json:"-"`
	Country           string            `json:"-"`
	OS                string            `json:"-"`
	DeviceModel       string            `json:"-"`
	Properties        map[string]string `json:"properties"`
	DefaultProperties map[string]string `json:"default_properties"`
	LibVersion        string            `json:"lib_version"`
	TenantID          string            `json:"-"`
	ProjectID         string            `json:"-"` // Explicit Project ID
	OrganizationID    string            `json:"-"` // Explicit Org ID
	Environment       string            `json:"-"` // 'live' or 'test'
	Timestamp         time.Time         `json:"-"`
}

type PersonProfile struct {
	DistinctID     string            `json:"distinct_id"`
	Properties     map[string]string `json:"properties"`
	TenantID       string            `json:"-"`
	ProjectID      string            `json:"-"`
	OrganizationID string            `json:"-"`
	Environment    string            `json:"-"`
	Timestamp      time.Time         `json:"-"`
}

type PersonAlias struct {
	AliasID        string    `json:"alias_id"`
	DistinctID     string    `json:"distinct_id"`
	TenantID       string    `json:"-"`
	ProjectID      string    `json:"-"`
	OrganizationID string    `json:"-"`
	Environment    string    `json:"-"`
	Timestamp      time.Time `json:"-"`
}

func main() {
	// 0. LOAD CONFIG
	loadConfig()

	// 1. SETUP LOGGING & SIGNALS
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// INIT GEOIP
	utils.InitGeoIP(GEOIP_DB_PATH)
	defer utils.CloseGeoIP()

	// 2. INIT SQLITE (The Brain)
	db, err := gorm.Open(sqlite.Open(SQLITE_FILE), &gorm.Config{})
	if err != nil {
		log.Fatal("❌ SQLite connect failed:", err)
	}

	// 4. INIT AUTH SECRET
	api.SetJWTSecret(API_SECRET)

	// MIGRATIONS
	if err := db.AutoMigrate(
		&database.User{},
		&database.Organization{},
		&database.Project{},
		&database.OrganizationMember{},
		&database.ProjectMember{},
		&database.Team{},
		&database.TeamMember{},
		&database.TeamProject{},
		&database.Plan{}, // Added Plan
		&database.OrganizationInvite{},
		&database.LexiconEvent{},
		&database.LexiconEventProperty{},
		&database.LexiconProfileProperty{},
		&database.Cohort{},
		&database.SavedFunnel{},
		&database.SavedInsight{},
		&database.SavedRetention{},
	); err != nil {
		log.Fatal("❌ Migration failed:", err)
	}

	// INIT LEXICON GATEKEEPER (Cache + Async Queue)
	database.InitLexiconStore(db)

	seedDefaultPlans(db) // Seed Plans before Admin
	seedDefaultSuperAdmin(db)

	// 3. INIT CLICKHOUSE (The Muscle)
	var chConn driver.Conn

	for i := 0; i < 10; i++ {
		chConn, err = connectClickHouse()
		if err == nil {
			// Ping to check actual connectivity
			if err = chConn.Ping(context.Background()); err == nil {
				log.Println("✅ Connected to ClickHouse")
				break
			}
		}
		log.Printf("⚠️ ClickHouse connect failed (attempt %d/10): %v. Retrying in 2s...", i+1, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatal("❌ ClickHouse unavailable after 10 attempts:", err)
	}

	initClickHouseSchema(chConn)

	// 4. START WORKERS
	eventStream := make(chan AnalyticsEvent, BUFFER_SIZE)
	personStream := make(chan PersonProfile, BUFFER_SIZE)
	aliasStream := make(chan PersonAlias, BUFFER_SIZE)
	doneChan := make(chan bool)

	// Background Sync Workers
	go database.StartStalenessSyncWorker(db, chConn)

	go startEventWorker(ctx, chConn, eventStream, doneChan)
	go startPersonWorker(ctx, chConn, personStream)
	go startAliasWorker(ctx, chConn, aliasStream)

	// 5. START WEB SERVER
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})
	app.Use(cors.New(cors.Config{
		AllowOrigins:     CORS_ALLOWED_ORIGINS,
		AllowCredentials: true,
		AllowHeaders:     "Origin, Content-Type, Accept, Authorization, x-api-key, x-project-id", // Added x-project-id
	}))

	apiRouter := app.Group("/api")
	v1 := apiRouter.Group("/v1")

	// HANDLERS
	authHandler := api.NewAuthHandler(db)
	projectHandler := api.NewProjectHandler(db, chConn)
	orgHandler := api.NewOrganizationHandler(db, chConn)                     // New
	eventsHandler := api.NewEventsHandler(db, chConn)                        // Events
	peopleHandler := api.NewPeopleHandler(db, chConn)                        // People
	lexiconHandler := api.NewLexiconHandler(db, chConn)                      // Lexicon
	funnelsHandler := api.NewFunnelsHandler(db, chConn, eventsHandler)       // Funnels
	insightsHandler := api.NewInsightsHandler(db, chConn, eventsHandler)     // Insights
	retentionsHandler := api.NewRetentionsHandler(db, chConn, eventsHandler) // Retentions
	middleware := middleware.NewAuthMiddleware(db, API_SECRET)

	authHandler.RegisterRoutes(apiRouter)

	// Protected Routes
	// protected := apiRouter.Group("/") // REMOVED: Capture-all group caused issues
	// protected.Use(middleware.RequireAuth)

	projectHandler.RegisterRoutes(apiRouter, middleware.RequireAuth)
	eventsHandler.RegisterRoutes(v1, middleware.RequireAuth)     // Events under /api/v1/events
	lexiconHandler.RegisterRoutes(v1, middleware.RequireAuth)    // Lexicon under /api/v1/lexicon
	funnelsHandler.RegisterRoutes(v1, middleware.RequireAuth)    // Funnels under /api/v1/projects/:id/funnels
	insightsHandler.RegisterRoutes(v1, middleware.RequireAuth)   // Insights under /api/v1/projects/:id/insights
	retentionsHandler.RegisterRoutes(v1, middleware.RequireAuth) // Retentions under /api/v1/projects/:id/retentions
	v1.Get("/people/properties/keys", middleware.RequireAuth, peopleHandler.GetPropertyKeys)
	v1.Get("/people/properties/values", middleware.RequireAuth, peopleHandler.GetPropertyValues)
	v1.Get("/people", middleware.RequireAuth, peopleHandler.ListPeople)
	v1.Get("/people/:id", middleware.RequireAuth, peopleHandler.GetPerson)

	// Cohorts
	cohortsHandler := api.NewCohortsHandler(db, chConn)
	v1.Post("/cohorts", middleware.RequireAuth, cohortsHandler.CreateCohort)
	v1.Get("/cohorts", middleware.RequireAuth, cohortsHandler.ListCohorts)
	v1.Post("/cohorts/preview", middleware.RequireAuth, cohortsHandler.PreviewCohort)
	v1.Get("/cohorts/:id", middleware.RequireAuth, cohortsHandler.GetCohort)
	v1.Put("/cohorts/:id", middleware.RequireAuth, cohortsHandler.UpdateCohort)
	v1.Delete("/cohorts/:id", middleware.RequireAuth, cohortsHandler.DeleteCohort)
	v1.Post("/cohorts/:id/members", middleware.RequireAuth, cohortsHandler.AddMembers)
	v1.Delete("/cohorts/:id/members", middleware.RequireAuth, cohortsHandler.RemoveMembers)

	apiRouter.Post("/orgs", middleware.RequireAuth, orgHandler.CreateOrganization)
	apiRouter.Post("/upload", middleware.RequireAuth, api.UploadHandler) // Upload Endpoint

	// Validating/Serving Static Uploads
	app.Static("/uploads", "./uploads")

	// Verify Invite (Open to unauthenticated users via token)
	v1.Get("/orgs/invite/verify", orgHandler.VerifyInvite)

	// Accept Invite (requires auth, but token determines Org)
	v1.Post("/orgs/invite/accept", middleware.RequireAuth, orgHandler.AcceptInvite)

	// Org Routes (Multiplayer)
	orgs := v1.Group("/orgs/:org_id")
	orgs.Use(middleware.RequireAuth)

	// Create Project & Invite & Remove -> Need Admin/Owner
	orgAdmin := orgs.Group("/", middleware.RequireOrgAccess("Admin"))
	orgAdmin.Put("/", orgHandler.UpdateOrganization)    // Update Org Details
	orgAdmin.Delete("/", orgHandler.DeleteOrganization) // Delete Org
	orgAdmin.Post("/projects", orgHandler.CreateProject)
	orgAdmin.Post("/invite", orgHandler.InviteMember)
	orgAdmin.Delete("/members/:user_id", orgHandler.RemoveMember)
	orgAdmin.Get("/members/:user_id/access", orgHandler.GetUserAccess)
	orgAdmin.Put("/members/:user_id/access", orgHandler.UpdateUserAccess)
	orgAdmin.Post("/teams", orgHandler.CreateTeam)
	orgAdmin.Put("/teams/:team_id", orgHandler.UpdateTeam)
	orgAdmin.Delete("/teams/:team_id", orgHandler.DeleteTeam)
	orgAdmin.Post("/teams/:team_id/members", orgHandler.AddTeamMember)
	orgAdmin.Delete("/teams/:team_id/members/:user_id", orgHandler.RemoveTeamMember)
	orgAdmin.Put("/teams/:team_id/projects", orgHandler.UpdateTeamProjects)

	// Read Members -> Needs Member
	orgMember := orgs.Group("/", middleware.RequireOrgAccess("Member"))
	orgMember.Get("/", orgHandler.GetOrganization) // Get Org Details
	orgMember.Get("/usage", orgHandler.GetUsage)   // Get Org Usage
	orgMember.Get("/members", orgHandler.GetMembers)
	orgMember.Get("/teams", orgHandler.GetTeams)
	orgMember.Get("/teams/:team_id/members", orgHandler.GetTeamMembers)
	orgMember.Get("/teams/:team_id/projects", orgHandler.GetTeamProjects)

	// --- ANALYTICS HANDLERS ---
	analytics := apiRouter.Group("/analytics")
	analytics.Use(middleware.RequireAuth)

	// Mock Endpoints for Dashboard
	analytics.Get("/pulse", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"active_users_30m": 12, // Mock
		})
	})

	analytics.Get("/trends", func(c *fiber.Ctx) error {
		// Mock last 7 days
		data := []fiber.Map{}
		now := time.Now()
		for i := 6; i >= 0; i-- {
			date := now.AddDate(0, 0, -i)
			data = append(data, fiber.Map{
				"day":   date.Format("2006-01-02"),
				"count": 100 + (i * 10), // variable fake data
			})
		}
		return c.JSON(data)
	})

	// Users: List profiles (Legacy - Redirect to v1/people in logic if needed, or just keep as backup for now but frontend will use v1)
	// I'll keep it commented out to avoid conflict/confusion
	/*
		analytics.Get("/users", func(c *fiber.Ctx) error {
			// ... legacy ...
		})
	*/

	// INGESTION (v1)
	v1.Post("/track", func(c *fiber.Ctx) error {
		apiKey := c.Get("x-api-key")
		if apiKey == "" {
			log.Println("⚠️ Missing API Key. Headers:", c.GetReqHeaders())
			return c.SendStatus(401)
		}
		log.Println("📝 Track Request. Key:", apiKey)

		// Calculate Environment & Find Project
		var project database.Project
		var environment string

		// Check Live Key
		if err := db.Where("api_key = ?", apiKey).First(&project).Error; err == nil {
			environment = "live"
		} else {
			// Check Test Key
			if err := db.Where("test_api_key = ?", apiKey).First(&project).Error; err == nil {
				environment = "test"
			} else {
				// Invalid Key
				return c.SendStatus(403)
			}
		}

		var e AnalyticsEvent
		if err := c.BodyParser(&e); err != nil {
			return c.SendStatus(400)
		}

		// Context Resolution
		nanoID, _ := gonanoid.New(21)
		e.ID = "evt_" + nanoID
		e.TenantID = fmt.Sprint(project.ID) // Still used for sharding/partitioning maybe? Or just legacy.
		e.ProjectID = fmt.Sprint(project.ID)
		e.OrganizationID = fmt.Sprint(project.OrganizationID)
		e.Environment = environment
		e.Timestamp = time.Now()

		// --- GEOIP RESOLUTION ---
		clientIP := c.IP()
		loc := utils.LookupIP(clientIP)
		if loc != nil {
			if e.DefaultProperties == nil {
				e.DefaultProperties = make(map[string]string)
			}
			if loc.City != "" {
				e.DefaultProperties["$city"] = loc.City
			}
			if loc.Region != "" {
				e.DefaultProperties["$region"] = loc.Region
			}
			if loc.Country != "" {
				e.DefaultProperties["$country"] = loc.Country
			}
			if loc.Timezone != "" {
				e.DefaultProperties["$timezone"] = loc.Timezone
			}
		}

		// --- PROMOTION LOGIC (Plucking high-value fields) ---
		if sid, ok := e.Properties["$session_id"]; ok {
			e.SessionID = sid
		}
		if dp := e.DefaultProperties; dp != nil {
			if city, ok := dp["$city"]; ok {
				e.City = city
			}
			if region, ok := dp["$region"]; ok {
				e.Region = region
			}
			if country, ok := dp["$country"]; ok {
				e.Country = country
			}
			if os, ok := dp["$os"]; ok {
				e.OS = os
			}
			if model, ok := dp["$device_model"]; ok {
				e.DeviceModel = model
			}
		}

		// --- LEXICON GATEKEEPER START ---
		// Async register event name & properties
		if database.Store != nil {
			props := make(map[string]interface{})
			for k, v := range e.Properties {
				props[k] = v
			}
			database.Store.TrackEvent(e.ProjectID, e.EventName, props)
		}
		// --- LEXICON GATEKEEPER END ---

		select {
		case eventStream <- e:
			return c.SendStatus(200)
		default:
			return c.Status(503).JSON(fiber.Map{"error": "Buffer full"})
		}
	})

	v1.Post("/people", func(c *fiber.Ctx) error {
		apiKey := c.Get("x-api-key")
		if apiKey == "" {
			return c.SendStatus(401)
		}

		// Calculate Environment & Find Project
		var project database.Project
		var environment string

		if err := db.Where("api_key = ?", apiKey).First(&project).Error; err == nil {
			environment = "live"
		} else if err := db.Where("test_api_key = ?", apiKey).First(&project).Error; err == nil {
			environment = "test"
		} else {
			return c.SendStatus(403)
		}

		var p PersonProfile
		if err := c.BodyParser(&p); err != nil {
			return c.SendStatus(400)
		}

		p.TenantID = fmt.Sprint(project.ID)
		p.ProjectID = fmt.Sprint(project.ID)
		p.OrganizationID = fmt.Sprint(project.OrganizationID)
		p.Environment = environment
		p.Timestamp = time.Now()

		// --- GEOIP RESOLUTION ---
		clientIP := c.IP()
		loc := utils.LookupIP(clientIP)
		if loc != nil {
			if p.Properties == nil {
				p.Properties = make(map[string]string)
			}
			if loc.City != "" {
				p.Properties["$city"] = loc.City
			}
			if loc.Region != "" {
				p.Properties["$region"] = loc.Region
			}
			if loc.Country != "" {
				p.Properties["$country"] = loc.Country
			}
			if loc.Timezone != "" {
				p.Properties["$timezone"] = loc.Timezone
			}
		}

		select {
		case personStream <- p:
			return c.SendStatus(200)
		default:
			return c.Status(503).JSON(fiber.Map{"error": "Buffer full"})
		}
	})

	v1.Post("/alias", func(c *fiber.Ctx) error {
		apiKey := c.Get("x-api-key")
		if apiKey == "" {
			return c.SendStatus(401)
		}

		// Calculate Environment & Find Project
		var project database.Project
		var environment string

		if err := db.Where("api_key = ?", apiKey).First(&project).Error; err == nil {
			environment = "live"
		} else if err := db.Where("test_api_key = ?", apiKey).First(&project).Error; err == nil {
			environment = "test"
		} else {
			return c.SendStatus(403)
		}

		var a PersonAlias
		if err := c.BodyParser(&a); err != nil {
			return c.SendStatus(400)
		}

		a.TenantID = fmt.Sprint(project.ID)
		a.ProjectID = fmt.Sprint(project.ID)
		a.OrganizationID = fmt.Sprint(project.OrganizationID)
		a.Environment = environment
		a.Timestamp = time.Now()

		select {
		case aliasStream <- a:
			return c.SendStatus(200)
		default:
			return c.Status(503).JSON(fiber.Map{"error": "Buffer full"})
		}
	})

	// START SERVER
	go func() {
		fmt.Printf("🚀 Sankofa Engine running on :%s\n", APP_PORT)
		if err := app.Listen(":" + APP_PORT); err != nil {
			log.Panic(err)
		}
	}()

	// WAIT FOR SHUTDOWN
	<-sigChan
	fmt.Println("\n🛑 Shutdown signal received. Draining buffer...")
	close(eventStream)
	<-doneChan
	fmt.Println("✅ Buffer drained. Exiting.")
}

// --- WORKERS (Unchanged mostly) ---
func startEventWorker(ctx context.Context, conn driver.Conn, stream <-chan AnalyticsEvent, done chan<- bool) {
	var batch []AnalyticsEvent
	ticker := time.NewTicker(FLUSH_INTERVAL)
	defer ticker.Stop()

	flush := func() {
		if len(batch) > 0 {
			writeEventBatch(conn, batch)
			batch = nil
		}
	}

	for {
		select {
		case e, ok := <-stream:
			if !ok {
				flush()
				done <- true
				return
			}
			batch = append(batch, e)
			if len(batch) >= BATCH_SIZE {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func startPersonWorker(ctx context.Context, conn driver.Conn, stream <-chan PersonProfile) {
	for p := range stream {
		ctx := context.Background()
		err := conn.Exec(ctx, "INSERT INTO persons (tenant_id, project_id, organization_id, environment, distinct_id, properties, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?)",
			p.TenantID, p.ProjectID, p.OrganizationID, p.Environment, p.DistinctID, p.Properties, p.Timestamp)
		if err != nil {
			log.Println("❌ Person Write Error:", err)
		}
	}
}

func startAliasWorker(ctx context.Context, conn driver.Conn, stream <-chan PersonAlias) {
	for a := range stream {
		ctx := context.Background()
		err := conn.Exec(ctx, "INSERT INTO person_aliases (tenant_id, project_id, organization_id, environment, alias_id, distinct_id) VALUES (?, ?, ?, ?, ?, ?)",
			a.TenantID, a.ProjectID, a.OrganizationID, a.Environment, a.AliasID, a.DistinctID)
		if err != nil {
			log.Println("❌ Alias Write Error:", err)
		}
	}
}

func writeEventBatch(conn driver.Conn, events []AnalyticsEvent) {
	ctx := context.Background()
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO events (id, tenant_id, project_id, organization_id, environment, timestamp, event_name, distinct_id, session_id, city, region, country, os, device_model, properties, default_properties, lib_version)")
	if err != nil {
		log.Println("❌ Batch Prep Error:", err)
		return
	}
	for _, e := range events {
		if e.Properties == nil {
			e.Properties = make(map[string]string)
		}
		if e.DefaultProperties == nil {
			e.DefaultProperties = make(map[string]string)
		}
		batch.Append(
			e.ID,
			e.TenantID,
			e.ProjectID,
			e.OrganizationID,
			e.Environment,
			e.Timestamp,
			e.EventName,
			e.DistinctID,
			e.SessionID,
			e.City,
			e.Region,
			e.Country,
			e.OS,
			e.DeviceModel,
			e.Properties,
			e.DefaultProperties,
			e.LibVersion,
		)
	}
	if err := batch.Send(); err != nil {
		log.Println("❌ ClickHouse Write Error:", err)
	} else {
		fmt.Printf("💾 Flushed %d events\n", len(events))
	}
}

func connectClickHouse() (driver.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{CLICKHOUSE_ADDR},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: CLICKHOUSE_USER,
			Password: CLICKHOUSE_PASS,
		},
	})
}

func initClickHouseSchema(conn driver.Conn) {
	ctx := context.Background()
	// Schema init logic (same as before)
	// Shortened for brevity in this replacement, assume same tables: events, persons, person_aliases

	// 1. Events
	err := conn.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS events (
		id String,
		tenant_id String,
		project_id String,
		organization_id String,
		environment String, -- 'live' or 'test'
		timestamp DateTime,
		event_name String,
		distinct_id String,
		session_id String,
		city String,
		region String,
		country String,
		os String,
		device_model String,
		properties Map(String, String),
		default_properties Map(String, String),
		lib_version String
	) ENGINE = MergeTree()
	ORDER BY (organization_id, project_id, environment, event_name, timestamp)
	`)
	if err != nil {
		log.Fatal("ClickHouse Init Error:", err)
	}

	// 2. Persons
	err = conn.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS persons (
		tenant_id String,
		project_id String,
		organization_id String,
		environment String,
		distinct_id String,
		properties Map(String, String),
		last_seen DateTime
	) ENGINE = ReplacingMergeTree(last_seen)
	ORDER BY (organization_id, project_id, environment, distinct_id)
	`)
	if err != nil {
		log.Fatal("ClickHouse Init Error:", err)
	}

	// MIGRATION: Add columns if they don't exist (Idempotent)
	cols := []string{"id", "project_id", "organization_id", "environment"}
	tables := []string{"events", "persons"}

	for _, table := range tables {
		for _, col := range cols {
			_ = conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s String", table, col))
		}
	}

	// MIGRATION: Add promoted columns to events if they don't exist
	promotedCols := []string{"session_id", "city", "region", "country", "os", "device_model"}
	for _, col := range promotedCols {
		_ = conn.Exec(ctx, fmt.Sprintf("ALTER TABLE events ADD COLUMN IF NOT EXISTS %s String", col))
	}

	// 3. Aliases
	err = conn.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS person_aliases (
		tenant_id String,
		project_id String,
		organization_id String,
		environment String,
		alias_id String,
		distinct_id String
	) ENGINE = ReplacingMergeTree()
	ORDER BY (organization_id, project_id, environment, alias_id)
	`)
	if err != nil {
		log.Fatal("ClickHouse Init Error:", err)
	}

	// MIGRATION: Add columns if they don't exist (Idempotent)
	cols = []string{"project_id", "organization_id", "environment"}
	tables = []string{"events", "persons", "person_aliases"}

	for _, table := range tables {
		for _, col := range cols {
			_ = conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s String", table, col))
		}
	}

	// 4. Cohort Static Members (CollapsingMergeTree)
	err = conn.Exec(ctx, `
	CREATE TABLE IF NOT EXISTS cohort_static_members (
		project_id String,
		cohort_id String,
		distinct_id String,
		sign Int8
	) ENGINE = CollapsingMergeTree(sign)
	ORDER BY (project_id, cohort_id, distinct_id)
	`)
	if err != nil {
		log.Fatal("ClickHouse Init Error (Cohorts):", err)
	}
}

func seedDefaultSuperAdmin(db *gorm.DB) {
	var count int64
	db.Model(&database.User{}).Count(&count)
	if count > 0 {
		return
	}

	fmt.Println("🌱 Seeding Super Admin...")
	hash, _ := bcrypt.GenerateFromPassword([]byte(ADMIN_PASSWORD), bcrypt.DefaultCost)

	// Create User
	admin := database.User{
		Email:        ADMIN_EMAIL,
		PasswordHash: string(hash),
		FullName:     "Super Admin",
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	db.Create(&admin)

	// Create Org
	org := database.Organization{
		Name:      ADMIN_ORG_NAME,
		Slug:      "sankofa-admin",
		Plan:      "Enterprise",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	db.Create(&org)

	// Create Project
	// Create Project (Idempotent)
	project := database.Project{
		OrganizationID: org.ID,
		Name:           ADMIN_PROJECT_NAME,
		APIKey:         "sk_live_admin_key",
		TestAPIKey:     "sk_test_admin_key",
		Timezone:       "UTC",
		Region:         "us-east-1",
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	var existingProject database.Project
	if err := db.Where("organization_id = ? AND name = ?", org.ID, ADMIN_PROJECT_NAME).First(&existingProject).Error; err == nil {
		// Project exists, ensure TestAPIKey is set (only if it's currently missing)
		if existingProject.TestAPIKey == "" {
			db.Model(&existingProject).Update("test_api_key", project.TestAPIKey)
			fmt.Println("✅ Updated existing project with Test API Key")
		}
		project = existingProject
	} else {
		// Project doesn't exist, create it
		db.Create(&project)
	}

	// Links
	db.Create(&database.OrganizationMember{OrganizationID: org.ID, UserID: admin.ID, Role: "Owner", CreatedAt: time.Now()})
	db.Create(&database.ProjectMember{ProjectID: project.ID, UserID: admin.ID, Role: "Admin", CreatedAt: time.Now()})

	// Update User Context
	db.Model(&admin).Update("current_project_id", project.ID)

	fmt.Printf("✅ Seed Complete. Login: %s / <your-password>\n", ADMIN_EMAIL)
}

func seedDefaultPlans(db *gorm.DB) {
	plans := []database.Plan{
		{
			Name:         "Free",
			EventLimit:   1000000,
			ProfileLimit: 1000,
			ReplayLimit:  1000,
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		},
		{
			Name:         "Pro",
			EventLimit:   10000000,
			ProfileLimit: 100000,
			ReplayLimit:  10000,
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		},
		{
			Name:         "Enterprise",
			EventLimit:   100000000,
			ProfileLimit: 1000000,
			ReplayLimit:  100000,
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		},
	}

	for _, p := range plans {
		// Upsert
		var px database.Plan
		if err := db.Where("name = ?", p.Name).First(&px).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				db.Create(&p)
			}
		} else {
			db.Model(&px).Updates(p)
		}
	}
	fmt.Println("✅ Plans Seeded")
}
