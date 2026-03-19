package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"sankofa/engine/internal/api"
	"sankofa/engine/internal/database"
	"sankofa/engine/internal/email"
	"sankofa/engine/internal/middleware"
	"sankofa/engine/internal/registry"
	"sankofa/engine/internal/utils"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/glebarez/sqlite"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/joho/godotenv"
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
	ROOT_REDIRECT_URL    string
	FRONTEND_URL         string

	// B2 Configuration
	B2_KEY_ID      string
	B2_APP_KEY     string
	B2_BUCKET_NAME string
	B2_ENDPOINT    string

	// Alerts
	DISCORD_WEBHOOK string
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
	ROOT_REDIRECT_URL = getEnv("ROOT_REDIRECT_URL", "https://sankofa.dev")
	FRONTEND_URL = getEnv("FRONTEND_URL", "http://localhost:3000")

	B2_KEY_ID = getEnv("B2_KEY_ID", "")
	B2_APP_KEY = getEnv("B2_APP_KEY", "")
	B2_BUCKET_NAME = getEnv("B2_BUCKET_NAME", "sankofa-replays")
	B2_ENDPOINT = getEnv("B2_ENDPOINT", "https://s3.us-east-005.backblazeb2.com")

	DISCORD_WEBHOOK = getEnv("DISCORD_WEBHOOK", "")
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
	Browser           string            `json:"-"`
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
		&database.OrganizationInvite{},
		&database.LexiconEvent{},
		&database.LexiconEventProperty{},
		&database.LexiconProfileProperty{},
		&database.Cohort{},
		&database.SavedFunnel{},
		&database.SavedInsight{},
		&database.SavedRetention{},
		&database.SavedFlow{},
		&database.Board{},
		&database.BoardWidget{},
		&database.BoardShare{},
	); err != nil {
		log.Fatal("❌ Migration failed:", err)
	}

	// INIT LEXICON GATEKEEPER (Cache + Async Queue)
	database.InitLexiconStore(db)

	// Seed Plans before Admin
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
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}

			// If it's a 500 (Server Crash) or 400 (Bad JSON from the Beta Tester)
			if code >= 500 && DISCORD_WEBHOOK != "" {
				msg := fmt.Sprintf("🚨 **SANKOFA ENGINE ALERT** 🚨\n**Path**: `%s`\n**Status**: `%d`\n**Error**: `%v`\n**IP**: `%s`",
					c.Path(), code, err.Error(), c.IP())
				fireDiscordAlert(DISCORD_WEBHOOK, msg)
			}

			return c.Status(code).JSON(fiber.Map{
				"ok":    false,
				"error": err.Error(),
			})
		},
	})

	// RECOVERY MIDDLEWARE: Catches panics and sends them to the ErrorHandler
	app.Use(recover.New())
	// Define CORS Middlewares
	dashboardCORS := cors.New(cors.Config{
		AllowOrigins:     CORS_ALLOWED_ORIGINS,
		AllowCredentials: true,
		AllowMethods:     "GET, POST, PUT, DELETE, PATCH, OPTIONS",
		AllowHeaders:     "*",
	})

	ingestCORS := cors.New(cors.Config{
		AllowOrigins: "*",                                      // Permissive for data collection preflights; strictly validated in handlers via Dashboard settings
		AllowMethods: "GET, POST, PUT, DELETE, PATCH, OPTIONS", // Permissive for both ingestion and dashboard/lexicon since they share the same prefix
		AllowHeaders: "Origin, Content-Type, Accept, Authorization, authorization, x-api-key, x-project-id, x-org-id, X-Session-Id, X-Chunk-Index, X-Distinct-Id, X-Replay-Mode",
	})

	// INGESTION RATE LIMITER (Protecting endpoints from flooding)
	ingestLimiter := middleware.IngestLimiter()

	// Redirect root API visits to the configured landing/docs page, or fallback to frontend
	app.Get("/", func(c *fiber.Ctx) error {
		if ROOT_REDIRECT_URL != "" {
			return c.Redirect(ROOT_REDIRECT_URL, fiber.StatusMovedPermanently)
		}
		if FRONTEND_URL != "" {
			return c.Redirect(FRONTEND_URL, fiber.StatusMovedPermanently)
		}
		return c.Status(200).JSON(fiber.Map{"name": "Sankofa API", "status": "running"})
	})

	// Health check endpoint
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.Status(200).JSON(fiber.Map{
			"status": "ok",
		})
	})

	// 1. INGESTION (v1/batch, v1/people, etc.) - DEFINED BEFORE restrictive CORS
	ingestV1 := app.Group("/api/v1")
	ingestV1.Use(ingestCORS) // Permissive CORS for data collection
	ingestV1.Use(ingestLimiter)

	ingestV1.Post("/batch", newBatchIngestHandler(db, eventStream, personStream, aliasStream))
	ingestV1.Post("/track", newTrackIngestHandler(db, eventStream))
	ingestV1.Post("/people", newPeopleIngestHandler(db, personStream))
	ingestV1.Post("/alias", newAliasIngestHandler(db, aliasStream))

	// 2. RESTRICTIVE DASHBOARD API
	apiRouter := app.Group("/api")
	apiRouter.Use(func(c *fiber.Ctx) error {
		// INGESTION EXEMPTION: Skip restrictive CORS for ingestion routes
		path := c.Path()
		if strings.HasPrefix(path, "/api/v1/batch") ||
			strings.HasPrefix(path, "/api/v1/track") ||
			strings.HasPrefix(path, "/api/v1/people") ||
			strings.HasPrefix(path, "/api/v1/alias") ||
			strings.HasPrefix(path, "/api/replay") ||
			strings.HasPrefix(path, "/api/ee/replay") {
			return c.Next()
		}
		return dashboardCORS(c)
	})

	v1 := apiRouter.Group("/v1") // This v1 is for reading data and management APIs

	// HANDLERS
	emailManager := email.NewManager()
	authHandler := api.NewAuthHandler(db, emailManager)
	projectHandler := api.NewProjectHandler(db, chConn)
	orgHandler := api.NewOrganizationHandler(db, chConn, emailManager)       // New
	eventsHandler := api.NewEventsHandler(db, chConn)                        // Events
	peopleHandler := api.NewPeopleHandler(db, chConn)                        // People
	lexiconHandler := api.NewLexiconHandler(db, chConn)                      // Lexicon
	funnelsHandler := api.NewFunnelsHandler(db, chConn, eventsHandler)       // Funnels
	flowsHandler := api.NewFlowsHandler(db, chConn, eventsHandler)           // Flows
	insightsHandler := api.NewInsightsHandler(db, chConn, eventsHandler)     // Insights
	retentionsHandler := api.NewRetentionsHandler(db, chConn, eventsHandler) // Retentions
	boardsHandler := api.NewBoardsHandler(db, chConn)                        // Boards
	widgetsHandler := api.NewWidgetsHandler(db, chConn)                      // Widgets
	middleware := middleware.NewAuthMiddleware(db, API_SECRET)

	// --- RBAC MIDDLEWARE SHORTCUTS ---
	// These are pre-built middleware handlers that can be embedded directly in route registration.
	requireEditor := middleware.RequireProjectAccess("Editor")
	requireAdmin := middleware.RequireProjectAccess("Admin")

	authHandler.RegisterRoutes(apiRouter)

	// Protected Routes
	projectHandler.RegisterRoutes(apiRouter, middleware.RequireAuth, requireEditor, requireAdmin)

	// Data read routes: Viewer+
	eventsHandler.RegisterRoutes(v1, middleware.RequireAuth)  // Events under /api/v1/events
	lexiconHandler.RegisterRoutes(v1, middleware.RequireAuth) // Lexicon under /api/v1/lexicon

	// Analysis routes: Read= Viewer+, Write=Editor+
	funnelsHandler.RegisterRoutes(v1, middleware.RequireAuth, middleware.RequireProjectAccess("Viewer"))
	flowsHandler.RegisterRoutes(v1, middleware.RequireAuth, middleware.RequireProjectAccess("Viewer"))
	insightsHandler.RegisterRoutes(v1, middleware.RequireAuth, middleware.RequireProjectAccess("Viewer"))
	retentionsHandler.RegisterRoutes(v1, middleware.RequireAuth, middleware.RequireProjectAccess("Viewer"))
	boardsHandler.RegisterRoutes(v1, middleware.RequireAuth, middleware.RequireProjectAccess("Viewer"))
	widgetsHandler.RegisterRoutes(v1, middleware.RequireAuth, middleware.RequireProjectAccess("Viewer"))
	v1.Get("/people/properties/keys", middleware.RequireAuth, peopleHandler.GetPropertyKeys)
	v1.Get("/people/properties/values", middleware.RequireAuth, peopleHandler.GetPropertyValues)
	v1.Get("/people", middleware.RequireAuth, peopleHandler.ListPeople)
	v1.Get("/people/:id", middleware.RequireAuth, peopleHandler.GetPerson)
	v1.Get("/people/:id/heatmap", middleware.RequireAuth, peopleHandler.GetPersonHeatmap)

	// --- ENTERPRISE HOOKS REGISTRATION ---
	// This will be a no-op in OSS builds, but will register all EE features
	// (SAML, Advanced RBAC, Audit Logs, etc.) when compiled with `-tags enterprise`.
	registry.InitializeAll(app, db, chConn, v1)

	// Cohorts — Read: Viewer+, Write: Editor+
	cohortsHandler := api.NewCohortsHandler(db, chConn)
	v1.Get("/cohorts", middleware.RequireAuth, cohortsHandler.ListCohorts)
	v1.Get("/cohorts/:id", middleware.RequireAuth, cohortsHandler.GetCohort)
	v1.Post("/cohorts", middleware.RequireAuth, requireEditor, cohortsHandler.CreateCohort)
	v1.Post("/cohorts/preview", middleware.RequireAuth, requireEditor, cohortsHandler.PreviewCohort)
	v1.Put("/cohorts/:id", middleware.RequireAuth, requireEditor, cohortsHandler.UpdateCohort)
	v1.Delete("/cohorts/:id", middleware.RequireAuth, requireEditor, cohortsHandler.DeleteCohort)
	v1.Post("/cohorts/:id/members", middleware.RequireAuth, requireEditor, cohortsHandler.AddMembers)
	v1.Delete("/cohorts/:id/members", middleware.RequireAuth, requireEditor, cohortsHandler.RemoveMembers)

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
	orgAdmin.Post("/invite/cancel", orgHandler.CancelInvite)
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

	// (Ingestion routes have been moved to top-level app for CORS decoupling)

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
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO events (id, tenant_id, project_id, organization_id, environment, timestamp, event_name, distinct_id, session_id, city, region, country, os, browser, device_model, properties, default_properties, lib_version)")
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
			e.Browser,
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
		browser String,
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
	sharedCols := []string{"id", "project_id", "organization_id", "environment"}
	for _, table := range []string{"events", "persons"} {
		for _, col := range sharedCols {
			_ = conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s String", table, col))
		}
	}
	// Event-specific columns
	eventCols := []string{"os", "browser", "device_model"}
	for _, col := range eventCols {
		_ = conn.Exec(ctx, fmt.Sprintf("ALTER TABLE events ADD COLUMN IF NOT EXISTS %s String", col))
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
	cols := []string{"project_id", "organization_id", "environment"}
	tables := []string{"events", "persons", "person_aliases"}

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
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	db.Create(&org)

	// Create Project
	// Create Project (Idempotent)
	project := database.Project{
		OrganizationID: org.ID,
		Name:           ADMIN_PROJECT_NAME,
		CreatedByID:    admin.ID,
		APIKey:         "sk_live_admin_key",
		TestAPIKey:     "sk_test_admin_key",
		Timezone:       "UTC",
		Region:         "eu-central-1",
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

// --- HELPERS ---

func fireDiscordAlert(webhookURL, message string) {
	if webhookURL == "" {
		return
	}
	payload, _ := json.Marshal(map[string]string{"content": message})
	// Run in background to avoid blocking request
	go func() {
		resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			log.Printf("⚠️ Discord alert failed: %v", err)
			return
		}
		defer resp.Body.Close()
	}()
}
