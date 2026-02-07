package main

import (
	"context"
	"embed"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/glebarez/sqlite"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/filesystem"
	"github.com/joho/godotenv"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

//go:embed dist
var frontend embed.FS

// --- CONFIG ---
var (
	BUFFER_SIZE     = 10000
	FLUSH_INTERVAL  = 2 * time.Second
	BATCH_SIZE      = 1000
	SQLITE_FILE     string
	CLICKHOUSE_ADDR string
	CLICKHOUSE_USER string
	CLICKHOUSE_PASS string
	APP_PORT        string
)

func loadConfig() {
	// Try loading from current dir, then parent dirs
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
	APP_PORT = getEnv("APP_PORT", "8080") // Default to 8080
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// --- MODELS ---
type User struct {
	ID        uint   `gorm:"primaryKey"`
	Email     string `gorm:"uniqueIndex"`
	Password  string // Hashed
	APIKey    string `gorm:"uniqueIndex"`
	CreatedAt time.Time
}

type AnalyticsEvent struct {
	EventName         string            `json:"event_name"`
	UserID            string            `json:"user_id"`
	Properties        map[string]string `json:"properties"`
	DefaultProperties map[string]string `json:"default_properties"`
	// Internal
	TenantID  string    `json:"-"`
	Timestamp time.Time `json:"-"`
}

func main() {
	// 0. LOAD CONFIG
	loadConfig()

	// 1. SETUP LOGGING & SIGNALS (Graceful Shutdown)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// 2. INIT SQLITE (The Brain)
	db, err := gorm.Open(sqlite.Open(SQLITE_FILE), &gorm.Config{})
	if err != nil {
		log.Fatal("❌ SQLite connect failed:", err)
	}
	db.AutoMigrate(&User{})
	seedDefaultUser(db)

	// 3. INIT CLICKHOUSE (The Muscle)
	chConn, err := connectClickHouse()
	if err != nil {
		log.Fatal("❌ ClickHouse connect failed:", err)
	}
	// *** AUTO MIGRATION ***
	initClickHouseSchema(chConn)

	// 4. START INGESTION WORKER
	eventStream := make(chan AnalyticsEvent, BUFFER_SIZE)
	doneChan := make(chan bool)

	go startBackgroundWorker(ctx, chConn, eventStream, doneChan)

	// 5. START WEB SERVER
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})
	app.Use(cors.New(cors.Config{
		AllowOrigins:     "http://localhost:3000",
		AllowCredentials: true,
	}))

	// API LAYOUT
	api := app.Group("/api")
	v1 := app.Group("/v1")

	// --- AUTH HANDLERS ---
	api.Post("/auth/login", func(c *fiber.Ctx) error {
		type LoginReq struct {
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		var req LoginReq
		if err := c.BodyParser(&req); err != nil {
			log.Println("❌ Auth: BodyParser failed:", err)
			return c.SendStatus(400)
		}

		log.Printf("🔍 Auth Attempt: Email='%s', PasswordLen=%d", req.Email, len(req.Password))

		var user User
		if err := db.First(&user, "email = ?", req.Email).Error; err != nil {
			log.Println("❌ Auth: User not found")
			return c.Status(401).JSON(fiber.Map{"error": "Invalid credentials"})
		}

		if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.Password)); err != nil {
			log.Println("❌ Auth: Password mismatch:", err)
			return c.Status(401).JSON(fiber.Map{"error": "Invalid credentials"})
		}
		log.Println("✅ Auth: Success for user", user.ID)

		// Set Cookie
		c.Cookie(&fiber.Cookie{
			Name:     "auth_token",
			Value:    fmt.Sprintf("%d", user.ID), // Simple ID for now
			HTTPOnly: true,
			Expires:  time.Now().Add(24 * time.Hour),
		})

		return c.JSON(fiber.Map{"status": "ok", "api_key": user.APIKey})
	})

	api.Post("/auth/logout", func(c *fiber.Ctx) error {
		c.ClearCookie("auth_token")
		return c.SendStatus(200)
	})

	api.Get("/auth/me", func(c *fiber.Ctx) error {
		userID := c.Cookies("auth_token")
		if userID == "" {
			return c.SendStatus(401)
		}
		var user User
		if err := db.First(&user, "id = ?", userID).Error; err != nil {
			return c.SendStatus(401)
		}
		return c.JSON(fiber.Map{
			"email":   user.Email,
			"api_key": user.APIKey,
		})
	})

	// --- ANALYTICS HANDLERS ---
	analytics := api.Group("/analytics")
	analytics.Use(func(c *fiber.Ctx) error {
		userID := c.Cookies("auth_token")
		if userID == "" {
			return c.SendStatus(401)
		}
		// TODO: Verify user exists
		return c.Next()
	})

	// Pulse: Real-time stats (last 30 mins)
	analytics.Get("/pulse", func(c *fiber.Ctx) error {
		var count uint64
		err := chConn.QueryRow(context.Background(), "SELECT count(*) FROM events WHERE timestamp > now() - INTERVAL 30 MINUTE").Scan(&count)
		if err != nil {
			return c.Status(500).SendString(err.Error())
		}
		return c.JSON(fiber.Map{"active_users_30m": count})
	})

	// Trends: Daily stats
	analytics.Get("/trends", func(c *fiber.Ctx) error {
		rows, err := chConn.Query(context.Background(), "SELECT toStartOfDay(timestamp) as day, count(*) FROM events GROUP BY day ORDER BY day")
		if err != nil {
			return c.Status(500).SendString(err.Error())
		}
		defer rows.Close()

		type Point struct {
			Day   time.Time `json:"day"`
			Count uint64    `json:"count"`
		}
		var points []Point
		for rows.Next() {
			var p Point
			if err := rows.Scan(&p.Day, &p.Count); err != nil {
				continue
			}
			points = append(points, p)
		}
		return c.JSON(points)
	})

	// INGESTION (v1)
	v1.Post("/track", func(c *fiber.Ctx) error {
		apiKey := c.Get("x-api-key")
		if apiKey == "" {
			return c.SendStatus(401)
		}

		// Cache this lookup in prod!
		var user User
		if err := db.First(&user, "api_key = ?", apiKey).Error; err != nil {
			return c.SendStatus(403)
		}

		var e AnalyticsEvent
		if err := c.BodyParser(&e); err != nil {
			return c.SendStatus(400)
		}

		e.TenantID = fmt.Sprint(user.ID)
		e.Timestamp = time.Now()

		select {
		case eventStream <- e:
			return c.SendStatus(200)
		default:
			return c.Status(503).JSON(fiber.Map{"error": "Buffer full"})
		}
	})

	// SPA CATCH-ALL
	app.Use("/", filesystem.New(filesystem.Config{
		Root:         http.FS(frontend),
		PathPrefix:   "dist",
		Browse:       false,
		Index:        "index.html",
		NotFoundFile: "dist/index.html",
	}))

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

// --- HELPERS ---

func startBackgroundWorker(ctx context.Context, conn driver.Conn, stream <-chan AnalyticsEvent, done chan<- bool) {
	var batch []AnalyticsEvent
	ticker := time.NewTicker(FLUSH_INTERVAL)
	defer ticker.Stop()

	flush := func() {
		if len(batch) > 0 {
			writeBatch(conn, batch)
			batch = nil
		}
	}

	for {
		select {
		case e, ok := <-stream:
			if !ok {
				// Channel closed (Shutdown triggered)
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

func writeBatch(conn driver.Conn, events []AnalyticsEvent) {
	ctx := context.Background()
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO events (tenant_id, timestamp, event_name, user_id, properties, default_properties)")
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
		batch.Append(e.TenantID, e.Timestamp, e.EventName, e.UserID, e.Properties, e.DefaultProperties)
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
	// Create Table
	query := `
	CREATE TABLE IF NOT EXISTS events (
		tenant_id String,
		timestamp DateTime,
		event_name String,
		user_id String,
		properties Map(String, String),
		default_properties Map(String, String)
	) ENGINE = MergeTree()
	ORDER BY (tenant_id, event_name, timestamp)
	`
	if err := conn.Exec(ctx, query); err != nil {
		log.Fatal("❌ Schema Init Failed:", err)
	}
	fmt.Println("✅ ClickHouse Schema Verified")
}

func seedDefaultUser(db *gorm.DB) {
	var user User
	// Check if admin exists
	err := db.First(&user, "email = ?", "admin@sankofa.dev").Error

	hash, _ := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)

	if err == gorm.ErrRecordNotFound {
		// Create new
		db.Create(&User{
			Email:     "admin@sankofa.dev",
			Password:  string(hash),
			APIKey:    "sk_live_12345",
			CreatedAt: time.Now(),
		})
		fmt.Println("🔑 Default User Created: admin@sankofa.dev / password")
	} else {
		// ALWAYS update password to ensure it matches 'password'
		db.Model(&user).Update("password", string(hash))
		fmt.Println("🔧 Default User Password Reset to 'password'")
	}
}
