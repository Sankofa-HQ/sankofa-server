package registry

import (
	"log"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofiber/fiber/v2"
	"gorm.io/gorm"
)

// EnterpriseFeature defines the interface that all EE plugins must implement
// to hook into the main Open Source application seamlessly.
type EnterpriseFeature interface {
	// Name returns the unique identifier for this enterprise plugin
	Name() string
	// Register attaches routes, middlewares, or logic to the main application
	Register(app *fiber.App, db *gorm.DB, chConn driver.Conn, v1 fiber.Router) error
}

var features []EnterpriseFeature

// RegisterFeature is called by enterprise init hooks (via go:build tags)
// to attach proprietary features to the running binary.
func RegisterFeature(feature EnterpriseFeature) {
	features = append(features, feature)
	log.Printf("🔌 Registered Enterprise Feature: %s", feature.Name())
}

// InitializeAll is called by the Open Source main.go to mount any registered EE features.
// In the purely OSS build, the features slice will be empty.
func InitializeAll(app *fiber.App, db *gorm.DB, chConn driver.Conn, v1 fiber.Router) {
	for _, feature := range features {
		log.Printf("🚀 Initializing Enterprise Feature: %s", feature.Name())
		if err := feature.Register(app, db, chConn, v1); err != nil {
			log.Printf("❌ Failed to initialize Enterprise Feature '%s': %v", feature.Name(), err)
		}
	}
}
