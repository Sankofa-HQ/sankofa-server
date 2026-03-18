package middleware

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/limiter"
)

// IngestLimiter returns a rate limiter middleware for ingestion endpoints
func IngestLimiter() fiber.Handler {
	return limiter.New(limiter.Config{
		Max:        500,             // 500 requests
		Expiration: 1 * time.Minute, // per 1 minute
		KeyGenerator: func(c *fiber.Ctx) string {
			// Per project limit if API key is present, otherwise per IP
			key := c.Get("x-api-key")
			if key != "" {
				return "limiter:apikey:" + key
			}
			return "limiter:ip:" + c.IP()
		},
		LimitReached: func(c *fiber.Ctx) error {
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"error": "Rate limit exceeded. Please wait a moment.",
			})
		},
	})
}
