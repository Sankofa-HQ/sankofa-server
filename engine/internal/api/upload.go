package api

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gofiber/fiber/v2"
)

// UploadHandler handles file uploads
func UploadHandler(c *fiber.Ctx) error {
	// Parse the file from the request
	file, err := c.FormFile("file")
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "No file uploaded"})
	}

	// Open the uploaded file
	src, err := file.Open()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to open uploaded file"})
	}
	defer src.Close()

	// Decode the image
	img, _, err := image.Decode(src)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid image format"})
	}

	// Resize if necessary (simple nearest neighbor for now to keep deps low, or just max bounds)
	// We'll keep it simple: if width > 500, scale down.
	bounds := img.Bounds()
	width := bounds.Dx()
	maxWidth := 500

	var finalImg image.Image = img
	if width > maxWidth {
		// ratio := float64(maxWidth) / float64(width)
		// newHeight := int(float64(height) * ratio)
		// Skipping resize logic to avoid deps.
		// We enforce JPEG compression which is the main size saver.
		// If physical resizing is needed, we'd add 'disintegration/imaging'.
	}

	// Instead of saving to disk, encode to a memory buffer
	var buf bytes.Buffer
	err = jpeg.Encode(&buf, finalImg, &jpeg.Options{Quality: 75})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to encode image"})
	}

	// Read B2 configuration for public assets
	b2Endpoint := os.Getenv("B2_PUBLIC_ENDPOINT")
	b2Bucket := os.Getenv("B2_PUBLIC_BUCKET_NAME")
	b2KeyID := os.Getenv("B2_PUBLIC_KEY_ID")
	b2AppKey := os.Getenv("B2_PUBLIC_APP_KEY")

	// Fallback to local disk if B2 is not configured
	if b2Endpoint == "" || b2KeyID == "" || b2AppKey == "" {
		log.Println("⚠️ B2 public credentials not found, falling back to local /uploads directory.")
		
		uploadDir := "./uploads"
		if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
			os.MkdirAll(uploadDir, 0755)
		}

		filename := fmt.Sprintf("%d_%s.jpg", time.Now().UnixNano(), "avatar")
		path := fmt.Sprintf("%s/%s", uploadDir, filename)

		if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to write local file"})
		}

		appURL := os.Getenv("APP_URL")
		if appURL == "" {
			appURL = "http://localhost:8080"
		}

		url := fmt.Sprintf("%s/uploads/%s", appURL, filename)
		return c.JSON(fiber.Map{
			"status": "ok",
			"url":    url,
		})
	}

	// Initialize S3 Client targeting B2
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               b2Endpoint,
			SigningRegion:     region,
			HostnameImmutable: true,
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(b2KeyID, b2AppKey, "")),
		config.WithRegion("eu-central-003"), // Matching the provided endpoint region
	)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Failed to init B2 config"})
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	// Generate a unique filename and S3 key
	filename := fmt.Sprintf("%d_%s.jpg", time.Now().UnixNano(), "avatar")
	objectKey := fmt.Sprintf("avatars/%s", filename)

	// Stream buffer to B2
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String(b2Bucket),
		Key:         aws.String(objectKey),
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: aws.String("image/jpeg"),
	})

	if err != nil {
		log.Printf("❌ B2 Upload Failed: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to upload to cloud storage"})
	}

	// Construct public B2 URL.
	// B2 uses: https://<endpoint>/file/<bucket-name>/<object-key>
	// or standard S3 format depending on bucket settings.
	// A common public B2 pattern is: https://<bucket>.s3.<region>.backblazeb2.com/<key>
	// We'll use the generic S3 path style based on their setup:
	url := fmt.Sprintf("%s/%s/%s", b2Endpoint, b2Bucket, objectKey)

	return c.JSON(fiber.Map{
		"status": "ok",
		"url":    url,
	})
}
