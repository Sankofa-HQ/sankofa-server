package utils

import (
	"log"
	"net"

	"github.com/oschwald/geoip2-golang"
)

var db *geoip2.Reader

// InitGeoIP loads the MaxMind database into memory on server boot.
func InitGeoIP(dbPath string) {
	var err error
	if dbPath == "" {
		log.Println("⚠️ GeoIP Database path is empty, GeoIP resolution will be disabled")
		return
	}
	
	db, err = geoip2.Open(dbPath)
	if err != nil {
		log.Printf("⚠️ Failed to load MaxMind database at %s: %v. GeoIP resolution will be disabled.", dbPath, err)
		return
	}
	log.Println("🌍 GeoIP Database loaded successfully")
}

// CloseGeoIP should be called on server shutdown
func CloseGeoIP() {
	if db != nil {
		db.Close()
	}
}

// LocationData represents the extracted geo fields
type LocationData struct {
	City    string
	Region  string
	Country string
	Timezone string
}

// LookupIP takes a raw IP string and returns the enriched location data
func LookupIP(ipString string) *LocationData {
	if db == nil || ipString == "" {
		return nil
	}

	ip := net.ParseIP(ipString)
	if ip == nil {
		return nil
	}

	record, err := db.City(ip)
	if err != nil {
		return nil // IP not found or local IP
	}

	loc := &LocationData{}

	// Extract Country (e.g., "Ghana", "United States")
	if record.Country.Names != nil {
		loc.Country = record.Country.Names["en"]
	}

	// Extract Region / State (e.g., "Greater Accra Region", "Texas")
	if len(record.Subdivisions) > 0 && record.Subdivisions[0].Names != nil {
		loc.Region = record.Subdivisions[0].Names["en"]
	}

	// Extract City (e.g., "Accra", "Duncanville")
	if record.City.Names != nil {
		loc.City = record.City.Names["en"]
	}

	// Extract Timezone (e.g., "Africa/Accra")
	if record.Location.TimeZone != "" {
		loc.Timezone = record.Location.TimeZone
	}

	return loc
}
