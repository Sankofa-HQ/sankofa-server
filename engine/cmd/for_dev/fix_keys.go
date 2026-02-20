//go:build ignore

package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"

	"sankofa/engine/internal/database"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func generateTestAPIKey() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return "sk_test_" + hex.EncodeToString(bytes)
}

func main() {
	db, err := gorm.Open(sqlite.Open("cmd/sankofa/sankofa.db"), &gorm.Config{}) // Adjusted path for sqlite file since we run from sankofa
	if err != nil {
		log.Fatal("DB Open Error:", err)
	}

	var projects []database.Project
	if err := db.Find(&projects).Error; err != nil {
		log.Fatal("Find Projects Error:", err)
	}

	fmt.Printf("Found %d projects.\n", len(projects))

	out, _ := json.MarshalIndent(projects, "", "  ")
	fmt.Println(string(out))
}
