//go:build ignore

package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func main() {
	// Generate 32 bytes (64 hex characters) of random data
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		fmt.Printf("Error generating secure random bytes: %v\n", err)
		return
	}

	secret := hex.EncodeToString(bytes)
	fmt.Println("Your new secure API_SECRET:")
	fmt.Println(secret)
}
