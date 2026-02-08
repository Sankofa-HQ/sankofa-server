package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

// CONFIG
const (
	API_URL = "http://localhost:8080/v1/track"
	API_KEY = "sk_live_12345"
	TOTAL   = 1000
)

var eventNames = []string{"app_open", "login", "view_product", "add_to_cart", "checkout_start", "purchase", "error_boundary"}
var users = []string{"u_1", "u_2", "u_3", "u_4", "u_5", "u_100", "u_999"}

func main() {
	fmt.Printf("🌊 Starting Data Flood: Sending %d events to %s...\n", TOTAL, API_URL)

	for i := 0; i < TOTAL; i++ {
		// 1. Randomize Data
		evt := map[string]interface{}{
			"event_name": eventNames[rand.Intn(len(eventNames))],
			"user_id":    users[rand.Intn(len(users))],
			"properties": map[string]string{
				"device": "mobile",
				"os":     "android",
			},
		}

		// 2. Encode
		payload, _ := json.Marshal(evt)

		// 3. Fire Request
		req, _ := http.NewRequest("POST", API_URL, bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-api-key", API_KEY)

		client := &http.Client{}
		resp, err := client.Do(req)

		if err != nil {
			fmt.Printf("❌ Failed: %v\n", err)
			continue
		}
		resp.Body.Close()

		// Speed limit: 50 requests per second (so we can watch the graph climb)
		if i%20 == 0 {
			fmt.Print(".")
			time.Sleep(20 * time.Millisecond)
		}
	}
	fmt.Println("\n✅ Flood Complete!")
}
