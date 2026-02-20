package main

import (
	"fmt"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load(".env")
	secret := os.Getenv("API_SECRET")
	if secret == "" {
		fmt.Println("No secret found")
		return
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id": 2, // User ID from the frontend logs
		"exp":     time.Now().Add(time.Hour * 72).Unix(),
	})
	t, _ := token.SignedString([]byte(secret))
	fmt.Println(t)
}
