package main

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func main() {
	secret := []byte("super-secret-key-change-me")
	claims := jwt.MapClaims{
		"user_id": float64(1),
		"email":   "user@example.com",
		"exp":     time.Now().Add(time.Hour).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(secret)
	if err != nil {
		panic(err)
	}
	fmt.Print(signed)
}
