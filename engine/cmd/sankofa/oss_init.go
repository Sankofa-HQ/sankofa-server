//go:build !enterprise
// +build !enterprise

package main

func InitializeEE(secret string) {
	// No-op in OSS
}
