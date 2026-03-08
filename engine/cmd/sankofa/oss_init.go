//go:build !enterprise
// +build !enterprise

package main

import "log"

// init is called automatically before main().
// In the Open Source build (!enterprise), this does nothing.
func init() {
	log.Println("❤️  Running Open Source Edition (OSP)")
}
