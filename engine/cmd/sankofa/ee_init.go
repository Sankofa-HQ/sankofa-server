//go:build enterprise
// +build enterprise

package main

import (
	"log"

	// Blank import to trigger the init() function in the EE package
	// This will register the EE features into the main registry
	_ "sankofa/engine/ee"
)

func init() {
	log.Println("💎 Running Enterprise Edition (EE)")
}
