package main

import (
	"log"

	"github.com/TekkenSteve/GoAgent/internal/app"
	"github.com/TekkenSteve/GoAgent/internal/config"
)

func main() {
	// Configuration
	cfg, err := config.NewConfig()
	if err != nil {
		log.Fatalf("Config error: %s", err)
	}

	// Run
	app.Run(cfg)
}
