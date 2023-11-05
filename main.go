package main

import (
	"github.com/joho/godotenv"
	"os"
	"log"
	_ "net/http/pprof"
	"net/http"
)


func init() {
	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
}

func main() {
	go func() {
		log.Println("Profiling server running on http://localhost:6060")
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			log.Fatalf("Error starting profiling server: %v", err)
		}
	}()
	rootPath := os.Getenv("ROOT_PATH")
	if rootPath == "" {
		log.Fatal("MONGO_URI not set in environment")
	}
	
	indexerEngine(rootPath)
}
