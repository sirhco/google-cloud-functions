package main

import (
	"fmt"
	"log"
	"os"

	// Blank-import the function package so the init() runs
	"github.com/GoogleCloudPlatform/functions-framework-go/funcframework"
	_ "github.com/sirhco/google-cloud-functions/bigquerybackup"
)

var port = "8080"

func init() {
	if envPort := os.Getenv("PORT"); envPort != "" {
		port = envPort
	}
	if gcpProject := os.Getenv("GCP_PROJECT"); gcpProject == "" {
		log.Fatalf("GCP_PROJECT environment variable must be set.")
	}
	fmt.Printf("BigQuery Backup starting up on PORT %v\n", os.Getenv("PORT"))
	fmt.Printf("GCP_PROJECT: %v\n\n", os.Getenv("GCP_PROJECT"))
	fmt.Printf(`Open a new tab in your terminal and run:

// curl http://localhost:%v`, os.Getenv("PORT"))
}
func main() {
	// Use PORT environment variable, or default to 8080.

	err := funcframework.Start(port)
	if err != nil {
		log.Fatalf("funcframework.Start: %v\n", err)
	}
}
