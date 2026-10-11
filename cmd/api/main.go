package main

import (
	"log"
	"net/http"
	"time"

	"github.com/NripeshB/distributed_job_processing_system/internals/httpapi"
)

func main() {

	mux := http.NewServeMux()

	handler := httpapi.NewHandler()
	handler.RegisterRouter(mux)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("server listening on %s", server.Addr)
	log.Fatal(server.ListenAndServe())
}
