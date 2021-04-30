package main

import (
	"log"
	"net/http"
	"time"
)

// StatusRecorder captures the status code
type StatusRecorder struct {
	http.ResponseWriter
	Status int
}

// WriteHeader is a simple wrapper
func (r *StatusRecorder) WriteHeader(status int) {
	r.Status = status
	r.ResponseWriter.WriteHeader(status)
}

// Logger is a middleware to log requests
func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := &StatusRecorder{
			ResponseWriter: w,
		}

		begin := time.Now()
		next.ServeHTTP(recorder, r)
		end := time.Now()

		log.Printf("%s %s %d %s", r.Method, r.RequestURI, recorder.Status, end.Sub(begin).String())
	})
}

// CORS is a middleware to allow CORS
func CORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

		if r.Method == http.MethodOptions {
			return
		}

		next.ServeHTTP(w, r)
	})
}
