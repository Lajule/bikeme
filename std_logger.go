package main

import (
	"bytes"
	"log"

	"github.com/hashicorp/go-hclog"
)

// STDLogger wraps a standard logger.
type STDLogger struct {
	l *log.Logger
}

// NewSTDLogger creates a logger.
func NewSTDLogger() (hclog.Logger, error) {
	return hclog.New(&hclog.LoggerOptions{
		DisableTime: true,
		Output: &STDLogger{
			l: log.Default(),
		},
	}), nil
}

// Write prints log.
func (l *STDLogger) Write(b []byte) (int, error) {
	l.l.Println(string(bytes.TrimRight(b, " \n\t")))
	return len(b), nil
}
