package main

import (
	"bytes"
	"log"

	"github.com/hashicorp/go-hclog"
)

type stdLogger struct {
	l *log.Logger
}

func newSTDLogger() (hclog.Logger, error) {
	return hclog.New(&hclog.LoggerOptions{
		DisableTime: true,
		Output: &stdLogger{
			l: log.Default(),
		},
	}), nil
}

func (l *stdLogger) Write(b []byte) (int, error) {
	l.l.Println(string(bytes.TrimRight(b, " \n\t")))
	return len(b), nil
}
