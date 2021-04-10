package main

import (
	"encoding/json"
	"io"
	"log"

	"github.com/hashicorp/raft"
)

type fsm struct {
	s *bikeStore
}

type fsmData struct {
	b *bike
	err  error
}

func newFSM(s *bikeStore) (*fsm, error) {
	return &fsm{
		s: s,
	}, nil
}

func (f *fsm) Apply(l *raft.Log) interface{} {
	log.Printf("[APPLY] log=%#v", l)

	switch l.Type {
	case raft.LogCommand:
		d := fsmData{}

		if err := json.Unmarshal(l.Data, &d); err != nil {
			return &fsmData{
				err: err,
			}
		}

		if err := f.s.StoreBike(d.b); err != nil {
			return &fsmData{
				err: err,
			}
		}

		return &d
	}

	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return newSnapshot(f.s)
}

func (f *fsm) Restore(rClose io.ReadCloser) error {
	defer func() {
		if err := rClose.Close(); err != nil {
			log.Fatal(err.Error())
		}
	}()

	log.Printf("[RESTORE] rClose=%#v", rClose)

	restored := 0

	decoder := json.NewDecoder(rClose)
	for decoder.More() {
		b := bike{}

		if err := decoder.Decode(&b); err != nil {
			return err
		}

		if err := f.s.StoreBike(&b); err != nil {
			return err
		}

		restored++
	}

	log.Printf("[RESTORE] restored=%d", restored)

	return nil
}
