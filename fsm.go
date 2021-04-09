package main

import (
	"encoding/json"
	"io"
	"log"

	"github.com/hashicorp/raft"
)

type fsm struct {
	store *bikeStore
}

type fsmData struct {
	Bike *bike
	Err  error
}

func newFSM(bs *bikeStore) (*fsm, error) {
	return &fsm{
		store: bs,
	}, nil
}

// Apply Commit something on cluster
func (f *fsm) Apply(l *raft.Log) interface{} {
	log.Printf("[APPLY] log=%#v", l)

	switch l.Type {
	case raft.LogCommand:
		d := fsmData{}

		if err := json.Unmarshal(l.Data, &d); err != nil {
			return &fsmData{
				Err: err,
			}
		}

		if err := f.store.StoreBike(d.Bike); err != nil {
			return &fsmData{
				Err: err,
			}
		}

		return &d
	}

	return nil
}

// Snapshot Get a snapshot
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return newSnapshot(f.store)
}

// Restore Restore a snapshot
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

		if err := f.store.StoreBike(&b); err != nil {
			return err
		}

		restored++
	}

	log.Printf("[RESTORE] restored=%d", restored)

	return nil
}
