package main

import (
	"encoding/json"
	"io"
	"log"

	"github.com/hashicorp/raft"
)

// FSM is the Raft FSM.
type FSM struct {
	BikeStore *BikeStore
}

// ApplyResponse is to get Apply future response.
type ApplyResponse struct {
	Bike *Bike
	Err  error
}

// NewFSM creates a FSM.
func NewFSM(bikeStore *BikeStore) (*FSM, error) {
	return &FSM{
		BikeStore: bikeStore,
	}, nil
}

// Apply stores the bike contained in the log.
func (fsm *FSM) Apply(l *raft.Log) interface{} {
	log.Printf("[APPLY] log=%#v", l)

	switch l.Type {
	case raft.LogCommand:
		bike := Bike{}
		if err := json.Unmarshal(l.Data, &bike); err != nil {
			return &ApplyResponse{
				Err: err,
			}
		}

		if err := fsm.BikeStore.StoreBike(&bike); err != nil {
			return &ApplyResponse{
				Err: err,
			}
		}

		return &ApplyResponse{
			Bike: &bike,
		}
	}

	return nil
}

// Snapshot creates a snapshot.
func (fsm *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return NewSnapshot(fsm.BikeStore)
}

// Restore store some bikes from a snapshot.
func (fsm *FSM) Restore(rClose io.ReadCloser) error {
	defer func() {
		if err := rClose.Close(); err != nil {
			log.Fatal(err.Error())
		}
	}()

	log.Printf("[RESTORE] rClose=%#v", rClose)

	restored := 0

	decoder := json.NewDecoder(rClose)
	for decoder.More() {
		bike := Bike{}
		if err := decoder.Decode(&bike); err != nil {
			return err
		}

		if err := fsm.BikeStore.StoreBike(&bike); err != nil {
			return err
		}

		restored++
	}

	log.Printf("[RESTORE] restored=%d", restored)

	return nil
}
