package main

import (
	"encoding/json"
	"errors"
	"log"

	"github.com/hashicorp/raft"
)

// Limit is the batch size to select bikes from database.
const Limit = 500

// Snapshot is Raft snapshot.
type Snapshot struct {
	BikeStore *BikeStore
}

// SnapshotData gives access to the snapshot data.
type SnapshotData struct {
	Bike *Bike
	Err  error
}

// NewSnapshot creates a snapshot.
func NewSnapshot(bikeStore *BikeStore) (*Snapshot, error) {
	return &Snapshot{
		BikeStore: bikeStore,
	}, nil
}

// Persist persists a snapshot.
func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	defer func() {
		if err := sink.Close(); err != nil {
			log.Fatal(err.Error())
		}
	}()

	log.Printf("[PERSIST] sink=%#v", sink)

	persisted := 0

	ch := make(chan *SnapshotData, 500)
	errSnapshotFinished := errors.New("snapshot finished")

	go func() {
		offset := uint64(0)

		for {
			bikes := []*Bike{}
			if err := s.BikeStore.GetBikes(Limit, offset, &bikes); err != nil {
				ch <- &SnapshotData{
					Err: err,
				}

				break
			}

			for _, bike := range bikes {
				ch <- &SnapshotData{
					Bike: bike,
				}
			}

			if len(bikes) < 500 {
				ch <- &SnapshotData{
					Err: errSnapshotFinished,
				}

				break
			}

			offset += 500
		}
	}()

	for {
		snapshotData := <-ch

		if snapshotData.Err == errSnapshotFinished {
			break
		}

		if snapshotData.Err != nil {
			return snapshotData.Err
		}

		data, err := json.Marshal(snapshotData.Bike)
		if err != nil {
			return err
		}

		if _, err := sink.Write(data); err != nil {
			return err
		}

		persisted++
	}

	log.Printf("[PERSIST] persisted=%d", persisted)

	return nil
}

// Release releases a snapshot.
func (s *Snapshot) Release() {
	log.Print("[RELEASE]")
}
