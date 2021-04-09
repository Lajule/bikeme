package main

import (
	"encoding/json"
	"errors"
	"log"

	"github.com/hashicorp/raft"
)

type snapshot struct {
	store *bikeStore
}

type snapshotData struct {
	Bike *bike
	Err  error
}

func newSnapshot(bs *bikeStore) (*snapshot, error) {
	return &snapshot{
		store: bs,
	}, nil
}

// Persist Create a snapshot
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	defer func() {
		if err := sink.Close(); err != nil {
			log.Fatal(err.Error())
		}
	}()

	log.Printf("[PERSIST] sink=%#v", sink)

	persisted := 0

	ch := make(chan *snapshotData, 500)
	errSnapshotFinished := errors.New("snapshot finished")

	go func() {
		offset := uint64(0)

		for {
			bikes := []*bike{}

			if err := s.store.GetBikes(500, offset, bikes); err != nil {
				ch <- &snapshotData{
					Err: err,
				}

				break
			}

			for _, b := range bikes {
				ch <- &snapshotData{
					Bike: b,
				}
			}

			if len(bikes) < 500 {
				ch <- &snapshotData{
					Err: errSnapshotFinished,
				}

				break
			}

			offset += 500
		}
	}()

	for {
		d := <-ch

		if d.Err == errSnapshotFinished {
			break
		}

		if d.Err != nil {
			return d.Err
		}

		data, err := json.Marshal(d.Bike)
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

// Release Release a snapshot
func (s *snapshot) Release() {
	log.Print("[RELEASE]")
}
