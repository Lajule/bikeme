package main

import (
	"encoding/json"
	"errors"
	"log"

	"github.com/hashicorp/raft"
)

type snapshot struct {
	s *bikeStore
}

type snapshotData struct {
	b *bike
	err  error
}

func newSnapshot(s *bikeStore) (*snapshot, error) {
	return &snapshot{
		s: s,
	}, nil
}

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

			if err := s.s.GetBikes(500, offset, bikes); err != nil {
				ch <- &snapshotData{
					err: err,
				}

				break
			}

			for _, b := range bikes {
				ch <- &snapshotData{
					b: b,
				}
			}

			if len(bikes) < 500 {
				ch <- &snapshotData{
					err: errSnapshotFinished,
				}

				break
			}

			offset += 500
		}
	}()

	for {
		d := <-ch

		if d.err == errSnapshotFinished {
			break
		}

		if d.err != nil {
			return d.err
		}

		data, err := json.Marshal(d.b)
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

func (s *snapshot) Release() {
	log.Print("[RELEASE]")
}
