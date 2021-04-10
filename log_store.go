package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
	_ "github.com/mattn/go-sqlite3"
)

type logStore struct {
	db *sql.DB
}

func newLogStore(path string) (*logStore, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS log(idx INTEGER, v BLOB, PRIMARY KEY(idx))"); err != nil {
		return nil, err
	}

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS store(k INTEGER, v BLOB, PRIMARY KEY(k))"); err != nil {
		return nil, err
	}

	return &logStore{
		db: db,
	}, nil
}

func (s *logStore) FirstIndex() (uint64, error) {
	idx := uint64(0)

	if err := s.db.QueryRow("SELECT idx FROM log ORDER BY idx ASC LIMIT 1").Scan(&idx); err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	return idx, nil
}

func (s *logStore) LastIndex() (uint64, error) {
	idx := uint64(0)

	if err := s.db.QueryRow("SELECT idx FROM log ORDER BY idx DESC LIMIT 1").Scan(&idx); err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	return idx, nil
}

func (s *logStore) GetLog(idx uint64, log *raft.Log) error {
	v := []byte{}

	if err := s.db.QueryRow("SELECT v FROM log WHERE idx = ?", idx).Scan(&v); err != nil {
		if err == sql.ErrNoRows {
			return raft.ErrLogNotFound
		}

		return err
	}

	return decodeMsgPack(v, log)
}

func (s *logStore) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

func (s *logStore) StoreLogs(logs []*raft.Log) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT INTO log(idx, v) VALUES(?, ?)")
	if err != nil {
		return err
	}

	for _, log := range logs {
		buffer, err := encodeMsgPack(log)
		if err != nil {
			tx.Rollback()
			return err
		}

		if _, err := stmt.Exec(log.Index, buffer.Bytes()); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (s *logStore) DeleteRange(min, max uint64) error {
	if _, err := s.db.Exec("DELETE FROM log WHERE idx BETWEEN ? AND ?", min, max); err != nil {
		return err
	}

	return nil
}

func (s *logStore) Set(k, v []byte) error {
	if _, err := s.db.Exec("INSERT OR REPLACE INTO store(k, v) VALUES(?, ?)", bytesToUint64(k), v); err != nil {
		return err
	}

	return nil
}

func (s *logStore) Get(k []byte) ([]byte, error) {
	v := []byte{}

	if err := s.db.QueryRow("SELECT v FROM store WHERE k = ?", bytesToUint64(k)).Scan(&v); err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.New("not found")
		}

		return nil, err
	}

	return v, nil
}

func (s *logStore) SetUint64(key []byte, val uint64) error {
	return s.Set(key, uint64ToBytes(val))
}

func (s *logStore) GetUint64(key []byte) (uint64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}

	return bytesToUint64(val), nil
}

func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	return buf, enc.Encode(in)
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
