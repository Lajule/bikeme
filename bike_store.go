package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type bikeStore struct {
	db *sql.DB
}

type bike struct {
	ID         uint64
	Name       string
	Components []*component
}

type component struct {
	ID     uint64
	BikeID uint64
	Name   string
}

func newBikeStore(path string) (*bikeStore, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS bike(id INTEGER, name TEXT NOT NULL, PRIMARY KEY(id))`); err != nil {
		return nil, err
	}

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS component(id INTEGER, bike_id INTEGER, name TEXT NOT NULL, PRIMARY KEY(id))"); err != nil {
		return nil, err
	}

	if _, err = db.Exec("CREATE INDEX IF NOT EXISTS component_bike_id_idx ON component(bike_id)"); err != nil {
		return nil, err
	}

	return &bikeStore{
		db: db,
	}, nil
}

// GetBikes Get same bikes
func (s *bikeStore) GetBikes(limit, offset uint64, bikes []*bike) error {
	rows, err := s.db.Query("SELECT id, name FROM bike LIMIT ? OFFSET ?", limit, offset)
	if err != nil {
		return err
	}
	defer rows.Close()

	ids := []string{}

	for rows.Next() {
		b := bike{}

		if err := rows.Scan(&b.ID, &b.Name); err != nil {
			return err
		}

		bikes = append(bikes, &b)

		ids = append(ids, fmt.Sprint(b.ID))
	}

	rows, err = s.db.Query(fmt.Sprintf("SELECT id, bike_id, name FROM component WHERE bike_id IN (%s)", strings.Join(ids, ",")))
	if err != nil {
		return err
	}
	defer rows.Close()

	components := []*component{}

	for rows.Next() {
		c := component{}

		if err := rows.Scan(&c.ID, &c.BikeID, &c.Name); err != nil {
			return err
		}

		components = append(components, &c)
	}

	for _, b := range bikes {
		for _, c := range components {
			if b.ID == c.BikeID {
				b.Components = append(b.Components, c)
			}
		}
	}

	return nil
}

// GetBike Get a bike
func (s *bikeStore) GetBike(id uint64, b *bike) error {
	if err := s.db.QueryRow("SELECT id, name FROM bike WHERE id = ?", id).Scan(&b.ID, &b.Name); err != nil {
		return err
	}

	rows, err := s.db.Query("SELECT id, bike_id, name FROM component WHERE bike_id = ?", id)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		c := component{}

		if err := rows.Scan(&c.ID, &c.BikeID, &c.Name); err != nil {
			return err
		}

		b.Components = append(b.Components, &c)
	}

	return nil
}

// StoreBike is used to store a single bike
func (s *bikeStore) StoreBike(b *bike) error {
	return s.StoreBikes([]*bike{b})
}

// StoreBike is used to store a set of bikes
func (s *bikeStore) StoreBikes(bikes []*bike) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT OR REPLACE INTO bike(id, name) VALUES(?, ?)")
	if err != nil {
		return err
	}

	for _, b := range bikes {
		if _, err := stmt.Exec(b.ID, b.Name); err != nil {
			tx.Rollback()
			return err
		}
	}

	stmt, err = tx.Prepare("INSERT OR REPLACE INTO component(id, bike_id, name) VALUES(?, ?, ?)")
	if err != nil {
		return err
	}

	for _, b := range bikes {
		for _, c := range b.Components {
			if _, err := stmt.Exec(c.ID, c.BikeID, c.Name); err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	return tx.Commit()
}

// DeleteRange is used to delete bikes within a given range inclusively.
func (s *bikeStore) DeleteRange(min, max uint64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	if _, err := s.db.Exec("DELETE FROM bike WHERE id BETWEEN ? AND ?", min, max); err != nil {
		tx.Rollback()
		return err
	}

	if _, err := s.db.Exec("DELETE FROM component WHERE bike_id BETWEEN ? AND ?", min, max); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
