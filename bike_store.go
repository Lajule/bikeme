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

	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS bike(name TEXT NOT NULL)`); err != nil {
		return nil, err
	}

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS component(bike_rowid INTEGER, name TEXT NOT NULL)"); err != nil {
		return nil, err
	}

	if _, err = db.Exec("CREATE INDEX IF NOT EXISTS component_bike_rowid_idx ON component(bike_rowid)"); err != nil {
		return nil, err
	}

	return &bikeStore{
		db: db,
	}, nil
}

// GetBikes Get same bikes
func (s *bikeStore) GetBikes(limit, offset uint64, bikes []*bike) error {
	rows, err := s.db.Query("SELECT rowid, name FROM bike LIMIT ? OFFSET ?", limit, offset)
	if err != nil {
		return err
	}
	defer rows.Close()

	bikeIds := []string{}

	for rows.Next() {
		b := bike{}

		if err := rows.Scan(&b.ID, &b.Name); err != nil {
			return err
		}

		bikes = append(bikes, &b)

		bikeIds = append(bikeIds, fmt.Sprint(b.ID))
	}

	rows, err = s.db.Query(fmt.Sprintf("SELECT rowid, bike_rowid, name FROM component WHERE bike_rowid IN (%s)", strings.Join(bikeIds, ",")))
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
	if err := s.db.QueryRow("SELECT rowid, name FROM bike WHERE rowid = ?", id).Scan(&b.ID, &b.Name); err != nil {
		return err
	}

	rows, err := s.db.Query("SELECT rowid, bike_rowid, name FROM component WHERE bike_rowid = ?", id)
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

	stmt, err := tx.Prepare("INSERT INTO bike(name) VALUES(?)")
	if err != nil {
		tx.Rollback()
		return err
	}

	for _, b := range bikes {
		if _, err := stmt.Exec(b.Name); err != nil {
			tx.Rollback()
			return err
		}
	}

	var id uint64

	if err := tx.QueryRow("SELECT last_insert_rowid()").Scan(&id); err != nil {
		tx.Rollback()
		return err
	}

	stmt, err = tx.Prepare("INSERT INTO component(bike_rowid, name) VALUES(?, ?)")
	if err != nil {
		tx.Rollback()
		return err
	}

	for _, b := range bikes {
		for _, c := range b.Components {
			if _, err := stmt.Exec(id, c.Name); err != nil {
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

	if _, err := s.db.Exec("DELETE FROM bike WHERE rowid BETWEEN ? AND ?", min, max); err != nil {
		tx.Rollback()
		return err
	}

	if _, err := s.db.Exec("DELETE FROM component WHERE bike_rowid BETWEEN ? AND ?", min, max); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
