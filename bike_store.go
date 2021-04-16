package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// BikeStore is a sqlite3 database.
type BikeStore struct {
	DB *sql.DB
}

// Bike is used to store bikes in database.
type Bike struct {
	ID         uint64       `json:"id"`
	Name       string       `json:"name"`
	Components []*Component `json:"components"`
}

// Component is a part of a bike.
type Component struct {
	ID     uint64 `json:"id"`
	BikeID uint64 `json:"bike_id"`
	Name   string `json:"name"`
}

// NewBikeStore creates a database.
func NewBikeStore(path string) (*BikeStore, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS bike(name TEXT NOT NULL)"); err != nil {
		return nil, err
	}

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS component(bike_rowid INTEGER, name TEXT NOT NULL)"); err != nil {
		return nil, err
	}

	if _, err = db.Exec("CREATE INDEX IF NOT EXISTS component_bike_rowid_idx ON component(bike_rowid)"); err != nil {
		return nil, err
	}

	return &BikeStore{
		DB: db,
	}, nil
}

// GetBikes selects bikes from database.
func (bs *BikeStore) GetBikes(limit, offset uint64, bikes *[]*Bike) error {
	rows, err := bs.DB.Query("SELECT rowid, name FROM bike ORDER BY rowid DESC LIMIT ? OFFSET ?", limit, offset)
	if err != nil {
		return err
	}
	defer rows.Close()

	bikeIDs := []string{}
	for rows.Next() {
		b := Bike{}

		if err := rows.Scan(&b.ID, &b.Name); err != nil {
			return err
		}

		bikeIDs = append(bikeIDs, fmt.Sprint(b.ID))

		*bikes = append(*bikes, &b)
	}

	rows, err = bs.DB.Query(fmt.Sprintf("SELECT rowid, bike_rowid, name FROM component WHERE bike_rowid IN (%s)", strings.Join(bikeIDs, ",")))
	if err != nil {
		return err
	}
	defer rows.Close()

	components := []*Component{}
	for rows.Next() {
		component := Component{}

		if err := rows.Scan(&component.ID, &component.BikeID, &component.Name); err != nil {
			return err
		}

		components = append(components, &component)
	}

	for _, bike := range *bikes {
		for _, component := range components {
			if bike.ID == component.BikeID {
				bike.Components = append(bike.Components, component)
			}
		}
	}

	return nil
}

// GetBike get a bike from database.
func (bs *BikeStore) GetBike(id uint64, bike *Bike) error {
	if err := bs.DB.QueryRow("SELECT rowid, name FROM bike WHERE rowid = ?", id).Scan(&bike.ID, &bike.Name); err != nil {
		return err
	}

	rows, err := bs.DB.Query("SELECT rowid, bike_rowid, name FROM component WHERE bike_rowid = ?", id)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		component := Component{}

		if err := rows.Scan(&component.ID, &component.BikeID, &component.Name); err != nil {
			return err
		}

		bike.Components = append(bike.Components, &component)
	}

	return nil
}

// StoreBike inserts a bike into the database.
func (bs *BikeStore) StoreBike(bike *Bike) error {
	return bs.StoreBikes([]*Bike{bike})
}

// StoreBikes inserts some bikes into the database.
func (bs *BikeStore) StoreBikes(bikes []*Bike) error {
	tx, err := bs.DB.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT INTO bike(name) VALUES(?)")
	if err != nil {
		tx.Rollback()
		return err
	}

	for _, bike := range bikes {
		if _, err := stmt.Exec(bike.Name); err != nil {
			tx.Rollback()
			return err
		}

		if err := tx.QueryRow("SELECT last_insert_rowid()").Scan(&bike.ID); err != nil {
			tx.Rollback()
			return err
		}
	}

	stmt, err = tx.Prepare("INSERT INTO component(bike_rowid, name) VALUES(?, ?)")
	if err != nil {
		tx.Rollback()
		return err
	}

	for _, bike := range bikes {
		for _, component := range bike.Components {
			component.BikeID = bike.ID

			if _, err := stmt.Exec(component.BikeID, component.Name); err != nil {
				tx.Rollback()
				return err
			}

			if err := tx.QueryRow("SELECT last_insert_rowid()").Scan(&component.ID); err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	return tx.Commit()
}

// DeleteRange deletes some bikes.
func (bs *BikeStore) DeleteRange(min, max uint64) error {
	tx, err := bs.DB.Begin()
	if err != nil {
		return err
	}

	if _, err := bs.DB.Exec("DELETE FROM bike WHERE rowid BETWEEN ? AND ?", min, max); err != nil {
		tx.Rollback()
		return err
	}

	if _, err := bs.DB.Exec("DELETE FROM component WHERE bike_rowid BETWEEN ? AND ?", min, max); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
