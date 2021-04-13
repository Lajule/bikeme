package main

import (
	"database/sql"
	"encoding/json"
	"html/template"
	"io"
	"net/http"
	"strconv"
)

// IndexHandler renders the index page.
type IndexHandler struct {
	Application *Application
	Template    *template.Template
}

// GetBikesHandler is a REST handler.
type GetBikesHandler struct {
	Application *Application
}

// GetBikeHandler is a REST handler.
type GetBikeHandler struct {
	Application *Application
}

// PostBikesHandler is REST handler.
type PostBikeHandler struct {
	Application *Application
}

// ServeHTTP handles GET call.
func (h *IndexHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	limit, err := strconv.ParseUint(r.URL.Query().Get("limit"), 10, 64)
	if err != nil {
		limit = uint64(50)
	}

	offset, err := strconv.ParseUint(r.URL.Query().Get("offset"), 10, 64)
	if err != nil {
		offset = uint64(0)
	}

	data := struct {
		Limit  uint64
		Offset uint64
		Bikes  []*Bike
	}{
		Limit:  limit,
		Offset: limit + offset,
		Bikes:  []*Bike{},
	}

	if err := h.Application.BikeStore.GetBikes(limit, offset, &data.Bikes); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	h.Template.Execute(w, data)
}

// ServeHTTP handles GET call.
func (h *GetBikesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	limit, err := strconv.ParseUint(r.URL.Query().Get("limit"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, err.Error())
		return
	}

	offset, err := strconv.ParseUint(r.URL.Query().Get("offset"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, err.Error())
		return
	}

	bikes := []*Bike{}
	if err := h.Application.BikeStore.GetBikes(limit, offset, &bikes); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, err.Error())
		return
	}

	resp, err := json.Marshal(bikes)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, string(resp))
}

// ServeHTTP handles GET call.
func (h *GetBikeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.URL.Query().Get(":id"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, err.Error())
		return
	}

	bike := Bike{}
	if err := h.Application.BikeStore.GetBike(id, &bike); err != nil {
		if err == sql.ErrNoRows {
			w.WriteHeader(http.StatusBadRequest)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		io.WriteString(w, err.Error())
		return
	}

	resp, err := json.Marshal(bike)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, string(resp))
}

// ServeHTTP handle POST call.
func (h *PostBikeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, err.Error())
		return
	}

	future := h.Application.Cluster.Apply(body, parseDuration(h.Application.Configuration.TCPTimeout))
	if err := future.Error(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, err.Error())
		return
	}

	futureResponse := future.Response()
	if err := futureResponse.(*ApplyResponse).Err; err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, err.Error())
		return
	}

	resp, err := json.Marshal(futureResponse.(*ApplyResponse).Bike)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, string(resp))
}
