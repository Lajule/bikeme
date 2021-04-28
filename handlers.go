package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/hashicorp/raft"
)

// IndexHandler renders the index page.
type IndexHandler struct {
	Application *Application
	Template    *template.Template
}

// ServeHTTP handles GET /.
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

// GetBikesHandler is a REST handler.
type GetBikesHandler struct {
	Application *Application
}

// ServeHTTP handles GET /bikes.
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

// GetBikeHandler is a REST handler.
type GetBikeHandler struct {
	Application *Application
}

// ServeHTTP handles GET /bikes/:id.
func (h *GetBikeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	id, err := strconv.ParseUint(vars["id"], 10, 64)
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

// PostBikesHandler is a REST handler.
type PostBikeHandler struct {
	Application *Application
}

// ServeHTTP handle POST /bikes.
func (h *PostBikeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, err.Error())
		return
	}

	if h.Application.Cluster.State() == raft.Leader {
		apply := h.Application.Cluster.Apply(body, 0)
		if err := apply.Error(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, err.Error())
			return
		}

		applyResponse := apply.Response()
		if err := applyResponse.(*ApplyResponse).Err; err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, err.Error())
			return
		}

		resp, err := json.Marshal(applyResponse.(*ApplyResponse).Bike)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, string(resp))
	} else {
		leader := strings.Split(string(h.Application.Cluster.Leader()), ":")
		req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s:%d%s", leader[0], h.Application.Config.APIPort, r.URL.Path), bytes.NewBuffer(body))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, err.Error())
			return
		}

		client := http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, err.Error())
			return
		}

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.WriteString(w, string(respBody))
	}
}
