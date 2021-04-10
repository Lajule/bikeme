package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
)

type getBikesHandler struct {
	s *bikeStore
}

type getBikeHandler struct {
	s *bikeStore
}

type postBikeHandler struct {
	s *bikeStore
}

func (h *getBikesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	bikes := []*bike{}
	if err := h.s.GetBikes(limit, offset, bikes); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, err.Error())
		return
	}

	data, err := json.Marshal(bikes)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, string(data))
}

func (h *getBikeHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	id := req.URL.Query().Get(":id")
	io.WriteString(w, id+"\n")
}

func (h *postBikeHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {

}
