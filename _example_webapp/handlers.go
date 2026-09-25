// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	halContentType = "application/hal+json"
	defaultPage    = 1
	defaultSize    = 2
	maxPageSize    = 100
	timeLayout     = "2006-01-02T15:04:05.000000Z"
)

// NewHandler serves the people collection. publicBaseURL, when set, is used
// for every link instead of the incoming request host.
func NewHandler(store Store, publicBaseURL string) http.Handler {
	h := &handler{store: store, publicBase: strings.TrimRight(publicBaseURL, "/")}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", h.root)
	mux.HandleFunc("GET /people", h.list)
	mux.HandleFunc("POST /people", h.create)
	mux.HandleFunc("GET /people/{id}", h.get)
	mux.HandleFunc("PUT /people/{id}", h.update)
	mux.HandleFunc("DELETE /people/{id}", h.delete)
	h.mux = mux
	return h
}

type handler struct {
	store      Store
	publicBase string
	mux        *http.ServeMux
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

type link struct {
	Href string `json:"href"`
}

type linkSet struct {
	Self       *link `json:"self,omitempty"`
	Collection *link `json:"collection,omitempty"`
	People     *link `json:"people,omitempty"`
	First      *link `json:"first,omitempty"`
	Prev       *link `json:"prev,omitempty"`
	Next       *link `json:"next,omitempty"`
	Last       *link `json:"last,omitempty"`
}

type personDoc struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Email        string   `json:"email"`
	PhoneNumbers []string `json:"phone_numbers"`
	CreatedAt    string   `json:"created_at"`
	Links        linkSet  `json:"_links"`
}

type listDoc struct {
	Page     int `json:"page"`
	Size     int `json:"size"`
	Total    int `json:"total"`
	Embedded struct {
		People []personDoc `json:"people"`
	} `json:"_embedded"`
	Links linkSet `json:"_links"`
}

type rootDoc struct {
	Links linkSet `json:"_links"`
}

type errorDoc struct {
	Error string  `json:"error"`
	Links linkSet `json:"_links"`
}

type personRequest struct {
	Name         string   `json:"name"`
	Email        string   `json:"email"`
	PhoneNumbers []string `json:"phone_numbers"`
	CreatedAt    *string  `json:"created_at"`
}

func (h *handler) root(w http.ResponseWriter, r *http.Request) {
	base := h.baseURL(r)
	writeJSON(w, http.StatusOK, rootDoc{Links: linkSet{
		Self:   &link{Href: base + "/"},
		People: &link{Href: base + "/people"},
	}})
}

func (h *handler) list(w http.ResponseWriter, r *http.Request) {
	page, size, f, err := parseListQuery(r)
	if err != nil {
		h.writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	result, err := h.store.List(r.Context(), f, page, size)
	if err != nil {
		h.writeStoreErr(w, r, err)
		return
	}

	base := h.baseURL(r)
	people := make([]personDoc, 0, len(result.People))
	for _, p := range result.People {
		people = append(people, h.personDoc(base, p))
	}
	doc := listDoc{Page: page, Size: size, Total: result.Total}
	doc.Embedded.People = people
	doc.Links = pageLinks(base, page, size, result.Total, r.URL.Query())
	writeJSON(w, http.StatusOK, doc)
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	p, err := h.store.Get(r.Context(), id)
	if err != nil {
		h.writeStoreErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, h.personDoc(h.baseURL(r), p))
}

func (h *handler) create(w http.ResponseWriter, r *http.Request) {
	var req personRequest
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	p, err := personFromCreate(req)
	if err != nil {
		h.writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	p, err = h.store.Insert(r.Context(), p)
	if err != nil {
		h.writeStoreErr(w, r, err)
		return
	}
	doc := h.personDoc(h.baseURL(r), p)
	w.Header().Set("Location", doc.Links.Self.Href)
	writeJSON(w, http.StatusCreated, doc)
}

func (h *handler) update(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	var req personRequest
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	p, err := personFromUpdate(id, req)
	if err != nil {
		h.writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.store.Update(r.Context(), p); err != nil {
		h.writeStoreErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, h.personDoc(h.baseURL(r), p))
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.store.Delete(r.Context(), id); err != nil {
		h.writeStoreErr(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) personDoc(base string, p Person) personDoc {
	return personDoc{
		ID:           p.ID,
		Name:         p.Name,
		Email:        p.Email,
		PhoneNumbers: normalizePhones(p.PhoneNumbers),
		CreatedAt:    p.CreatedAt.UTC().Format(timeLayout),
		Links: linkSet{
			Self:       &link{Href: personHref(base, p.ID)},
			Collection: &link{Href: base + "/people"},
		},
	}
}

func (h *handler) baseURL(r *http.Request) string {
	if h.publicBase != "" {
		return h.publicBase
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

func (h *handler) writeStoreErr(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		h.writeError(w, r, http.StatusNotFound, "not found")
	case errors.Is(err, ErrConflict):
		h.writeError(w, r, http.StatusConflict, "person already exists")
	default:
		log.Printf("store error: %v", err)
		h.writeError(w, r, http.StatusInternalServerError, "internal error")
	}
}

func (h *handler) writeError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	writeJSON(w, status, errorDoc{
		Error: msg,
		Links: linkSet{Collection: &link{Href: h.baseURL(r) + "/people"}},
	})
}

func personHref(base string, id string) string {
	return base + "/people/" + url.PathEscape(id)
}

func parseID(s string) (string, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return "", errors.New("id must be a UUID")
	}
	return id.String(), nil
}

func pageLinks(base string, page, size, total int, q url.Values) linkSet {
	last := 1
	if total > 0 {
		last = (total + size - 1) / size
	}
	links := linkSet{
		Self:  &link{Href: pageHref(base, page, size, q)},
		First: &link{Href: pageHref(base, 1, size, q)},
		Last:  &link{Href: pageHref(base, last, size, q)},
	}
	if page > 1 {
		links.Prev = &link{Href: pageHref(base, page-1, size, q)}
	}
	if page < last {
		links.Next = &link{Href: pageHref(base, page+1, size, q)}
	}
	return links
}

func pageHref(base string, page, size int, q url.Values) string {
	v := url.Values{}
	v.Set("page", strconv.Itoa(page))
	v.Set("size", strconv.Itoa(size))
	for _, key := range []string{"name", "email", "phone", "created_after", "created_before"} {
		if s := q.Get(key); s != "" {
			v.Set(key, s)
		}
	}
	return base + "/people?" + v.Encode()
}

func parseListQuery(r *http.Request) (int, int, Filter, error) {
	q := r.URL.Query()
	page := defaultPage
	if s := q.Get("page"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			return 0, 0, Filter{}, errors.New("page must be an integer >= 1")
		}
		page = n
	}
	size := defaultSize
	if s := q.Get("size"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > maxPageSize {
			return 0, 0, Filter{}, fmt.Errorf("size must be an integer from 1 to %d", maxPageSize)
		}
		size = n
	}
	if page > (int(^uint(0)>>1))/size {
		return 0, 0, Filter{}, errors.New("page is too large")
	}

	f := Filter{Name: q.Get("name"), Email: q.Get("email"), Phone: q.Get("phone")}
	if s := q.Get("created_after"); s != "" {
		t, err := parseFilterTime("created_after", s)
		if err != nil {
			return 0, 0, Filter{}, err
		}
		f.CreatedAfter = &t
	}
	if s := q.Get("created_before"); s != "" {
		t, err := parseFilterTime("created_before", s)
		if err != nil {
			return 0, 0, Filter{}, err
		}
		f.CreatedBefore = &t
	}
	return page, size, f, nil
}

func personFromCreate(req personRequest) (Person, error) {
	name := strings.TrimSpace(req.Name)
	email := strings.TrimSpace(req.Email)
	if name == "" || email == "" {
		return Person{}, errors.New("name and email are required")
	}
	created := time.Now().UTC()
	if req.CreatedAt != nil {
		t, err := parseFilterTime("created_at", *req.CreatedAt)
		if err != nil {
			return Person{}, err
		}
		created = t
	}
	return Person{
		Name:         name,
		Email:        email,
		PhoneNumbers: normalizePhones(req.PhoneNumbers),
		CreatedAt:    created,
	}, nil
}

func personFromUpdate(id string, req personRequest) (Person, error) {
	name := strings.TrimSpace(req.Name)
	email := strings.TrimSpace(req.Email)
	if name == "" || email == "" {
		return Person{}, errors.New("name and email are required")
	}
	if req.PhoneNumbers == nil {
		return Person{}, errors.New("phone_numbers is required")
	}
	if req.CreatedAt == nil {
		return Person{}, errors.New("created_at is required")
	}
	created, err := parseFilterTime("created_at", *req.CreatedAt)
	if err != nil {
		return Person{}, err
	}
	return Person{
		ID:           id,
		Name:         name,
		Email:        email,
		PhoneNumbers: req.PhoneNumbers,
		CreatedAt:    created,
	}, nil
}

func parseFilterTime(label, value string) (time.Time, error) {
	t, err := parseTime(value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be RFC3339 or YYYY-MM-DD HH:MM:SS", label)
	}
	return t, nil
}

func parseTime(value string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}
	var last error
	for _, layout := range layouts {
		t, err := time.Parse(layout, value)
		if err == nil {
			return t.UTC(), nil
		}
		last = err
	}
	return time.Time{}, last
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		media, _, err := mime.ParseMediaType(ct)
		if err != nil || (media != "application/json" && media != halContentType) {
			return errors.New("content type must be application/json")
		}
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return publicDecodeError(err)
	}
	return nil
}

func publicDecodeError(err error) error {
	var max *http.MaxBytesError
	if errors.As(err, &max) {
		return errors.New("request body is too large")
	}
	if errors.Is(err, io.EOF) {
		return errors.New("request body is required")
	}
	var syn *json.SyntaxError
	if errors.As(err, &syn) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errors.New("invalid JSON body")
	}
	var ute *json.UnmarshalTypeError
	if errors.As(err, &ute) && ute.Field != "" {
		return fmt.Errorf("%s has the wrong type", ute.Field)
	}
	return err
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", halContentType)
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		log.Printf("write response: %v", err)
	}
}
