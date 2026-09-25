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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRootLinksToCollection(t *testing.T) {
	rec := perform(t, NewHandler(newSeedStore(), ""), http.MethodGet, "/", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != halContentType {
		t.Fatalf("content type %q", ct)
	}
	body := decode[rootDoc](t, rec)
	if body.Links.People == nil || body.Links.People.Href != "http://example.com/people" {
		t.Fatalf("people link %#v", body.Links.People)
	}
	if body.Links.Self == nil || body.Links.Self.Href != "http://example.com/" {
		t.Fatalf("self link %#v", body.Links.Self)
	}
}

func TestListPaginationLinks(t *testing.T) {
	rec := perform(t, NewHandler(newSeedStore(), ""), http.MethodGet, "/people", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	body := decode[listDoc](t, rec)
	if body.Page != 1 || body.Size != 2 || body.Total != 4 {
		t.Fatalf("page meta %+v", body)
	}
	if len(body.Embedded.People) != 2 {
		t.Fatalf("got %d people", len(body.Embedded.People))
	}
	if body.Embedded.People[0].Name != "Jane Deo" || body.Embedded.People[1].Email != "jane@doe.com" {
		t.Fatalf("order %+v", body.Embedded.People)
	}
	if body.Embedded.People[0].CreatedAt != "2022-11-01T12:00:00.000001Z" {
		t.Fatalf("created_at %s", body.Embedded.People[0].CreatedAt)
	}
	self := body.Embedded.People[0].Links.Self
	if body.Embedded.People[0].ID != idJaneDeo || self == nil || self.Href != "http://example.com/people/"+idJaneDeo {
		t.Fatalf("item self id=%s %#v", body.Embedded.People[0].ID, self)
	}
	if body.Links.Prev != nil {
		t.Fatalf("unexpected prev %#v", body.Links.Prev)
	}
	assertPageQuery(t, body.Links.Next, "2", "2", url.Values{})
	assertPageQuery(t, body.Links.Last, "2", "2", url.Values{})
	assertPageQuery(t, body.Links.First, "1", "2", url.Values{})

	rec = perform(t, NewHandler(newSeedStore(), ""), http.MethodGet, "/people?page=2&size=2", "")
	body = decode[listDoc](t, rec)
	if body.Links.Next != nil {
		t.Fatalf("unexpected next %#v", body.Links.Next)
	}
	assertPageQuery(t, body.Links.Prev, "1", "2", url.Values{})
	if body.Embedded.People[0].ID != idJohnDoe || body.Embedded.People[1].ID != idJohnAlt {
		t.Fatalf("page 2 %+v", body.Embedded.People)
	}
}

func TestListCopiesFiltersOntoLinks(t *testing.T) {
	store := newSeedStore()
	target := "/people?name=Jane&email=doe.com&phone=555&created_after=2022-01-01T00:00:00Z&created_before=2023-01-01%2000:00:00&size=1"
	rec := perform(t, NewHandler(store, ""), http.MethodGet, target, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	store.mu.Lock()
	got := store.last
	store.mu.Unlock()
	if got.Name != "Jane" || got.Email != "doe.com" || got.Phone != "555" {
		t.Fatalf("filter %+v", got)
	}
	if got.CreatedAfter == nil || !got.CreatedAfter.Equal(time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("created_after %#v", got.CreatedAfter)
	}
	if got.CreatedBefore == nil || !got.CreatedBefore.Equal(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("created_before %#v", got.CreatedBefore)
	}

	body := decode[listDoc](t, rec)
	if body.Total != 0 || len(body.Embedded.People) != 0 {
		t.Fatalf("filtered page total=%d people=%+v", body.Total, body.Embedded.People)
	}
	if body.Links.Next != nil {
		t.Fatalf("unexpected next %#v", body.Links.Next)
	}
	want := url.Values{
		"name":           []string{"Jane"},
		"email":          []string{"doe.com"},
		"phone":          []string{"555"},
		"created_after":  []string{"2022-01-01T00:00:00Z"},
		"created_before": []string{"2023-01-01 00:00:00"},
	}
	assertPageQuery(t, body.Links.Self, "1", "1", want)

	rec = perform(t, NewHandler(newSeedStore(), ""), http.MethodGet, "/people?name=Jane&size=1", "")
	body = decode[listDoc](t, rec)
	if body.Total != 2 || body.Embedded.People[0].Email != "janedeo@gmail.com" {
		t.Fatalf("name filter %+v", body)
	}
	assertPageQuery(t, body.Links.Next, "2", "1", url.Values{"name": []string{"Jane"}})
}

func TestCreateGetUpdateDelete(t *testing.T) {
	store := newSeedStore()
	h := NewHandler(store, "http://api.test")

	rec := perform(t, h, http.MethodPost, "/people", `{
		"name": "Ada Lovelace",
		"email": "ada@example.com",
		"phone_numbers": ["111-222-333"],
		"created_at": "2024-05-06T07:08:09.000010Z"
	}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status %d body %s", rec.Code, rec.Body)
	}
	created := decode[personDoc](t, rec)
	if rec.Header().Get("Location") != created.Links.Self.Href {
		t.Fatalf("location %q self %#v", rec.Header().Get("Location"), created.Links.Self)
	}
	if _, err := uuid.Parse(created.ID); err != nil || created.Links.Self.Href != "http://api.test/people/"+created.ID {
		t.Fatalf("id %s self %q", created.ID, created.Links.Self.Href)
	}
	if created.Links.Collection.Href != "http://api.test/people" {
		t.Fatalf("collection %q", created.Links.Collection.Href)
	}

	rec = perform(t, h, http.MethodGet, "/people/"+created.ID, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get status %d", rec.Code)
	}
	got := decode[personDoc](t, rec)
	if len(got.PhoneNumbers) != 1 || got.PhoneNumbers[0] != "111-222-333" {
		t.Fatalf("phones %#v", got.PhoneNumbers)
	}

	rec = perform(t, h, http.MethodPut, "/people/"+created.ID, `{
		"name": "Ada Lovelace",
		"email": "ada@example.com",
		"phone_numbers": ["999"],
		"created_at": "2024-01-02 03:04:05"
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("update status %d body %s", rec.Code, rec.Body)
	}
	updated := decode[personDoc](t, rec)
	if updated.ID != created.ID || updated.PhoneNumbers[0] != "999" || updated.CreatedAt != "2024-01-02T03:04:05.000000Z" {
		t.Fatalf("updated %+v", updated)
	}

	rec = perform(t, h, http.MethodDelete, "/people/"+created.ID, "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status %d", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("delete body %q", rec.Body)
	}

	rec = perform(t, h, http.MethodGet, "/people/"+created.ID, "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing status %d", rec.Code)
	}
	missing := decode[errorDoc](t, rec)
	if missing.Links.Collection == nil || missing.Links.Collection.Href != "http://api.test/people" {
		t.Fatalf("404 links %+v", missing.Links)
	}
}

func TestBadInput(t *testing.T) {
	h := NewHandler(newSeedStore(), "")

	rec := perform(t, h, http.MethodGet, "/people?size=0", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("size status %d", rec.Code)
	}
	rec = perform(t, h, http.MethodGet, "/people?created_after=yesterday", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("time status %d", rec.Code)
	}
	rec = perform(t, h, http.MethodPost, "/people", `{"email":"a@b.c"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("create status %d", rec.Code)
	}
	rec = perform(t, h, http.MethodGet, "/people/nope", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("id status %d body %s", rec.Code, rec.Body)
	}
	rec = perform(t, h, http.MethodPut, "/people/"+idJaneDoe, `{"phone_numbers":[],"created_at":"2024-01-02T03:04:05Z"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("put status %d body %s", rec.Code, rec.Body)
	}
	rec = perform(t, h, http.MethodGet, "/people/"+idMissing, "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing status %d", rec.Code)
	}
}

func TestLikePatternEscapesWildcards(t *testing.T) {
	if got := likeContains(`100%_a\b`); got != `%100\%\_a\\b%` {
		t.Fatalf("like pattern %q", got)
	}
}

func perform(t *testing.T, h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rdr)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode: %v body=%s", err, rec.Body)
	}
	return v
}

func assertPageQuery(t *testing.T, l *link, page, size string, extra url.Values) {
	t.Helper()
	if l == nil {
		t.Fatal("missing link")
	}
	u, err := url.Parse(l.Href)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if q.Get("page") != page || q.Get("size") != size {
		t.Fatalf("page query %s", l.Href)
	}
	for key, vals := range extra {
		if q.Get(key) != vals[0] {
			t.Fatalf("query %s got %q want %q in %s", key, q.Get(key), vals[0], l.Href)
		}
	}
}

type memoryStore struct {
	mu     sync.Mutex
	people []Person
	last   Filter
}

const (
	idJaneDeo = "11111111-1111-4111-8111-111111111111"
	idJaneDoe = "22222222-2222-4222-8222-222222222222"
	idJohnDoe = "33333333-3333-4333-8333-333333333333"
	idJohnAlt = "44444444-4444-4444-8444-444444444444"
	idMissing = "99999999-9999-4999-8999-999999999999"
)

func newSeedStore() *memoryStore {
	created := time.Unix(0, 1667304000000001000).UTC()
	return &memoryStore{people: []Person{
		{ID: idJaneDeo, Name: "Jane Deo", Email: "janedeo@gmail.com", PhoneNumbers: []string{"556-565-566", "777-777-777"}, CreatedAt: created},
		{ID: idJaneDoe, Name: "Jane Doe", Email: "jane@doe.com", PhoneNumbers: []string{}, CreatedAt: created},
		{ID: idJohnDoe, Name: "John Doe", Email: "john@doe.com", PhoneNumbers: []string{"555-555-555"}, CreatedAt: created},
		{ID: idJohnAlt, Name: "John Doe", Email: "johnalt@doe.com", PhoneNumbers: []string{}, CreatedAt: created},
	}}
}

func (m *memoryStore) List(_ context.Context, f Filter, page, size int) (ListResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.last = f
	matched := make([]Person, 0, len(m.people))
	for _, p := range m.people {
		if !matchPerson(p, f) {
			continue
		}
		cp := p
		cp.PhoneNumbers = append([]string(nil), p.PhoneNumbers...)
		matched = append(matched, cp)
	}
	sortPeople(matched)
	total := len(matched)
	start := (page - 1) * size
	if start > total {
		start = total
	}
	end := start + size
	if end > total {
		end = total
	}
	return ListResult{People: append([]Person(nil), matched[start:end]...), Total: total}, nil
}

func (m *memoryStore) Get(_ context.Context, id string) (Person, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.people {
		if p.ID == id {
			p.PhoneNumbers = append([]string(nil), p.PhoneNumbers...)
			return p, nil
		}
	}
	return Person{}, ErrNotFound
}

func (m *memoryStore) Insert(_ context.Context, p Person) (Person, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p.ID = uuid.New().String()
	p.PhoneNumbers = append([]string(nil), normalizePhones(p.PhoneNumbers)...)
	m.people = append(m.people, p)
	return p, nil
}

func (m *memoryStore) Update(_ context.Context, p Person) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, existing := range m.people {
		if existing.ID == p.ID {
			p.PhoneNumbers = append([]string(nil), p.PhoneNumbers...)
			m.people[i] = p
			return nil
		}
	}
	return ErrNotFound
}

func (m *memoryStore) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, existing := range m.people {
		if existing.ID == id {
			m.people = append(m.people[:i], m.people[i+1:]...)
			return nil
		}
	}
	return ErrNotFound
}

func matchPerson(p Person, f Filter) bool {
	if f.Name != "" && !strings.Contains(p.Name, f.Name) {
		return false
	}
	if f.Email != "" && !strings.Contains(p.Email, f.Email) {
		return false
	}
	if f.Phone != "" && !strings.Contains(strings.Join(p.PhoneNumbers, " "), f.Phone) {
		return false
	}
	if f.CreatedAfter != nil && p.CreatedAt.Before(*f.CreatedAfter) {
		return false
	}
	if f.CreatedBefore != nil && p.CreatedAt.After(*f.CreatedBefore) {
		return false
	}
	return true
}

func sortPeople(people []Person) {
	for i := 1; i < len(people); i++ {
		j := i
		for j > 0 && (people[j].Name < people[j-1].Name || (people[j].Name == people[j-1].Name && people[j].Email < people[j-1].Email)) {
			people[j], people[j-1] = people[j-1], people[j]
			j--
		}
	}
}
