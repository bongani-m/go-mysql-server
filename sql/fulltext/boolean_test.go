// Copyright 2026 Dolthub, Inc.
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

package fulltext

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestParseBooleanQuery(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tests := []struct {
		query    string
		expected []BooleanTerm
	}{
		{"", nil},
		{"+user ", []BooleanTerm{{Words: []string{"user"}, Kind: BooleanTermRequired}}},
		{"apple -banana", []BooleanTerm{
			{Words: []string{"apple"}, Kind: BooleanTermOptional},
			{Words: []string{"banana"}, Kind: BooleanTermExcluded},
		}},
		{"+app*", []BooleanTerm{{Words: []string{"app"}, Kind: BooleanTermRequired, Prefix: true}}},
		{"+a*", []BooleanTerm{{Words: []string{"a"}, Kind: BooleanTermRequired, Prefix: true}}},
		{`+"red apple" pie`, []BooleanTerm{
			{Words: []string{"red", "apple"}, Kind: BooleanTermRequired},
			{Words: []string{"pie"}, Kind: BooleanTermOptional},
		}},
		{`"unterminated phrase`, []BooleanTerm{{Words: []string{"unterminated", "phrase"}, Kind: BooleanTermOptional}}},
		// Words shorter than three characters are dropped, along with their operator.
		{"+an +cat", []BooleanTerm{{Words: []string{"cat"}, Kind: BooleanTermRequired}}},
		// An operator followed by a separator does not carry over to the next word.
		{"+ cat", []BooleanTerm{{Words: []string{"cat"}, Kind: BooleanTermOptional}}},
		{"don't", []BooleanTerm{{Words: []string{"don't"}, Kind: BooleanTermOptional}}},
	}
	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			q, err := ParseBooleanQuery(ctx, sql.Collation_Default, test.query)
			require.NoError(t, err)
			assert.Equal(t, test.expected, q.Terms)
		})
	}

	for _, query := range []string{"(apple banana)", ">apple", "<apple", "~apple", `"apple pie" @3`} {
		t.Run(query, func(t *testing.T) {
			_, err := ParseBooleanQuery(ctx, sql.Collation_Default, query)
			require.Error(t, err)
		})
	}
}

func TestBooleanQueryRelevancy(t *testing.T) {
	ctx := sql.NewEmptyContext()
	const doc = "The Quick brown fox jumps over the lazy dog"
	tests := []struct {
		query    string
		expected float32
	}{
		{"", 0},
		{"fox", 1},
		{"FOX", 1},
		{"cat", 0},
		{"fox dog cat", 2},
		{"+fox", 1},
		{"+fox +cat", 0},
		{"+fox cat", 1},
		{"+fox dog", 2},
		{"-cat", 0},
		{"fox -cat", 1},
		{"fox -dog", 0},
		{"+fox -dog", 0},
		{"jum*", 1},
		{"+qui*", 1},
		{"quic", 0},
		{"+jumpsover*", 0},
		{`"brown fox"`, 1},
		{`"fox brown"`, 0},
		{`+"quick brown fox" +lazy`, 2},
		{`-"lazy dog" fox`, 0},
		{`-"lazy cat" fox`, 1},
	}
	collation := sql.Collation_utf8mb4_0900_ai_ci
	parser, err := NewDefaultParser(ctx, collation, doc)
	require.NoError(t, err)
	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			q, err := ParseBooleanQuery(ctx, collation, test.query)
			require.NoError(t, err)
			relevancy, err := q.Relevancy(ctx, &parser)
			require.NoError(t, err)
			assert.Equal(t, test.expected, relevancy)
		})
	}
}

func TestBooleanQueryRelevancyBinaryCollation(t *testing.T) {
	ctx := sql.NewEmptyContext()
	parser, err := NewDefaultParser(ctx, sql.Collation_utf8mb4_0900_bin, "The Quick brown fox")
	require.NoError(t, err)
	for query, expected := range map[string]float32{"Quick": 1, "quick": 0, "Qui*": 1, "qui*": 0} {
		q, err := ParseBooleanQuery(ctx, sql.Collation_utf8mb4_0900_bin, query)
		require.NoError(t, err)
		relevancy, err := q.Relevancy(ctx, &parser)
		require.NoError(t, err)
		assert.Equal(t, expected, relevancy, query)
	}
}
