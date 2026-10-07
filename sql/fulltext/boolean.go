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
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/dolthub/go-mysql-server/sql"
)

// BooleanTermKind states whether a term in a boolean search must, must not, or may appear in a document.
type BooleanTermKind byte

const (
	BooleanTermOptional BooleanTermKind = iota // no operator
	BooleanTermRequired                        // +
	BooleanTermExcluded                        // -
)

// BooleanTerm is a single word, prefix, or phrase from a boolean search string.
type BooleanTerm struct {
	// Words holds one word for a word or prefix term, and one or more words for a phrase.
	Words []string
	Kind  BooleanTermKind
	// Prefix is true when the word ended with the truncation operator (*).
	Prefix bool
}

// BooleanQuery is a parsed "IN BOOLEAN MODE" search string. It supports the +, - and * operators and double-quoted
// phrases. Grouping, the relevance operators (<, >, ~) and proximity search (@) are not supported.
type BooleanQuery struct {
	Terms     []BooleanTerm
	collation sql.CollationID
}

// ParseBooleanQuery parses |query| using MySQL's boolean search syntax. Words are split with the same rules as
// DefaultParser, so a word shorter than three characters is dropped unless it ends with the truncation operator.
func ParseBooleanQuery(ctx *sql.Context, collation sql.CollationID, query string) (BooleanQuery, error) {
	q := BooleanQuery{collation: collation}
	kind := BooleanTermOptional
	for i := 0; i < len(query); {
		r, size := utf8.DecodeRuneInString(query[i:])
		switch {
		case r == '+':
			kind = BooleanTermRequired
			i += size
		case r == '-':
			kind = BooleanTermExcluded
			i += size
		case r == '"':
			end := i + size
			for end < len(query) && query[end] != '"' {
				end++
			}
			words, err := parseWords(ctx, collation, query[i+size:end])
			if err != nil {
				return BooleanQuery{}, err
			}
			if len(words) > 0 {
				q.Terms = append(q.Terms, BooleanTerm{Words: words, Kind: kind})
			}
			kind = BooleanTermOptional
			i = end + 1
		case r == '(' || r == ')' || r == '<' || r == '>' || r == '~' || r == '@':
			return BooleanQuery{}, fmt.Errorf(`the "%c" operator is not supported in "IN BOOLEAN MODE"`, r)
		case isWordRune(r):
			end := i
			for end < len(query) {
				r, size := utf8.DecodeRuneInString(query[end:])
				if !isWordRune(r) && r != '\'' {
					break
				}
				end += size
			}
			prefix := end < len(query) && query[end] == '*'
			words, err := parseWords(ctx, collation, query[i:end])
			if err != nil {
				return BooleanQuery{}, err
			}
			if len(words) > 0 {
				// More than one word can only come from stray apostrophes, so those words are matched as a phrase.
				q.Terms = append(q.Terms, BooleanTerm{Words: words, Kind: kind, Prefix: prefix && len(words) == 1})
			} else if prefix {
				// A truncated word is kept even when it is shorter than the minimum word length.
				if word := trimApostrophes(query[i:end]); word != "" {
					q.Terms = append(q.Terms, BooleanTerm{Words: []string{word}, Kind: kind, Prefix: true})
				}
			}
			kind = BooleanTermOptional
			i = end
			if prefix {
				i++
			}
		default:
			// Whitespace and other punctuation separate terms, and an operator only applies to the term right after it.
			kind = BooleanTermOptional
			i += size
		}
	}
	return q, nil
}

// Relevancy returns the relevancy of the document held by |doc|. A document matches when it contains every required
// term, no excluded term, and, when there are no required terms, at least one optional term. A matching document scores
// the number of required and optional terms it contains. A document that does not match scores zero.
func (q BooleanQuery) Relevancy(ctx *sql.Context, doc *DefaultParser) (float32, error) {
	hashes := make([]uint64, len(doc.words))
	for i, word := range doc.words {
		hash, err := q.collation.HashToUint(word.Word)
		if err != nil {
			return 0, err
		}
		hashes[i] = hash
	}

	matched := 0
	hasRequired := false
	for _, term := range q.Terms {
		found, err := q.contains(doc, hashes, term)
		if err != nil {
			return 0, err
		}
		switch term.Kind {
		case BooleanTermExcluded:
			if found {
				return 0, nil
			}
		case BooleanTermRequired:
			if !found {
				return 0, nil
			}
			hasRequired = true
			matched++
		case BooleanTermOptional:
			if found {
				matched++
			}
		}
	}
	if matched == 0 && !hasRequired {
		return 0, nil
	}
	return float32(matched), nil
}

// contains reports whether |term| appears in |doc|. |hashes| holds the collation hash of each word in |doc|.
func (q BooleanQuery) contains(doc *DefaultParser, hashes []uint64, term BooleanTerm) (bool, error) {
	if term.Prefix {
		prefixLen := utf8.RuneCountInString(term.Words[0])
		want, err := q.collation.HashToUint(term.Words[0])
		if err != nil {
			return false, err
		}
		for _, word := range doc.words {
			if utf8.RuneCountInString(word.Word) < prefixLen {
				continue
			}
			got, err := q.collation.HashToUint(firstRunes(word.Word, prefixLen))
			if err != nil {
				return false, err
			}
			if got == want {
				return true, nil
			}
		}
		return false, nil
	}

	want := make([]uint64, len(term.Words))
	for i, word := range term.Words {
		hash, err := q.collation.HashToUint(word)
		if err != nil {
			return false, err
		}
		want[i] = hash
	}
	for start := 0; start+len(want) <= len(hashes); start++ {
		match := true
		for i := range want {
			if hashes[start+i] != want[i] {
				match = false
				break
			}
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

// parseWords splits |text| into words using the same rules as DefaultParser.
func parseWords(ctx *sql.Context, collation sql.CollationID, text string) ([]string, error) {
	parser, err := NewDefaultParser(ctx, collation, text)
	if err != nil {
		return nil, err
	}
	words := make([]string, len(parser.words))
	for i, word := range parser.words {
		words[i] = word.Word
	}
	return words, nil
}

// isWordRune matches the characters that DefaultParser treats as part of a word.
func isWordRune(r rune) bool {
	return ((unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.IsDigit(r)) && !unicode.IsPunct(r)) || r == '_'
}

// trimApostrophes removes leading and trailing apostrophes, as newParserWord does.
func trimApostrophes(word string) string {
	for len(word) > 0 && word[0] == '\'' {
		word = word[1:]
	}
	for len(word) > 0 && word[len(word)-1] == '\'' {
		word = word[:len(word)-1]
	}
	return word
}

// firstRunes returns the first |n| runes of |s|.
func firstRunes(s string, n int) string {
	for i := range s {
		if n == 0 {
			return s[:i]
		}
		n--
	}
	return s
}
