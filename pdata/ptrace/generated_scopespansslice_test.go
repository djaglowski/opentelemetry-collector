// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Code generated by "pdata/internal/cmd/pdatagen/main.go". DO NOT EDIT.
// To regenerate this file run "make genpdata".

package ptrace

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"

	"go.opentelemetry.io/collector/pdata/internal"
	otlptrace "go.opentelemetry.io/collector/pdata/internal/data/protogen/trace/v1"
)

func TestScopeSpansSlice(t *testing.T) {
	es := NewScopeSpansSlice()
	assert.Equal(t, 0, es.Len())
	state := internal.StateMutable
	es = newScopeSpansSlice(&[]*otlptrace.ScopeSpans{}, &state)
	assert.Equal(t, 0, es.Len())

	emptyVal := NewScopeSpans()
	testVal := generateTestScopeSpans()
	for i := 0; i < 7; i++ {
		el := es.AppendEmpty()
		assert.Equal(t, emptyVal, es.At(i))
		fillTestScopeSpans(el)
		assert.Equal(t, testVal, es.At(i))
	}
	assert.Equal(t, 7, es.Len())
}

func TestScopeSpansSliceReadOnly(t *testing.T) {
	sharedState := internal.StateReadOnly
	es := newScopeSpansSlice(&[]*otlptrace.ScopeSpans{}, &sharedState)
	assert.Equal(t, 0, es.Len())
	assert.Panics(t, func() { es.AppendEmpty() })
	assert.Panics(t, func() { es.EnsureCapacity(2) })
	es2 := NewScopeSpansSlice()
	es.CopyTo(es2)
	assert.Panics(t, func() { es2.CopyTo(es) })
	assert.Panics(t, func() { es.MoveAndAppendTo(es2) })
	assert.Panics(t, func() { es2.MoveAndAppendTo(es) })
}

func TestScopeSpansSlice_CopyTo(t *testing.T) {
	dest := NewScopeSpansSlice()
	// Test CopyTo to empty
	NewScopeSpansSlice().CopyTo(dest)
	assert.Equal(t, NewScopeSpansSlice(), dest)

	// Test CopyTo larger slice
	generateTestScopeSpansSlice().CopyTo(dest)
	assert.Equal(t, generateTestScopeSpansSlice(), dest)

	// Test CopyTo same size slice
	generateTestScopeSpansSlice().CopyTo(dest)
	assert.Equal(t, generateTestScopeSpansSlice(), dest)
}

func TestScopeSpansSlice_EnsureCapacity(t *testing.T) {
	es := generateTestScopeSpansSlice()

	// Test ensure smaller capacity.
	const ensureSmallLen = 4
	es.EnsureCapacity(ensureSmallLen)
	assert.Less(t, ensureSmallLen, es.Len())
	assert.Equal(t, es.Len(), cap(*es.orig))
	assert.Equal(t, generateTestScopeSpansSlice(), es)

	// Test ensure larger capacity
	const ensureLargeLen = 9
	es.EnsureCapacity(ensureLargeLen)
	assert.Less(t, generateTestScopeSpansSlice().Len(), ensureLargeLen)
	assert.Equal(t, ensureLargeLen, cap(*es.orig))
	assert.Equal(t, generateTestScopeSpansSlice(), es)
}

func TestScopeSpansSlice_MoveAndAppendTo(t *testing.T) {
	// Test MoveAndAppendTo to empty
	expectedSlice := generateTestScopeSpansSlice()
	dest := NewScopeSpansSlice()
	src := generateTestScopeSpansSlice()
	src.MoveAndAppendTo(dest)
	assert.Equal(t, generateTestScopeSpansSlice(), dest)
	assert.Equal(t, 0, src.Len())
	assert.Equal(t, expectedSlice.Len(), dest.Len())

	// Test MoveAndAppendTo empty slice
	src.MoveAndAppendTo(dest)
	assert.Equal(t, generateTestScopeSpansSlice(), dest)
	assert.Equal(t, 0, src.Len())
	assert.Equal(t, expectedSlice.Len(), dest.Len())

	// Test MoveAndAppendTo not empty slice
	generateTestScopeSpansSlice().MoveAndAppendTo(dest)
	assert.Equal(t, 2*expectedSlice.Len(), dest.Len())
	for i := 0; i < expectedSlice.Len(); i++ {
		assert.Equal(t, expectedSlice.At(i), dest.At(i))
		assert.Equal(t, expectedSlice.At(i), dest.At(i+expectedSlice.Len()))
	}
}

func TestScopeSpansSlice_RemoveIf(t *testing.T) {
	// Test RemoveIf on empty slice
	emptySlice := NewScopeSpansSlice()
	emptySlice.RemoveIf(func(el ScopeSpans) bool {
		t.Fail()
		return false
	})

	// Test RemoveIf
	filtered := generateTestScopeSpansSlice()
	pos := 0
	filtered.RemoveIf(func(el ScopeSpans) bool {
		pos++
		return pos%3 == 0
	})
	assert.Equal(t, 5, filtered.Len())
}

func TestScopeSpansSlice_Sort(t *testing.T) {
	es := generateTestScopeSpansSlice()
	es.Sort(func(a, b ScopeSpans) bool {
		return uintptr(unsafe.Pointer(a.orig)) < uintptr(unsafe.Pointer(b.orig))
	})
	for i := 1; i < es.Len(); i++ {
		assert.True(t, uintptr(unsafe.Pointer(es.At(i-1).orig)) < uintptr(unsafe.Pointer(es.At(i).orig)))
	}
	es.Sort(func(a, b ScopeSpans) bool {
		return uintptr(unsafe.Pointer(a.orig)) > uintptr(unsafe.Pointer(b.orig))
	})
	for i := 1; i < es.Len(); i++ {
		assert.True(t, uintptr(unsafe.Pointer(es.At(i-1).orig)) > uintptr(unsafe.Pointer(es.At(i).orig)))
	}
}

func TestScopeSpansSlice_Range(t *testing.T) {
	// Test _Range on empty slice
	emptySlice := NewScopeSpansSlice()
	emptySlice.Range(func(i int, el ScopeSpans) {
		t.Fail()
	})

	// Test _Range
	slice := generateTestScopeSpansSlice()
	total := 0
	slice.Range(func(i int, el ScopeSpans) {
		total += i
	})
	assert.Equal(t, 0+1+2+3+4+5+6, total)
}

func generateTestScopeSpansSlice() ScopeSpansSlice {
	es := NewScopeSpansSlice()
	fillTestScopeSpansSlice(es)
	return es
}

func fillTestScopeSpansSlice(es ScopeSpansSlice) {
	*es.orig = make([]*otlptrace.ScopeSpans, 7)
	for i := 0; i < 7; i++ {
		(*es.orig)[i] = &otlptrace.ScopeSpans{}
		fillTestScopeSpans(newScopeSpans((*es.orig)[i], es.state))
	}
}
