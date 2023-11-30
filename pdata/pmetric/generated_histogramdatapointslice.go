// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Code generated by "pdata/internal/cmd/pdatagen/main.go". DO NOT EDIT.
// To regenerate this file run "make genpdata".

package pmetric

import (
	"sort"

	"go.opentelemetry.io/collector/pdata/internal"
	otlpmetrics "go.opentelemetry.io/collector/pdata/internal/data/protogen/metrics/v1"
)

// HistogramDataPointSlice logically represents a slice of HistogramDataPoint.
//
// This is a reference type. If passed by value and callee modifies it, the
// caller will see the modification.
//
// Must use NewHistogramDataPointSlice function to create new instances.
// Important: zero-initialized instance is not valid for use.
type HistogramDataPointSlice struct {
	orig  *[]*otlpmetrics.HistogramDataPoint
	state *internal.State
}

func newHistogramDataPointSlice(orig *[]*otlpmetrics.HistogramDataPoint, state *internal.State) HistogramDataPointSlice {
	return HistogramDataPointSlice{orig: orig, state: state}
}

// NewHistogramDataPointSlice creates a HistogramDataPointSlice with 0 elements.
// Can use "EnsureCapacity" to initialize with a given capacity.
func NewHistogramDataPointSlice() HistogramDataPointSlice {
	orig := []*otlpmetrics.HistogramDataPoint(nil)
	state := internal.StateMutable
	return newHistogramDataPointSlice(&orig, &state)
}

// Len returns the number of elements in the slice.
//
// Returns "0" for a newly instance created with "NewHistogramDataPointSlice()".
func (es HistogramDataPointSlice) Len() int {
	return len(*es.orig)
}

// At returns the element at the given index.
//
// This function is used mostly for iterating over all the values in the slice:
//
//	for i := 0; i < es.Len(); i++ {
//	    e := es.At(i)
//	    ... // Do something with the element
//	}
func (es HistogramDataPointSlice) At(i int) HistogramDataPoint {
	return newHistogramDataPoint((*es.orig)[i], es.state)
}

// EnsureCapacity is an operation that ensures the slice has at least the specified capacity.
// 1. If the newCap <= cap then no change in capacity.
// 2. If the newCap > cap then the slice capacity will be expanded to equal newCap.
//
// Here is how a new HistogramDataPointSlice can be initialized:
//
//	es := NewHistogramDataPointSlice()
//	es.EnsureCapacity(4)
//	for i := 0; i < 4; i++ {
//	    e := es.AppendEmpty()
//	    // Here should set all the values for e.
//	}
func (es HistogramDataPointSlice) EnsureCapacity(newCap int) {
	es.state.AssertMutable()
	oldCap := cap(*es.orig)
	if newCap <= oldCap {
		return
	}

	newOrig := make([]*otlpmetrics.HistogramDataPoint, len(*es.orig), newCap)
	copy(newOrig, *es.orig)
	*es.orig = newOrig
}

// AppendEmpty will append to the end of the slice an empty HistogramDataPoint.
// It returns the newly added HistogramDataPoint.
func (es HistogramDataPointSlice) AppendEmpty() HistogramDataPoint {
	es.state.AssertMutable()
	*es.orig = append(*es.orig, &otlpmetrics.HistogramDataPoint{})
	return es.At(es.Len() - 1)
}

// MoveAndAppendTo moves all elements from the current slice and appends them to the dest.
// The current slice will be cleared.
func (es HistogramDataPointSlice) MoveAndAppendTo(dest HistogramDataPointSlice) {
	es.state.AssertMutable()
	dest.state.AssertMutable()
	if *dest.orig == nil {
		// We can simply move the entire vector and avoid any allocations.
		*dest.orig = *es.orig
	} else {
		*dest.orig = append(*dest.orig, *es.orig...)
	}
	*es.orig = nil
}

// RemoveIf calls f sequentially for each element present in the slice.
// If f returns true, the element is removed from the slice.
func (es HistogramDataPointSlice) RemoveIf(f func(HistogramDataPoint) bool) {
	es.state.AssertMutable()
	newLen := 0
	for i := 0; i < len(*es.orig); i++ {
		if f(es.At(i)) {
			continue
		}
		if newLen == i {
			// Nothing to move, element is at the right place.
			newLen++
			continue
		}
		(*es.orig)[newLen] = (*es.orig)[i]
		newLen++
	}
	*es.orig = (*es.orig)[:newLen]
}

// CopyTo copies all elements from the current slice overriding the destination.
func (es HistogramDataPointSlice) CopyTo(dest HistogramDataPointSlice) {
	dest.state.AssertMutable()
	srcLen := es.Len()
	destCap := cap(*dest.orig)
	if srcLen <= destCap {
		(*dest.orig) = (*dest.orig)[:srcLen:destCap]
		for i := range *es.orig {
			newHistogramDataPoint((*es.orig)[i], es.state).CopyTo(newHistogramDataPoint((*dest.orig)[i], dest.state))
		}
		return
	}
	origs := make([]otlpmetrics.HistogramDataPoint, srcLen)
	wrappers := make([]*otlpmetrics.HistogramDataPoint, srcLen)
	for i := range *es.orig {
		wrappers[i] = &origs[i]
		newHistogramDataPoint((*es.orig)[i], es.state).CopyTo(newHistogramDataPoint(wrappers[i], dest.state))
	}
	*dest.orig = wrappers
}

// Sort sorts the HistogramDataPoint elements within HistogramDataPointSlice given the
// provided less function so that two instances of HistogramDataPointSlice
// can be compared.
func (es HistogramDataPointSlice) Sort(less func(a, b HistogramDataPoint) bool) {
	es.state.AssertMutable()
	sort.SliceStable(*es.orig, func(i, j int) bool { return less(es.At(i), es.At(j)) })
}

// Range iterates over HistogramDataPointSlice and executes f against each element.
// The function also passes the iteration index.
func (es HistogramDataPointSlice) Range(f func(int, HistogramDataPoint)) {
	for i := 0; i < es.Len(); i++ {
		f(i, es.At(i))
	}
}
