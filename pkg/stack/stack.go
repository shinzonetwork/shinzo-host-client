// Package stack provides a generic stack implementation.
// A stack is a Last-In-First-Out (LIFO) data structure.
package stack

import "errors"

// ErrEmptyStack is returned when trying to pop or peek from an empty stack.
var ErrEmptyStack = errors.New("stack: cannot perform operation on empty stack")

// Stack represents a generic stack data structure.
type Stack[T any] struct {
	items []T
}

// New creates and returns a new empty stack.
func New[T any]() *Stack[T] {
	return &Stack[T]{
		items: make([]T, 0),
	}
}

// Push adds an element to the top of the stack.
func (s *Stack[T]) Push(item T) {
	s.items = append(s.items, item)
}

// Pop removes and returns the top element from the stack.
// Returns an error if the stack is empty.
func (s *Stack[T]) Pop() (T, error) {
	var defaultT T
	if s.IsEmpty() {
		return defaultT, ErrEmptyStack
	}

	index := len(s.items) - 1
	item := s.items[index]
	s.items = s.items[:index]
	return item, nil
}

// Peek returns the top element without removing it from the stack.
// Returns an error if the stack is empty.
func (s *Stack[T]) Peek() (T, error) {
	var defaultT T
	if s.IsEmpty() {
		return defaultT, ErrEmptyStack
	}

	return s.items[len(s.items)-1], nil
}

// IsEmpty returns true if the stack has no elements.
func (s *Stack[T]) IsEmpty() bool {
	return len(s.items) == 0
}

// Size returns the number of elements in the stack.
func (s *Stack[T]) Size() int {
	return len(s.items)
}

// Clear removes all elements from the stack.
func (s *Stack[T]) Clear() {
	s.items = s.items[:0]
}

// ToSlice returns a copy of the stack's elements as a slice.
// The slice is ordered from bottom to top of the stack.
func (s *Stack[T]) ToSlice() []T {
	result := make([]T, len(s.items))
	copy(result, s.items)
	return result
}
