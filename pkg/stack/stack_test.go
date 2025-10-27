package stack

import (
	"testing"
)

func TestNew(t *testing.T) {
	s := New[int]()
	if !s.IsEmpty() {
		t.Error("New stack should be empty")
	}
	if s.Size() != 0 {
		t.Error("New stack should have size 0")
	}
}

func TestPush(t *testing.T) {
	s := New[int]()

	s.Push(1)
	if s.IsEmpty() {
		t.Error("Stack should not be empty after push")
	}
	if s.Size() != 1 {
		t.Error("Stack size should be 1 after pushing one element")
	}

	s.Push(2)
	if s.Size() != 2 {
		t.Error("Stack size should be 2 after pushing two elements")
	}
}

func TestPop(t *testing.T) {
	s := New[int]()

	s.Push(1)
	s.Push(2)
	s.Push(3)

	// Test pop order (LIFO)
	val, err := s.Pop()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val != 3 {
		t.Errorf("Expected 3, got %d", val)
	}

	val, err = s.Pop()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val != 2 {
		t.Errorf("Expected 2, got %d", val)
	}

	val, err = s.Pop()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val != 1 {
		t.Errorf("Expected 1, got %d", val)
	}

	if !s.IsEmpty() {
		t.Error("Stack should be empty after popping all elements")
	}
}

func TestPopEmpty(t *testing.T) {
	s := New[int]()

	_, err := s.Pop()
	if err == nil {
		t.Error("Pop on empty stack should return error")
	}
	if err != ErrEmptyStack {
		t.Errorf("Expected ErrEmptyStack, got %v", err)
	}
}

func TestPeek(t *testing.T) {
	s := New[int]()

	s.Push(1)
	val, err := s.Peek()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val != 1 {
		t.Errorf("Expected 1, got %d", val)
	}

	s.Push(2)
	val, err = s.Peek()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val != 2 {
		t.Errorf("Expected 2, got %d", val)
	}

	// Ensure peek doesn't remove elements
	if s.Size() != 2 {
		t.Error("Stack size should remain 2 after peek")
	}
}

func TestPeekEmpty(t *testing.T) {
	s := New[int]()

	_, err := s.Peek()
	if err == nil {
		t.Error("Peek on empty stack should return error")
	}
	if err != ErrEmptyStack {
		t.Errorf("Expected ErrEmptyStack, got %v", err)
	}
}

func TestSize(t *testing.T) {
	s := New[string]()

	if s.Size() != 0 {
		t.Error("Empty stack should have size 0")
	}

	s.Push("a")
	s.Push("b")
	if s.Size() != 2 {
		t.Error("Stack should have size 2 after pushing two elements")
	}

	_, err := s.Pop()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if s.Size() != 1 {
		t.Error("Stack should have size 1 after popping one element")
	}
}

func TestClear(t *testing.T) {
	s := New[int]()

	s.Push(1)
	s.Push(2)
	s.Push(3)

	s.Clear()

	if !s.IsEmpty() {
		t.Error("Stack should be empty after clear")
	}
	if s.Size() != 0 {
		t.Error("Stack size should be 0 after clear")
	}
}

func TestToSlice(t *testing.T) {
	s := New[int]()

	s.Push(1)
	s.Push(2)
	s.Push(3)

	slice := s.ToSlice()
	expected := []int{1, 2, 3}

	if len(slice) != len(expected) {
		t.Errorf("Expected slice length %d, got %d", len(expected), len(slice))
	}

	for i, val := range slice {
		if val != expected[i] {
			t.Errorf("Expected %d at index %d, got %d", expected[i], i, val)
		}
	}

	// Ensure modifying the returned slice doesn't affect the stack
	slice[0] = 999
	val, err := s.Peek()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val == 999 {
		t.Error("Modifying returned slice should not affect the stack")
	}
}

func TestGenericTypes(t *testing.T) {
	// Test with different types

	// String stack
	s1 := New[string]()
	s1.Push("hello")
	s1.Push("world")
	val, err := s1.Pop()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val != "world" {
		t.Error("String stack pop failed")
	}

	// Float stack
	s2 := New[float64]()
	s2.Push(3.14)
	s2.Push(2.71)
	val2, err := s2.Pop()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if val2 != 2.71 {
		t.Error("Float stack pop failed")
	}

	// Struct stack
	type Person struct {
		Name string
		Age  int
	}

	s3 := New[Person]()
	s3.Push(Person{"Alice", 30})
	s3.Push(Person{"Bob", 25})

	person, err := s3.Pop()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if person.Name != "Bob" || person.Age != 25 {
		t.Error("Struct stack pop failed")
	}
}

func TestStackBehavior(t *testing.T) {
	// Test typical stack usage pattern
	s := New[int]()

	// Push some elements
	elements := []int{1, 2, 3, 4, 5}
	for _, elem := range elements {
		s.Push(elem)
	}

	// Pop all elements and verify LIFO order
	for i := len(elements) - 1; i >= 0; i-- {
		val, err := s.Pop()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if val != elements[i] {
			t.Errorf("Expected %d, got %d", elements[i], val)
		}
	}

	if !s.IsEmpty() {
		t.Error("Stack should be empty after popping all elements")
	}
}
