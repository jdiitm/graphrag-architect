package dedup

import (
	"fmt"
	"sync"
	"testing"
)

func TestLRUStore_MarkAndIsDuplicate(t *testing.T) {
	store := NewLRUStore(100)
	if store.IsDuplicate("msg-1") {
		t.Fatal("expected msg-1 to not be a duplicate before marking")
	}
	store.Mark("msg-1")
	if !store.IsDuplicate("msg-1") {
		t.Fatal("expected msg-1 to be a duplicate after marking")
	}
}

func TestLRUStore_EmptyKeyNeverDuplicate(t *testing.T) {
	store := NewLRUStore(100)
	store.Mark("")
	if store.IsDuplicate("") {
		t.Fatal("empty key should never be considered a duplicate")
	}
}

func TestLRUStore_EvictsOldest(t *testing.T) {
	store := NewLRUStore(3)
	store.Mark("a")
	store.Mark("b")
	store.Mark("c")
	store.Mark("d")

	if store.IsDuplicate("a") {
		t.Fatal("expected 'a' to be evicted after capacity exceeded")
	}
	if !store.IsDuplicate("b") {
		t.Fatal("expected 'b' to still be present")
	}
	if !store.IsDuplicate("d") {
		t.Fatal("expected 'd' to still be present")
	}
}

func TestLRUStore_DuplicateMarkNoDoubleCount(t *testing.T) {
	store := NewLRUStore(3)
	store.Mark("x")
	store.Mark("x")
	store.Mark("y")
	store.Mark("z")

	if !store.IsDuplicate("x") {
		t.Fatal("expected 'x' to still be present after re-mark")
	}
}

func TestLRUStore_ConcurrentAccess(t *testing.T) {
	store := NewLRUStore(1000)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("msg-%d", id)
			store.Mark(key)
			store.IsDuplicate(key)
		}(i)
	}
	wg.Wait()
}

func TestNoopStore_NeverDuplicate(t *testing.T) {
	store := NoopStore{}
	store.Mark("msg-1")
	if store.IsDuplicate("msg-1") {
		t.Fatal("NoopStore should never report duplicates")
	}
}
