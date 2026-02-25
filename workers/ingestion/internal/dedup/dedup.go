package dedup

import "sync"

type Store interface {
	IsDuplicate(key string) bool
	Mark(key string)
}

type LRUStore struct {
	mu       sync.Mutex
	capacity int
	keys     map[string]struct{}
	order    []string
}

func NewLRUStore(capacity int) *LRUStore {
	if capacity <= 0 {
		capacity = 10000
	}
	return &LRUStore{
		capacity: capacity,
		keys:     make(map[string]struct{}, capacity),
		order:    make([]string, 0, capacity),
	}
}

func (s *LRUStore) IsDuplicate(key string) bool {
	if key == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.keys[key]
	return exists
}

func (s *LRUStore) Mark(key string) {
	if key == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.keys[key]; exists {
		return
	}
	if len(s.order) >= s.capacity {
		evicted := s.order[0]
		s.order = s.order[1:]
		delete(s.keys, evicted)
	}
	s.keys[key] = struct{}{}
	s.order = append(s.order, key)
}

type NoopStore struct{}

func (NoopStore) IsDuplicate(string) bool { return false }
func (NoopStore) Mark(string)             {}
