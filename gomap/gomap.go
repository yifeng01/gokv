package gomap

import (
	"sync"
	"time"

	"github.com/yifeng01/gokv/encoding"
	"github.com/yifeng01/gokv/util"
)

// Store is a gokv.Store implementation for a Go map with a sync.RWMutex for concurrent access.
type Store struct {
	m     map[string]*Item
	lock  *sync.RWMutex
	codec encoding.Codec
}

// Set stores the given value for the given key.
// Values are automatically marshalled to JSON or gob (depending on the configuration).
// The key must not be "" and the value must not be nil.
func (s *Store) Set(k string, v interface{}) error {
	return s.SetEx(k, v, 0)
}

// SetEx store the give value for the given key and the key expire after expires.
func (s *Store) SetEx(k string, v interface{}, expires time.Duration) error {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return err
	}

	data, err := s.codec.Marshal(v)
	if err != nil {
		return err
	}

	var item *Item
	if expires == 0 {
		item = &Item{
			Data: util.CopyData(data),
		}
	} else {
		item = &Item{
			ExpiresAt: time.Now().Add(expires),
			Data:      util.CopyData(data),
		}
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	s.m[k] = item
	return nil
}

// Get retrieves the stored value for the given key.
// You need to pass a pointer to the value, so in case of a struct
// the automatic unmarshalling can populate the fields of the object
// that v points to with the values of the retrieved object's values.
// If no value is found it returns (false, nil).
// The key must not be "" and the pointer must not be nil.
func (s *Store) Get(k string, v interface{}) (found bool, err error) {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return false, err
	}

	s.lock.RLock()
	data, found := s.m[k]
	// Unlock right after reading instead of with defer(),
	// because following unmarshalling will take some time
	// and we don't want to block writing threads until that's done.
	s.lock.RUnlock()
	if !found {
		return false, nil
	}

	return true, s.codec.Unmarshal(data.Data, v)
}

// Has judge store has a key for k
func (s *Store) Has(k string) bool {
	if err := util.CheckKey(k); err != nil {
		return false
	}

	s.lock.RLock()
	_, found := s.m[k]
	s.lock.RUnlock()

	return found
}

// Delete deletes the stored value for the given key.
// Deleting a non-existing key-value pair does NOT lead to an error.
// The key must not be "".
func (s *Store) Delete(k string) error {
	if err := util.CheckKey(k); err != nil {
		return err
	}

	delete(s.m, k)
	return nil
}

// Close closes the store.
// When called, the store's pointer to the internal Go map is set to nil,
// leading to the map being free for garbage collection.
func (s *Store) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.m = nil
	return nil
}

// GC recycle expire items
func (s *Store) GC() {
	s.lock.Lock()
	defer s.lock.Unlock()

	for k, v := range s.m {
		if v.IsExpired() {
			delete(s.m, k)
		}
	}
}

// auto GC
func (s *Store) autoGC(interval time.Duration) {
	if interval == 0 {
		interval = 30 * time.Second
	}

	tk := time.NewTicker(interval)
	defer tk.Stop()

	for {
		<-tk.C
		s.GC()
	}
}

// Options are the options for the Go map store.
type Options struct {
	// Encoding format.
	// Optional (encoding.JSON by default).
	Codec    encoding.Codec
	Interval time.Duration
}

// DefaultOptions is an Options object with default values.
// Codec: encoding.JSON
var DefaultOptions = Options{
	Codec:    encoding.JSON,
	Interval: time.Second * 30,
}

// NewStore creates a new Go map store.
//
// You should call the Close() method on the store when you're done working with it.
func New(options Options) *Store {
	// Set default options
	if options.Codec == nil {
		options.Codec = DefaultOptions.Codec
	}

	s := Store{
		m:     make(map[string]*Item),
		lock:  new(sync.RWMutex),
		codec: options.Codec,
	}

	go s.autoGC(options.Interval)

	return &s
}

//Item identifes a cached piece of data
type Item struct {
	ExpiresAt time.Time
	Data      []byte
}

//Helper method to check if an item is expired.
//Current usecase for this is for garbage collection
func (i *Item) IsExpired() bool {
	//zero means never expire
	if i.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(i.ExpiresAt)
}
