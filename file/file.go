package file

import (
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yifeng01/gokv/encoding"
	"github.com/yifeng01/gokv/util"
)

var defaultFilenameExtension = "json"

// Store is a gokv.Store implementation for storing key-value pairs as files.
type Store struct {
	// For locking the locks map
	// (no two goroutines may create a lock for a filename that doesn't have a lock yet).
	locksLock *sync.Mutex
	// For locking file access.
	fileLocks         map[string]*sync.RWMutex
	filenameExtension string
	directory         string
	codec             encoding.Codec
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

	var item *Item
	if expires == 0 {
		item = &Item{
			Data: v,
		}
	} else {
		item = &Item{
			ExpiresAt: time.Now().Add(expires),
			Data:      v,
		}
	}

	data, err := s.codec.Marshal(item)
	if err != nil {
		return err
	}

	escapedKey := url.PathEscape(k)

	// Prepare file lock.
	lock := s.prepFileLock(escapedKey)

	filename := escapedKey
	if s.filenameExtension != "" {
		filename += "." + s.filenameExtension
	}
	filePath := filepath.Clean(s.directory + "/" + filename)

	// File lock and file handling.
	lock.Lock()
	defer lock.Unlock()
	return ioutil.WriteFile(filePath, data, 0600)
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

	escapedKey := url.PathEscape(k)

	// Prepare file lock.
	lock := s.prepFileLock(escapedKey)

	filename := escapedKey
	if s.filenameExtension != "" {
		filename += "." + s.filenameExtension
	}
	filePath := filepath.Clean(s.directory + "/" + filename)

	// File lock and file handling.
	lock.RLock()
	// Deferring the unlocking would lead to the unmarshalling being done during the lock, which is bad for performance.
	data, err := ioutil.ReadFile(filePath)
	lock.RUnlock()
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	item := &Item{Data: v}
	if err := s.codec.Unmarshal(data, item); err != nil {
		return false, err
	}

	return true, nil
}

// Has judge store has a key for k
func (s *Store) Has(k string) bool {
	if err := util.CheckKey(k); err != nil {
		return false
	}

	escapedKey := url.PathEscape(k)

	// Prepare file lock.
	lock := s.prepFileLock(escapedKey)

	filename := escapedKey
	if s.filenameExtension != "" {
		filename += "." + s.filenameExtension
	}
	filePath := filepath.Clean(s.directory + "/" + filename)

	// File lock and file handling.
	lock.RLock()
	// Deferring the unlocking would lead to the unmarshalling being done during the lock, which is bad for performance.
	_, err := ioutil.ReadFile(filePath)
	lock.RUnlock()
	if err != nil {
		return false
	}

	return true
}

// Delete deletes the stored value for the given key.
// Deleting a non-existing key-value pair does NOT lead to an error.
// The key must not be "".
func (s *Store) Delete(k string) error {
	if err := util.CheckKey(k); err != nil {
		return err
	}

	escapedKey := url.PathEscape(k)

	// Prepare file lock.
	lock := s.prepFileLock(escapedKey)

	filename := escapedKey
	if s.filenameExtension != "" {
		filename += "." + s.filenameExtension
	}
	filePath := filepath.Clean(s.directory + "/" + filename)

	// File lock and file handling.
	lock.Lock()
	defer lock.Unlock()
	err := os.Remove(filePath)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// Close closes the store.
// When called, some resources of the store are left for garbage collection.
func (s *Store) Close() error {
	s.fileLocks = nil
	return nil
}

// GC recycle expire items
func (s *Store) GC() {
	log.Println("gc begin....")
	filepath.Walk(
		s.directory,
		func(path string, finfo os.FileInfo, err error) error {
			lock := s.prepFileLock(path)
			lock.Lock()
			defer lock.Unlock()

			log.Printf("gc: check path=%s\n", path)

			if err != nil {
				return err
			}

			if finfo.IsDir() {
				return nil
			}

			data, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}

			item := &Item{}
			err = s.codec.Unmarshal(data, item)
			if err != nil {
				return err
			}
			if item.IsExpired() {
				return os.Remove(path)
			}

			return nil
		})
	log.Println("gc end...")
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

// prepFileLock returns an existing file lock or creates a new one
func (s *Store) prepFileLock(escapedKey string) *sync.RWMutex {
	s.locksLock.Lock()
	lock, found := s.fileLocks[escapedKey]
	if !found {
		lock = new(sync.RWMutex)
		s.fileLocks[escapedKey] = lock
	}
	s.locksLock.Unlock()
	return lock
}

// Options are the options for the Go file store.
type Options struct {
	// The directory in which to store files.
	// Can be absolute or relative.
	// Optional ("gokv" by default).
	Directory string
	// Extension of the filename, e.g. "json".
	// This makes it easier for example to open a file with a text editor that supports syntax highlighting.
	// But it can lead to redundant and/or stale data if you switch the Codec back and forth!
	// You also should make sure to change this when changing the Codec,
	// although it doesn't matter for gokv, but it might be confusing when there's a gob file with a ".json" filename extension.
	// Set to "" to disable.
	// Optional ("json" by default).
	FilenameExtension *string
	// Encoding format.
	// Note: When you change this, you should also change the FilenameExtension if it's not empty ("").
	// Optional (encoding.JSON by default).
	Codec encoding.Codec

	Interval time.Duration
}

// DefaultOptions is an Options object with default values.
// Directory: "gokv", Codec: encoding.JSON
var DefaultOptions = Options{
	Directory:         "kvs",
	FilenameExtension: &defaultFilenameExtension,
	Codec:             encoding.JSON,
	Interval:          30 * time.Second,
}

// New creates a new Go file store.
//
// You should call the Close() method on the store when you're done working with it.
func New(options Options) *Store {
	// Set default options
	if options.Directory == "" {
		options.Directory = DefaultOptions.Directory
	}
	if options.FilenameExtension == nil {
		options.FilenameExtension = DefaultOptions.FilenameExtension
	}
	if options.Codec == nil {
		options.Codec = DefaultOptions.Codec
	}

	err := os.MkdirAll(options.Directory, 0700)
	if err != nil {
		return nil
	}

	result := Store{
		directory:         options.Directory,
		locksLock:         new(sync.Mutex),
		fileLocks:         make(map[string]*sync.RWMutex),
		filenameExtension: *options.FilenameExtension,
		codec:             options.Codec,
	}

	go result.autoGC(options.Interval)

	return &result
}

//Item identifes a cached piece of data
type Item struct {
	ExpiresAt time.Time
	Data      interface{}
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
