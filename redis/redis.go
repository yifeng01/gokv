package redis

import (
	"time"

	"github.com/go-redis/redis"

	"github.com/yifeng01/gokv/encoding"
	"github.com/yifeng01/gokv/util"
)

// Store is a io.Store implementation for Redis.
type Store struct {
	c     *redis.Client
	codec encoding.Codec
	keyFn KeyFunc
}

// Set stores the given value for the given key.
// Values are automatically marshalled to JSON or gob (depending on the configuration).
// The key must not be "" and the value must not be nil.
func (c *Store) Set(k string, v interface{}) error {
	return c.SetEx(k, v, 0)
}

// SetEx store the give value for the given key and the key expire after expires.
func (c *Store) SetEx(k string, v interface{}, expires time.Duration) error {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return err
	}

	// First turn the passed object into something that Redis can handle
	// (the Set method takes an interface{}, but the Get method only returns a string,
	// so it can be assumed that the interface{} parameter type is only for convenience
	// for a couple of builtin types like int etc.).
	data, err := c.codec.Marshal(v)
	if err != nil {
		return err
	}

	err = c.c.Set(c.keyFn(k), string(data), expires).Err()
	if err != nil {
		return err
	}
	return nil
}

// Get retrieves the stored value for the given key.
// You need to pass a pointer to the value, so in case of a struct
// the automatic unmarshalling can populate the fields of the object
// that v points to with the values of the retrieved object's values.
// If no value is found it returns (false, nil).
// The key must not be "" and the pointer must not be nil.
func (c *Store) Get(k string, v interface{}) (found bool, err error) {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return false, err
	}

	dataString, err := c.c.Get(c.keyFn(k)).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}

	return true, c.codec.Unmarshal([]byte(dataString), v)
}

// Has judge store has a key for k
func (c *Store) Has(k string) bool {
	if err := util.CheckKey(k); err != nil {
		return false
	}

	_, err := c.c.Get(c.keyFn(k)).Result()
	if err != nil {
		return false
	}
	return true
}

// Delete deletes the stored value for the given key.
// Deleting a non-existing key-value pair does NOT lead to an error.
// The key must not be "".
func (c *Store) Delete(k string) error {
	if err := util.CheckKey(k); err != nil {
		return err
	}

	_, err := c.c.Del(c.keyFn(k)).Result()
	return err
}

// Close closes the client.
// It must be called to release any open resources.
func (c *Store) Close() error {
	return c.c.Close()
}

// Options are the options for the Redis client.
type Options struct {
	// Address of the Redis server, including the port.
	// Optional ("localhost:6379" by default).
	Address string
	// Password for the Redis server.
	// Optional ("" by default).
	Password string
	// DB to use.
	// Optional (0 by default).
	DB int
	// Encoding format.
	// Optional (encoding.JSON by default).
	Codec encoding.Codec
	// key fn
	KeyFn KeyFunc
}

// DefaultOptions is an Options object with default values.
// Address: "localhost:6379", Password: "", DB: 0, Codec: encoding.JSON
var DefaultOptions = Options{
	Address: "localhost:6379",
	Codec:   encoding.JSON,
	// No need to set Password or DB because their Go zero values are fine for that.
}

// NewClient creates a new Redis client.
//
// You must call the Close() method on the client when you're done working with it.
func New(options Options) *Store {
	// Set default values
	if options.Address == "" {
		options.Address = DefaultOptions.Address
	}
	if options.Codec == nil {
		options.Codec = DefaultOptions.Codec
	}
	if options.KeyFn == nil {
		options.KeyFn = DefaultKeyFunc
	}

	client := redis.NewClient(&redis.Options{
		Addr:     options.Address,
		Password: options.Password,
		DB:       options.DB,
	})

	err := client.Ping().Err()
	if err != nil {
		return nil
	}

	s := &Store{
		c:     client,
		codec: options.Codec,
		keyFn: options.KeyFn,
	}

	return s
}

// DefaultKeyFunc is the default implementation of cache keys
// All it does is to preprend "gokv:" to the key sent in by client code
func DefaultKeyFunc(s string) string {
	return "gokv:" + s
}

// KeyFunc defines a transformer for cache keys
type KeyFunc func(s string) string
