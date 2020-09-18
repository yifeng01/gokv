package mssql

import (
	"log"
	"time"

	"github.com/yifeng01/gokv/encoding"
	"github.com/yifeng01/gokv/util"
)

// Store is a gokv.Store implementation for SQL databases.
type Store struct {
	Sql   *SqlSvr
	Codec encoding.Codec
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

	data, err := s.Codec.Marshal(v)
	if err != nil {
		return err
	}

	var item *Item
	if expires == 0 {
		item = &Item{
			Key:   k,
			Data:  string(data),
			Table: s.Sql.table,
		}
	} else {
		item = &Item{
			Key:       k,
			Data:      string(data),
			ExpiresAt: time.Now().Add(expires),
			Table:     s.Sql.table,
		}
	}

	return Insert(s.Sql.engine, item)
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

	item := &Item{
		Table: s.Sql.table,
	}
	found, err = s.Sql.engine.Where("id = ?", k).Get(item)
	if err != nil || !found {
		return false, err
	}

	return true, s.Codec.Unmarshal([]byte(item.Data), v)
}

func (s *Store) Has(k string) bool {
	if err := util.CheckKey(k); err != nil {
		return false
	}

	item := Item{
		Key:   k,
		Table: s.Sql.table,
	}
	if found, err := s.Sql.engine.Get(&item); err != nil || !found {
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

	item := Item{
		Key:   k,
		Table: s.Sql.table,
	}
	_, err := s.Sql.engine.Delete(&item)
	return err
}

// Close closes the Store.
// It must be called to return all open connections to the connection pool and to release any open resources.
func (s *Store) Close() error {
	s.Sql.Close()
	return nil
}

// GC recycle expire items
func (s *Store) GC() {
	log.Println("[mssql]GC begin....")

	tm := time.Now()
	log.Println("[mssql]GC tm=", tm)

	item := &Item{
		Table: s.Sql.table,
	}
	count, err := s.Sql.engine.Where("expiresAt < ?", tm).Delete(item)
	if err != nil {
		log.Println("[mssql]GC: err=", err)
		return
	}

	log.Printf("[mssql]GC end...[del=%v]\n", count)
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

// options are the options for the mssql store.
type Options struct {
	User      string
	Pwd       string
	Host      string
	Db        string
	Codec     encoding.Codec
	Interval  time.Duration
	TableName string
}

var DefaultOptions = Options{
	Codec:     encoding.JSON,
	Interval:  30 * time.Second,
	TableName: "gokv_test",
}

// New create a mssql connection.
func New(options Options) *Store {
	if options.Codec == nil {
		options.Codec = DefaultOptions.Codec
	}

	if options.Interval == 0 {
		options.Interval = DefaultOptions.Interval
	}

	if options.TableName == "" {
		options.TableName = DefaultOptions.TableName
	}

	sql := newSqlSvr(options.User, options.Pwd, options.Host, options.Db, options.TableName)
	if sql == nil {
		return nil
	}

	s := &Store{
		Sql:   sql,
		Codec: options.Codec,
	}

	//go s.autoGC(options.Interval)

	return s
}

//Item identifes a cached piece of data
type Item struct {
	Key       string    `xorm:"varchar(64) not null pk id"`
	Data      string    `xorm:"varchar(256) not null data"`
	ExpiresAt time.Time `xorm:"datetime expiresAt"`
	CTime     time.Time `xorm:"updated ctime"`
	Table     string    `xorm:"-"`
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

func (i *Item) TableName() string {
	return i.Table
}
