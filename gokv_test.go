package gokv

import (
	"testing"
	"time"

	"github.com/yifeng01/gokv/file"
	"github.com/yifeng01/gokv/gomap"
	"github.com/yifeng01/gokv/mssql"
	"github.com/yifeng01/gokv/redis"
	"github.com/yifeng01/gokv/syncmap"
)

// Notice: when test you should change you own redis and mssql addr.
const (
	_defUserId       = "22231028"
	_defRedisAddress = "127.0.0.1:10000"
	_defRedisPwd     = "123456"
	_defMssqlAddr    = "127.0.0.1:1433"
	_defMssqlUser    = "sa"
	_defMssqlPwd     = "123456"
	_defMssqlDb      = "LogFlow"
	_defMssqlTb      = "t_gokv_test"
)

func TestGokv_syncMap(t *testing.T) {
	store := syncmap.New(syncmap.DefaultOptions)
	err := store.SetEx(_defUserId, 1, 5*time.Second)
	if err != nil {
		t.Errorf("SetEx: err=%v", err)
	}

	var val int
	if found, err := store.Get(_defUserId, &val); err != nil || !found || val != 1 {
		t.Errorf("Get: err=%v, found=%v, val=%d", err, found, val)
	}

	time.Sleep(31 * time.Second)

	if found, err := store.Get(_defUserId, &val); err != nil || found {
		t.Errorf("Get: after expire, err=%v, found=%v", err, found)
	}
}

func TestGokv_map(t *testing.T) {
	store := gomap.New(gomap.DefaultOptions)
	err := store.SetEx(_defUserId, 1, 5*time.Second)
	if err != nil {
		t.Errorf("SetEx: err=%v", err)
	}

	var val int
	if found, err := store.Get(_defUserId, &val); err != nil || !found || val != 1 {
		t.Errorf("Get: err=%v, found=%v, val=%d", err, found, val)
	}

	time.Sleep(31 * time.Second)

	if found, err := store.Get(_defUserId, &val); err != nil || found {
		t.Errorf("Get: after expire, err=%v, found=%v", err, found)
	}
}

func TestGokv_redis(t *testing.T) {
	store := redis.New(redis.Options{
		Address:  _defRedisAddress,
		Password: _defRedisPwd})
	if store == nil {
		t.Errorf("New: connect redis failed...")
	}

	err := store.SetEx(_defUserId, 1, 25*time.Second)
	if err != nil {
		t.Errorf("SetEx: err=%v", err)
	}

	var val int
	if found, err := store.Get(_defUserId, &val); err != nil || !found || val != 1 {
		t.Errorf("Get: err=%v, found=%v, val=%d", err, found, val)
	}

	time.Sleep(31 * time.Second)

	if found, err := store.Get(_defUserId, &val); err != nil || found {
		t.Errorf("Get: after expire, err=%v, found=%v", err, found)
	}
}

func TestGokv_mssql(t *testing.T) {
	store := mssql.New(
		mssql.Options{
			User:      _defMssqlUser,
			Pwd:       _defMssqlPwd,
			Host:      _defMssqlAddr,
			Db:        _defMssqlDb,
			TableName: _defMssqlTb,
		})
	if store == nil {
		t.Errorf("new: connect mssql failed...")
	}

	err := store.SetEx(_defUserId, 1, 25*time.Second)
	if err != nil {
		t.Errorf("SetEx: err=%v", err)
	}

	var val int
	if found, err := store.Get(_defUserId, &val); err != nil || !found || val != 1 {
		t.Errorf("Get: err=%v, found=%v, val=%d", err, found, val)
	}

	err = store.SetEx(_defUserId, 2, 0*time.Second)
	if err != nil {
		t.Errorf("SetEx: err=%v", err)
	}
}

func TestGokv_file(t *testing.T) {
	store := file.New(
		file.Options{
			Directory: "kvs",
		},
	)

	err := store.SetEx(_defUserId, 1, 25*time.Second)
	if err != nil {
		t.Errorf("SetEx: err=%v", err)
	}

	var val int
	if found, err := store.Get(_defUserId, &val); err != nil || !found || val != 1 {
		t.Errorf("Get: err=%v, found=%v, val=%d", err, found, val)
	}

	time.Sleep(31 * time.Second)

	if found, err := store.Get(_defUserId, &val); err != nil || found {
		t.Errorf("Get: after expire, err=%v, found=%v", err, found)
	}
}
