# gokv
kv store for memory, redis, db and file

# install
go get github.com/yifeng01/gokv

# usage
```
package main

import (
	"log"
	"sync"
	"time"

	"github.com/yifeng01/gokv/file"
	"github.com/yifeng01/gokv/gomap"
	"github.com/yifeng01/gokv/mssql"
	"github.com/yifeng01/gokv/redis"
	"github.com/yifeng01/gokv/syncmap"
)

//Notice: when test you should change you own redis and mssql address.
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

func main() {
	wg := sync.WaitGroup{}

	wg.Add(5)

	go kvSyncmap(&wg)
	go kvGomap(&wg)
	go kvRedis(&wg)
	go kvMssql(&wg)
	go kvFile(&wg)

	wg.Wait()
}

func kvSyncmap(wg *sync.WaitGroup) {
	store := syncmap.New(syncmap.DefaultOptions)
	err := store.SetEx(_defUserId, 1, 5*time.Second)
	if err != nil {
		log.Printf("SetEx: err=%v\n", err)
	}

	var val int
	if found, err := store.Get(_defUserId, &val); err != nil || !found || val != 1 {
		log.Printf("Get: err=%v, found=%v, val=%d\n", err, found, val)
	}

	time.Sleep(31 * time.Second)

	if found, err := store.Get(_defUserId, &val); err != nil || found {
		log.Printf("Get: after expire, err=%v, found=%v\n", err, found)
	}

	wg.Done()
}

func kvGomap(wg *sync.WaitGroup) {
	store := gomap.New(gomap.DefaultOptions)
	err := store.SetEx(_defUserId, 1, 5*time.Second)
	if err != nil {
		log.Printf("SetEx: err=%v\n", err)
	}

	var val int
	if found, err := store.Get(_defUserId, &val); err != nil || !found || val != 1 {
		log.Printf("Get: err=%v, found=%v, val=%d\n", err, found, val)
	}

	time.Sleep(31 * time.Second)

	if found, err := store.Get(_defUserId, &val); err != nil || found {
		log.Printf("Get: after expire, err=%v, found=%v\n", err, found)
	}

	wg.Done()
}

func kvRedis(wg *sync.WaitGroup) {
	store := redis.New(redis.Options{
		Address:  _defRedisAddress,
		Password: _defRedisPwd,
	})
	err := store.SetEx(_defUserId, 1, 5*time.Second)
	if err != nil {
		log.Printf("SetEx: err=%v\n", err)
	}

	var val int
	if found, err := store.Get(_defUserId, &val); err != nil || !found || val != 1 {
		log.Printf("Get: err=%v, found=%v, val=%d\n", err, found, val)
	}

	time.Sleep(31 * time.Second)

	if found, err := store.Get(_defUserId, &val); err != nil || found {
		log.Printf("Get: after expire, err=%v, found=%v\n", err, found)
	}

	wg.Done()
}

func kvMssql(wg *sync.WaitGroup) {
	store := mssql.New(mssql.Options{
		User:      _defMssqlUser,
		Pwd:       _defMssqlPwd,
		Host:      _defMssqlAddr,
		Db:        _defMssqlDb,
		TableName: _defMssqlTb,
	})
	err := store.SetEx(_defUserId, 1, 5*time.Second)
	if err != nil {
		log.Printf("SetEx: err=%v\n", err)
	}

	var val int
	if found, err := store.Get(_defUserId, &val); err != nil || !found || val != 1 {
		log.Printf("Get: err=%v, found=%v, val=%d\n", err, found, val)
	}


```
