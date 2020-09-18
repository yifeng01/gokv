package mssql

import (
	"log"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/go-xorm/xorm"
)

func Insert(engine *xorm.Engine, data interface{}) error {
	_, err := engine.InsertOne(data)
	if err != nil {
		//log.Println("mssql: Insert, err=", err)
		return insertOrUpdate(engine, err, data)
	}
	return err
}

func insertOrUpdate(engine *xorm.Engine, err error, data interface{}) error {
	e1, ok := err.(mssql.Error)
	if !ok {
		return err
	}

	errNum := e1.SQLErrorNumber()

	if errNum != 208 && errNum != 2627 {
		log.Println("mssql: insertOrUpdate, errnum=", errNum)
		return err
	}

	//insert
	if errNum == 208 {
		if e2 := engine.CreateTables(data); e2 != nil {
			return e2
		}
		_, e3 := engine.InsertOne(data)
		return e3

	} else if errNum == 2627 {
		//update
		d, _ := data.(*Item)
		_, e3 := engine.Where("id = ?", d.Key).Cols("expiresAt", "data").Update(d)
		return e3
	}

	return nil
}
