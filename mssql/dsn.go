package mssql

import (
	"bytes"
	"log"
	"net/url"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/go-xorm/xorm"
	"xorm.io/core"
)

const (
	_defMSSqlDriverName = "mssql"
)

type SqlSvr struct {
	engine *xorm.Engine
	table  string
	split  bool
}

func (s *SqlSvr) Close() {
}

func newSqlSvr(user, pwd, host, db, tb string, split bool) *SqlSvr {
	dsn := getMssqlDsn(user, pwd, host, db)
	engine, err := xorm.NewEngine(_defMSSqlDriverName, dsn)
	if err != nil {
		log.Printf("newSqlSvr: err=%s,host=%s,db=%s,user=%s\n", err, host, db, user)
		return nil
	}

	//设置参数
	engine.ShowSQL(true)
	engine.ShowExecTime(true)
	engine.SetMaxIdleConns(10)
	engine.SetMaxOpenConns(20)
	engine.SetMapper(core.GonicMapper{})
	engine.Sync()

	return &SqlSvr{
		engine: engine,
		table:  tb,
		split:  split,
	}
}

//////////////////////////////////////////////////////
//组数据源名字
func getMssqlDsn(user, pwd, host, database string) string {
	var buffer bytes.Buffer
	buffer.WriteString("sqlserver://")
	buffer.WriteString(user)
	buffer.WriteString(":")
	buffer.WriteString(url.QueryEscape(pwd))
	buffer.WriteString("@")
	buffer.WriteString(host)
	buffer.WriteString("?database=")
	buffer.WriteString(database)
	buffer.WriteString("&encrypt=disable")
	return buffer.String()
}
