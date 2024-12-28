// A quickly mysql access component.
// Copyright 2023 The daog Authors. All rights reserved.

// Package daog, 是轻量级的数据库访问组件，它并不能称之为orm组件，仅仅提供了一组函数用以实现常用的数据库访问功能。
// 它是高性能的，与原生的使用sql包函数相比，没有性能损耗，这是因为，它并没有使用反射技术，而是使用编译技术把create table sql语句编译成daog需要的go代码。
// 它目前仅支持mysql。
//
// 设计思路来源于java的[orm框架sampleGenericDao](https://github.com/tiandarwin/simpleGenericDao)和protobuf的编译思路。之所以选择编译
// 而没有使用反射，是因为基于编译的抽象没有性能损耗。
package daog

import (
	"context"
	"database/sql"
	"github.com/soulgm/daog/utils"
	"log"
	"strings"
	"time"
)

// DbConf 数据源配置, 包括数据库url和连接池相关配置，特别注意，它支持按数据源在日志中输出执行的sql
type DbConf struct {
	// 数据库url
	DbUrl string
	// 最大连接数
	Size int
	// 连接的最大生命周期，单位是秒
	Life int
	// 最大空闲连接数
	IdleCons int
	// 最大空闲时间，单位是秒
	IdleTime int
	// 该在该数据源上执行sql是是否需要把待执行的sql输出到日志
	LogSQL bool
	// 读取连接超时时间，单位是秒
	GetConnTimeout int64
}

// Datasource 描述一个数据源，确切的说是一个数据源分片，它对应一个mysql database
type Datasource interface {
	getDB(ctx context.Context) *sql.DB
	// Shutdown 关闭数据源
	Shutdown()
	// IsLogSQL 本数据源是否需要输出执行的sql到日志
	IsLogSQL() bool
	acquireConnTimeout() time.Duration
}

func NewDatasource(conf *DbConf) (Datasource, error) {
	dbUrl := conf.DbUrl
	if strings.Index(conf.DbUrl, "interpolateParams") == -1 {
		if strings.Index(conf.DbUrl, "?") != -1 {
			dbUrl = dbUrl + "&interpolateParams=true"
		} else {
			dbUrl = dbUrl + "?interpolateParams=true"
		}
	}
	db, err := sql.Open("mysql", dbUrl)
	if err != nil {
		log.Printf("goid=%d, %v\n", utils.QuickGetGoroutineId(), err)
		return nil, err
	}
	if conf.Size != 0 {
		db.SetMaxOpenConns(conf.Size)
	}
	if conf.IdleCons > 0 {
		db.SetMaxIdleConns(conf.IdleCons)
	}
	if conf.IdleTime > 0 {
		db.SetConnMaxIdleTime(time.Duration(int64(conf.IdleTime) * 1e9))
	}
	if conf.Life > 0 {
		db.SetConnMaxLifetime(time.Duration(int64(conf.Life) * 1e9))
	}
	if conf.GetConnTimeout <= 0 {
		conf.GetConnTimeout = 10
	}
	return &singleDatasource{db, conf.LogSQL, time.Second * time.Duration(conf.GetConnTimeout)}, nil
}

type singleDatasource struct {
	db             *sql.DB
	logSQL         bool
	getConnTimeout time.Duration
}

func (db *singleDatasource) getDB(ctx context.Context) *sql.DB {
	return db.db
}
func (db *singleDatasource) Shutdown() {
	db.db.Close()
}

func (db *singleDatasource) IsLogSQL() bool {
	return db.logSQL
}
func (db *singleDatasource) acquireConnTimeout() time.Duration {
	return db.getConnTimeout
}
