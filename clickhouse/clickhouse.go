package clickhouse

import (
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickhouseConnector struct {
	nativeConn driver.Conn
}

type ClickhouseConfig struct {
	Host            string
	Port            int
	UserName        string
	Password        string
	DBName          string
	ConnectTimeout  time.Duration
	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifetime time.Duration
}

func NewClickhouseConnector(cfg ClickhouseConfig) (*ClickhouseConnector, error) {
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.DBName,
			Username: cfg.UserName,
			Password: cfg.Password,
		},
		DialTimeout:  cfg.ConnectTimeout,
		MaxOpenConns: cfg.MaxOpenConns,
		MaxIdleConns: cfg.MaxIdleConns,
	}

	conn, err := clickhouse.Open(options)
	if nil != err {
		log.Println("Failed to connect Clickhouse Server.", err.Error())
		return nil, err
	}

	return &ClickhouseConnector{
		nativeConn: conn,
	}, nil
}

func (conn *ClickhouseConnector) GetNativeConn() driver.Conn {
	return conn.nativeConn
}
