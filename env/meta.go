package env

import (
	"github.com/995933447/confloader"
	"github.com/gzjjyz/srvlib/utils"
	"sync"
	"time"
)

const (
	Dev  = "dev"
	Prod = "prod"
	Test = "test"
)

const (
	DiscoveryFileCacheProxy = "proxy"
	DiscoveryEtcd           = "etcd"
)

type Etcd struct {
	ConnectTimeoutMs int32    `json:"connect_timeout_ms"`
	Endpoints        []string `json:"endpoints"`
}

type DiscoveryProxy struct {
	Dir  string `json:"dir"`
	Conn string `json:"connection"`
}

type MysqlConn struct {
	Host      string `json:"host"`
	Port      int    `json:"port"`
	User      string `json:"user"`
	Password  string `json:"password"`
	Databases string `json:"databases"`
}

type DBConnections struct {
	Mysql map[string]MysqlConn
}

func (c *DBConnections) GetMysqlConn(connName string) (MysqlConn, bool) {
	conn, ok := c.Mysql[connName]
	return conn, ok
}

type Meta struct {
	Env            string `json:"env"`
	Discovery      string `json:"discovery"`
	Etcd           `json:"etcd"`
	DiscoveryProxy `json:"discovery_proxy"`
	DBConnections  `json:"db"`
}

func (m *Meta) IsDev() bool {
	return m.Env == Dev
}

func (m *Meta) IsTest() bool {
	return m.Env == Test
}

func (m *Meta) IsProd() bool {
	return m.Env == Prod
}

var (
	meta        *Meta
	hasInitMeta bool
	initMetaMu  sync.RWMutex
)

func InitMeta(cfgFilePath string) error {
	if cfgFilePath == "" {
		cfgFilePath = defaultCfgFilePath
	}

	if hasInitMeta {
		return nil
	}

	initMetaMu.Lock()
	defer initMetaMu.Unlock()

	if hasInitMeta {
		return nil
	}

	meta = &Meta{
		DBConnections: DBConnections{
			Mysql: map[string]MysqlConn{},
		},
	}
	cfgLoader := confloader.NewLoader(cfgFilePath, 5*time.Second, meta)
	if err := cfgLoader.Load(); err != nil {
		return err
	}

	hasInitMeta = true

	watchMetaErrCh := make(chan error)
	utils.ProtectGo(func() {
		cfgLoader.WatchToLoad(watchMetaErrCh)
	})
	utils.ProtectGo(func() {
		for {
			err := <-watchMetaErrCh
			if err != nil {
				utils.SafeLogErr(err, true)
			}
		}
	})

	return nil
}

func MustMeta() *Meta {
	if !hasInitMeta {
		panic("meta not init")
	}

	return meta
}
