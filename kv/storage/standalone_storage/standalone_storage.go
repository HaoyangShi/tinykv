package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.

/*
project1 主要就是参考 /kv/storage/raft_storage/raft_server下的实现，
类似的实现standalone下各个函数的定义
 */

// 参考RaftStorage的定义，其中与Raft无关的为engines和config，
// 分别是用于记录KVDB的元信息和当前节点配置
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	config *config.Config
}

// 参考NewRaftStorage定义，本质上就是按照config中节点信息，创建相应的DB
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	log.Debugf("create a standAloneStorage, DBPath: %s", conf.DBPath)
	dbPath := conf.DBPath

	standalone_kvPath := filepath.Join(dbPath,"kv")
	os.Mkdir(standalone_kvPath,os.ModePerm)


	kvDB := engine_util.CreateDB(standalone_kvPath,false)
	engines := engine_util.NewEngines(kvDB,nil,standalone_kvPath,"")

	return &StandAloneStorage{engines: engines, config: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	log.Debugf("standalone start")
	return nil
}

// 关闭Engines中的对应DB，
//在standalone情况下只有KVDB，所以仿照raft_server中关闭KV即可
func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.engines.Kv.Close(); err != nil {
		return err
	}
	return nil
}

// 读和写对于standalone来说不需要询问其他节点，可以直接进行操作
// Reader直接创建一个类似RegionReader
// Write 直接将要进行的写入操作加入到操作队列中，之后会逐渐完成

// 在TinyKV实现的Raft中进行了分区，所以它有一个Region
// 但是对于StandAlone场景，并不需要考虑分区，
// 所以仿照RegionReader构造一个用Badger的Txn读取的对象，用于创建StorageReader
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	// 用Badger.Txn 进行读，
	//在Raft_server中是由callback中的事务担任
	// 所以在standalone中，需要另外创建一个事务用于读
	txn :=s.engines.Kv.NewTransaction(false)
	return NewStandAloneReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	StandAloneWB := new(engine_util.WriteBatch)
	for _,m :=range batch{
		switch m.Data.(type){
		case storage.Put:
			put := m.Data.(storage.Put)
			StandAloneWB.SetCF(put.Cf,put.Key,put.Value)
		case storage.Delete:
			delete:=m.Data.(storage.Delete)
			StandAloneWB.DeleteCF(delete.Cf,delete.Key)
		}
	}
	return StandAloneWB.WriteToDB(s.engines.Kv)
}
