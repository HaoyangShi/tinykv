package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandAloneReader struct {
	txn *badger.Txn
}

func NewStandAloneReader(txn *badger.Txn) *StandAloneReader{
	return &StandAloneReader{
		txn: txn,
	}
}

func (r *StandAloneReader) GetCF(cf string, key []byte)([]byte, error){
//	在Raft的Region_Reader中检查是否在此节点的保存区域内有此键值的内容
//	但是StandAlone只有单一节点，包含着所有的内容，所以不用检查
//	（当然也因为StandAlone场景下不涉及Region相关的任何内容，所以也不用对Region进行检查）
	val, err := engine_util.GetCFFromTxn(r.txn,cf,key)
	if err == badger.ErrKeyNotFound{
		return nil,nil
	}
	return val,err
}

