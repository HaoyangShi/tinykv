package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
// bug:需要将返回的response在一开始就定义好，而不可以中间再去定义
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	//response :=kvrpcpb.RawGetResponse{}

	storage_reader,err:=server.storage.Reader(req.Context)
	defer storage_reader.Close()
	if err!=nil{
		log.Errorf("RawGet: Storage Reader error")
		response:=kvrpcpb.RawGetResponse{Error: err.Error()}
		//response.Error=err.Error()
		return &response,err
	}

	value,get_err:=storage_reader.GetCF(req.GetCf(),req.GetKey())
	if get_err!=nil{
		log.Errorf("RawGet: GetCF error")
		response:=kvrpcpb.RawGetResponse{Error: err.Error()}
		//response.Error=err.Error()
		return &response,get_err
	}
	response:=kvrpcpb.RawGetResponse{}
	if value==nil{
		response.NotFound=true
	}
	response.Value=value
	return &response,get_err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	response := kvrpcpb.RawPutResponse{}

	var batch []storage.Modify
	batch = append(batch,
		storage.Modify{
		storage.Put{
			Key: req.GetKey(),
			Value: req.GetValue(),
			Cf: req.GetCf()}})

	err:=server.storage.Write(req.GetContext(),batch)
	// 只有出现错误的时候才需要返回错误
	if err!=nil{
		log.Errorf("RawPut: Write error")
		response.Error=err.Error()
	}
	return &response,err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	response := kvrpcpb.RawDeleteResponse{}
	var batch []storage.Modify
	batch = append(batch,
		storage.Modify{
			storage.Delete{
				Key: req.GetKey(),
				Cf: req.GetCf()}})

	err:=server.storage.Write(req.GetContext(),batch)
	if err!=nil{
		log.Errorf("RawDelete: Write error")
		response.Error=err.Error()
	}
	return &response,err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	response := kvrpcpb.RawScanResponse{}

	storage_reader,err:=server.storage.Reader(req.Context)
	defer storage_reader.Close()
	if err!=nil{
		log.Errorf("RawScan: Storage Reader error")
		response.Error=err.Error()
		return &response,err
	}

	var KVs []*kvrpcpb.KvPair
	limit :=req.GetLimit()
	start :=req.GetStartKey()
	it := storage_reader.IterCF(req.GetCf())
	for it.Seek(start);it.Valid()&&limit!=0;it.Next(){
		key:=it.Item().Key()
		value,err:=it.Item().Value()
		if err!=nil{
			log.Errorf("RaeScan: iterator value error")
			response.Error=err.Error()
			return &response,err
		}
		KVs=append(KVs,&kvrpcpb.KvPair{Key: key,Value: value})
		// bug: 忘记limit递减
		limit--
	}
	// bug: 关闭迭代器
	it.Close()

	response.Kvs=KVs
	return &response,nil
}
