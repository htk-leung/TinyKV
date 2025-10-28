package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).

	/*
		receiver 	Server
		input 		context, req.cf/key

					type RawGetRequest struct {
						Context              *Context `protobuf:"bytes,1,opt,name=context" json:"context,omitempty"`
				>		Key                  []byte   `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
				>		Cf                   string   `protobuf:"bytes,3,opt,name=cf,proto3" json:"cf,omitempty"`
						XXX_NoUnkeyedLiteral struct{} `json:"-"`
						XXX_unrecognized     []byte   `json:"-"`
						XXX_sizecache        int32    `json:"-"`
					}

		output		kvrpcpb.RawGetResponse, error

					type RawGetResponse struct {
						RegionError *errorpb.Error `protobuf:"bytes,1,opt,name=region_error,json=regionError" json:"region_error,omitempty"`
				>		Error       string         `protobuf:"bytes,2,opt,name=error,proto3" json:"error,omitempty"`
				>		Value       []byte         `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
						// True if the requested key doesn't exist; another error will not be signalled.
				>		NotFound             bool     `protobuf:"varint,4,opt,name=not_found,json=notFound,proto3" json:"not_found,omitempty"`
						XXX_NoUnkeyedLiteral struct{} `json:"-"`
						XXX_unrecognized     []byte   `json:"-"`
						XXX_sizecache        int32    `json:"-"`
					}
		task		given cf+key >> get value
	*/

	// new response
	resp := &kvrpcpb.RawGetResponse{}
    
    // get reader
	// func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) >> reader is a StorageReader
    reader, err := server.storage.Reader(req.Context) 
    if err != nil {
        resp.Error = err.Message() // errorpb.pb.go
        return resp, nil
    }
    defer reader.Close() // delay discarding txn
    
	// get value
	// func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) val is value of key
	val, err := reader.GetCF(req.Cf, req.key)
	if err != nil {
        resp.Error = err.Message()
        return resp, nil
	}

	// set resp value
	resp.Value = val
	if val == nil {
		resp.NotFound = true
	}

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	return nil, nil
}
