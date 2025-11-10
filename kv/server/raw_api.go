package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

var (
	ErrEmptyReqCF := errors.New("Missing CF in request")
	ErrEmptyReqKey := errors.New("Missing Key in request")
	ErrEmptyReqVal := errors.New("Missing Val in request")
	ErrEmptyReqStartKey := errors.New("Missing StartKey in request")
	ErrEmptyReqLimit := errors.New("Missing iteration limit in request")
	ErrEmptyReqContext := errors.New("Missing context in request")
)

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).

	// new response
	resp := &kvrpcpb.RawGetResponse{}

	// get vars
	if reqContext := req.GetContext(); reqContext == nil {
		return resp, ErrEmptyReqContext
	}
	if reqCF := req.GetCF(); reqCF == nil {
		return resp, ErrEmptyReqCF
	}
	if reqKey := req.GetKey(); reqKey == nil {
		return resp, ErrEmptyReqKey
	}
    
    // get reader
    reader, err := server.storage.Reader(reqContext) 
    defer reader.Close() // delay discarding txn
    
	// get value and error
	resp.Value, resp.Error = reader.GetCF(reqCF, reqKey)

	// if not found set bool
	if resp.Error == badger.ErrKeyNotFound {
        resp.NotFound = true
	}

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	resp := &kvrpcpb.RawPutResponse{}

	// get vars
	if reqContext := req.GetContext(); reqContext == nil {
		return resp, ErrEmptyReqContext
	}
	if reqCF := req.GetCF(); reqCF == nil {
		return resp, ErrEmptyReqCF
	}
	if reqKey := req.GetKey(); reqKey == nil {
		return resp, ErrEmptyReqKey
	}
	if reqVal := req.GetValue(); reqVal == nil {
		return resp, ErrEmptyReqVal
	}

	// create batch
	batch := []storage.Modify {
		{
			Data: storage.Put {
				Cf:		reqCF,
				Key:	reqKey,
				Value:	reqVal,
			},
		}
	}

	// acquire latch
	keys = [][]byte{reqKey}
	server.Latches.WaitForLatches(keys)

	// write to storage
	resp.err = server.storage.Write(reqContext, batch)

	// release latch
	server.Latches.ReleaseLatches(keys)

	// return
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	// create response
	resp := &kvrpcpb.RawDeleteResponse{}

	// get vars
	if reqContext := req.GetContext(); reqContext == nil {
		return resp, ErrEmptyReqContext
	}
	if reqCF := req.GetCF(); reqCF == nil {
		return resp, ErrEmptyReqCF
	}
	if reqKey := req.GetKey(); reqKey == nil {
		return resp, ErrEmptyReqKey
	}

	// create batch
	batch := []storage.Modify {
		{
			Data: storage.Delete {
				Cf:		reqCF,
				Key:	reqKey,
			},
		}
	}

	// acquire latch
	keys = [][]byte{reqKey}
	server.Latches.WaitForLatches(keys)

	// write to storage
	resp.err = server.storage.Write(reqContext, batch)

	// release latch
	server.Latches.ReleaseLatches(keys)

	// return
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	// create response
	resp := &kvrpcpb.RawScanResponse{}

	// get vars
	if reqContext := req.GetContext(); reqContext == nil {
		return resp, ErrEmptyReqContext
	}
	if reqSK := req.GetStartKey(); reqSK == nil {
		return resp, ErrEmptyReqStartKey
	}
	if reqLimit := req.GetLimit(); reqLimit == nil {
		return resp, ErrEmptyReqLimit
	}
	if reqCF := req.GetCF(); reqCF == nil {
		return resp, ErrEmptyReqCF
	}

	// get reader
	reader, err := serer.storage.Reader(reqContext)
	defer reader.Close()

	// get it
	it := reader.IterCF(reqCF)

	// scan
	pair := make([]kvPair, reqLimit)
	it.Seek(reqSK)

	for i := 0; i < reqLimit; i++ {
		it.Next()
		item := it.Item()

		pair[i] = kvPair {
			keyError:	item.err,
			Key:		item.KeyCopy(),
			Value:		item.ValueCopy(),
		}
	}

	resp.Kvs = pair

	return nil, nil
}
