package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"

	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).

	/*
		// Read the value of a key at the given time.
		type GetRequest struct {
			Context              *Context `protobuf:"bytes,1,opt,name=context" json:"context,omitempty"`
			Key                  []byte   `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
			Version              uint64   `protobuf:"varint,3,opt,name=version,proto3" json:"version,omitempty"`
		}
		type Context struct {
			RegionId             uint64              `protobuf:"varint,1,opt,name=region_id,json=regionId,proto3" json:"region_id,omitempty"`
			RegionEpoch          *metapb.RegionEpoch `protobuf:"bytes,2,opt,name=region_epoch,json=regionEpoch" json:"region_epoch,omitempty"`
			Peer                 *metapb.Peer        `protobuf:"bytes,3,opt,name=peer" json:"peer,omitempty"`
			Term                 uint64              `protobuf:"varint,5,opt,name=term,proto3" json:"term,omitempty"`
		}
		type GetResponse struct {
			RegionError *errorpb.Error `protobuf:"bytes,1,opt,name=region_error,json=regionError" json:"region_error,omitempty"`
			Error       *KeyError      `protobuf:"bytes,2,opt,name=error" json:"error,omitempty"`
			Value       []byte         `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
			// True if the requested key doesn't exist; another error will not be signalled.
			NotFound             bool     `protobuf:"varint,4,opt,name=not_found,json=notFound,proto3" json:"not_found,omitempty"`
		}
		KvGet reads a value from the database at a supplied timestamp. If the key to be read is locked 
		by another transaction at the time of the KvGet request, then TinyKV should return an error. Otherwise, TinyKV must 
		search the versions of the key to find the most recent, valid value.
	*/

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		// A. region error
		if regionErr, ok := err.(*raft_storage.RegionError) {
			return &kvrpcpb.GetRequest{
				RegionError: regionErr.RequestErr,
			}
		}
		// B. not region error, not key error, not value, not not found
		return nil, err
	}
	defer reader.Close()

	// new transaction
	txn := mvcc.NewMvccTxn(reader, req.Version)

	// check for locks that signal concurrent writes
	lock, err := txn.GetLock(req.Key)
	if err != nil { // not region error, not key error, not value, not not found
		return nil, err
	}
	if lock != nil && lock.Ts < req.Version { // if key is locked 
		return &kvrpcpb.GetResponse{	// return key error
			Error:	&kvrpcpb.KeyError{
				Locked:	&kvrpcpb.LockInfo{
					PrimaryLock:	lock.Primary,
					LockVersion:	lock.Ts,
					Key:			req.Key,
					LockTtl:		lock.Ttl,
				}
			}
		}, nil
	}

	// get value
	val, err := txn.GetValue(key)
	if err != nil { // not region error, not key error, not value, not not found
		return nil, err
	}
	return &kvrpcpb.GetResponse{
		Value:     	val, 
		NotFound:   val == nil, // if val is nil then key is not found, refer to region_reader.go
	}, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).

	/*
		The protocol starts with the client getting a start timestamp from TinyScheduler. 
		It then builds the transaction locally, reading from the database 
		(using a KvGet or KvScan request which includes the start timestamp, 
		in contrast to RawGet or RawScan requests), but only recording writes locally in memory. 
		Once the transaction is built, the client will select one key as the primary key 
		(note that this has nothing to do with an SQL primary key). 
		The client sends KvPrewrite messages to TinyKV. A KvPrewrite message contains all the 
		writes in the transaction. A TinyKV server will attempt to lock all keys required by 
		the transaction. If locking any key fails, then TinyKV responds to the client 
		that the transaction has failed. The client can retry the transaction later 
		(i.e., with a different start timestamp). If all keys are locked, the prewrite succeeds. 
		Each lock stores the primary key of the transaction and a time to live (TTL).

		In fact, since the keys in a transaction may be in multiple regions and thus be 
		stored in different Raft groups, the client will send multiple KvPrewrite requests, 
		one to each region leader. Each prewrite contains only the modifications for that region.
	*/
	return nil, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).

	/*
		If all prewrites succeed, then the client will send a commit request for the region 
		containing the primary key. The commit request will contain a commit timestamp 
		(which the client also gets from TinyScheduler) which is the time at which the 
		transaction's writes are committed and thus become visible to other transactions.
	*/
	return nil, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
