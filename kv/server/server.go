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

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	// "fmt"
	// "bytes"
	// "github.com/pingcap-incubator/tinykv/kv/util/engine_util"
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
		KvGet reads a value from the database at a supplied timestamp. If the key to be read is locked 
		by another transaction at the time of the KvGet request, then TinyKV should return an error. Otherwise, TinyKV must 
		search the versions of the key to find the most recent, valid value.

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
	*/

	// new transaction
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		// A. region error
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.GetResponse{
				RegionError: regionErr.RequestErr,
			}, nil
		}
		// B. not region error, not key error, not value, not not found
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)

	// wait for latch
	lock, err := txn.GetLock(req.Key)
	if err != nil { // not region error, not key error, not value, not not found
		return nil, err
	}
	// if lock ts is at least as old than current then you can't read yet
	if lock != nil && lock.Ts <= req.Version { 
		return &kvrpcpb.GetResponse{	// return key error
			Error:	&kvrpcpb.KeyError{
				Locked:	&kvrpcpb.LockInfo{
					PrimaryLock:	lock.Primary,
					LockVersion:	lock.Ts,
					Key:			req.Key,
					LockTtl:		lock.Ttl,
				},
			},
		}, nil
	}
	// if lock ts is newer than current then you can read

	// get value
	val, err := txn.GetValue(req.Key)
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

		type PrewriteRequest struct {
			Context   *Context    `protobuf:"bytes,1,opt,name=context" json:"context,omitempty"`
			Mutations []*Mutation `protobuf:"bytes,2,rep,name=mutations" json:"mutations,omitempty"`
			// Key of the primary lock.
			PrimaryLock          []byte   `protobuf:"bytes,3,opt,name=primary_lock,json=primaryLock,proto3" json:"primary_lock,omitempty"`
			StartVersion         uint64   `protobuf:"varint,4,opt,name=start_version,json=startVersion,proto3" json:"start_version,omitempty"`
			LockTtl              uint64   `protobuf:"varint,5,opt,name=lock_ttl,json=lockTtl,proto3" json:"lock_ttl,omitempty"`
		}
		type Context struct {
			RegionId             uint64              `protobuf:"varint,1,opt,name=region_id,json=regionId,proto3" json:"region_id,omitempty"`
			RegionEpoch          *metapb.RegionEpoch `protobuf:"bytes,2,opt,name=region_epoch,json=regionEpoch" json:"region_epoch,omitempty"`
			Peer                 *metapb.Peer        `protobuf:"bytes,3,opt,name=peer" json:"peer,omitempty"`
			Term                 uint64              `protobuf:"varint,5,opt,name=term,proto3" json:"term,omitempty"`
		}
		type Mutation struct {
			Op                   Op       `protobuf:"varint,1,opt,name=op,proto3,enum=kvrpcpb.Op" json:"op,omitempty"`
			Key                  []byte   `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
			Value                []byte   `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
		}
		type Op int32
		const (
			Op_Put      Op = 0
			Op_Del      Op = 1
			Op_Rollback Op = 2
			// Used by TinySQL but not TinyKV.
			Op_Lock Op = 3
		)
		type PrewriteResponse struct {
			RegionError          *errorpb.Error `protobuf:"bytes,1,opt,name=region_error,json=regionError" json:"region_error,omitempty"`
			Errors               []*KeyError    `protobuf:"bytes,2,rep,name=errors" json:"errors,omitempty"`
		}	
	*/

	// new transaction
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		// A. region error
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.PrewriteResponse{
				RegionError: regionErr.RequestErr,
			}, nil
		}
		// B. not region error, not key error, not value, not not found
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// attempt to lock all keys for THIS server
	var keyErrors []*kvrpcpb.KeyError
	var allKeys [][]byte
	for _, m := range req.Mutations {
		allKeys = append(allKeys, m.Key)
	}
	wg := server.Latches.AcquireLatches(allKeys)
	if wg != nil {
		keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Retryable: "Latch acquisition failed",
		})
	}
	defer server.Latches.ReleaseLatches(allKeys)

	// If locking any key for mvcc fails, then TinyKV responds to the client that the transaction has failed.
	for _, m := range req.Mutations {
		// save error on writes after our start timestamp
		write, ts, err := txn.MostRecentWrite(m.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && ts > req.StartVersion {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:		req.StartVersion,
					ConflictTs:		ts,
					Key:			m.Key,
					Primary:		req.PrimaryLock,
				},
			})
			continue // next key
		}

		// Abort on locks at any timestamp
		lock, err := txn.GetLock(m.Key)
		if err != nil { // not region error, not key error, not value, not not found
			return nil, err
		}
		if lock != nil && lock.Ts != req.StartVersion { 
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Locked:	&kvrpcpb.LockInfo{
					PrimaryLock:	lock.Primary,
					LockVersion:	lock.Ts,
					Key:			m.Key,
					LockTtl:		lock.Ttl,
				},
			})
			continue // next key
		}

		// if no one has lock and there is no conflicting write then stage put lock
		lock = &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(m.Op),
		}
		txn.PutLock(m.Key, lock)

		switch m.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(m.Key, m.Value)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(m.Key)
		case kvrpcpb.Op_Rollback: // ?
		default:
		}
	}

	// if there are errors don't write to storage
	if len(keyErrors) > 0 {
		return &kvrpcpb.PrewriteResponse{Errors: keyErrors}, nil
	}

	// if no errors stage writes
	server.storage.Write(req.Context, txn.Writes())
	
	// Return errors if any
	return &kvrpcpb.PrewriteResponse{}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).

	/*
		If all prewrites succeed, then the client will send a commit request for the region 
		containing the primary key. The commit request will contain a commit timestamp 
		(which the client also gets from TinyScheduler) which is the time at which the 
		transaction's writes are committed and thus become visible to other transactions.

		type CommitRequest struct {
			Context *Context `protobuf:"bytes,1,opt,name=context" json:"context,omitempty"`
			// Identifies the transaction, must match the start_version in the transaction's
			// prewrite request.
			StartVersion uint64 `protobuf:"varint,2,opt,name=start_version,json=startVersion,proto3" json:"start_version,omitempty"`
			// Must match the keys mutated by the transaction's prewrite request.
			Keys [][]byte `protobuf:"bytes,3,rep,name=keys" json:"keys,omitempty"`
			// Must be greater than start_version.
			CommitVersion        uint64   `protobuf:"varint,4,opt,name=commit_version,json=commitVersion,proto3" json:"commit_version,omitempty"`
		}
		type CommitResponse struct {
			RegionError          *errorpb.Error `protobuf:"bytes,1,opt,name=region_error,json=regionError" json:"region_error,omitempty"`
			Error                *KeyError      `protobuf:"bytes,2,opt,name=error" json:"error,omitempty"`
			XXX_NoUnkeyedLiteral struct{}       `json:"-"`
			XXX_unrecognized     []byte         `json:"-"`
			XXX_sizecache        int32          `json:"-"`
		}
	*/

	// new transaction
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		// A. region error
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CommitResponse{
				RegionError: regionErr.RequestErr,
			}, nil
		}
		// B. not region error, not key error, not value, not not found
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// get latches
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	var lock *mvcc.Lock

	// for each key
	for _, k := range req.Keys { 
		// check for repeated writes/rollback
		// if found ignore
		write, _, err := txn.CurrentWrite(k)
		if err != nil {
			return nil, err
		}
		if write != nil { 
			// if write exists, then
			// A. txn rolledback, default doesn't exist, leaving only the write entry
			if write.Kind == mvcc.WriteKindRollback {
				return &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{
						Abort:	"Transaction rolled back, write exists, data does not",
					},
				}, nil
			}
			// B. repeated commit, ignore key
			continue 
		}

		/*
			Consider prewritten checks
			A. If there is no data for key, then return nothing
			B. If there is data, but none is written by the same transaction, then return Retryable error
			C. If there is data, but that written by the same transaction is found, then continue
		*/
		keyWritten, thisTxn := txn.KeyExists(k)

		// A. key not written
		if !keyWritten && !thisTxn {
			return &kvrpcpb.CommitResponse{}, nil
		}
		
		// B. not prewritten by this txn
		if keyWritten && !thisTxn { 
			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable:	"KvCommit: prewritten data not found",
				},
			}, nil
		} 
		// C. VVVV

		// get lock : if lock still exists then it's valid = checking for write conflict
		// but can lock be replaced?
		lock, err = txn.GetLock(k)
		// if err, abort
		if err != nil {
			return nil, err
		}
		// if lock found and doesn't belong to this transaction or
		// if lock not found then someone else might have written to the key, abort
		if lock == nil || (lock != nil && lock.Ts != req.StartVersion) {
			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable:	"KvCommit: keyLocked, txn rolled back?",
				},
			}, nil
		}

		// PutWrite PutWrite(key []byte, ts uint64, write *Write)
		txn.PutWrite(k, req.CommitVersion, &mvcc.Write{
			StartTS: 	req.StartVersion,
			Kind:		lock.Kind,
		})
		// delete lock
		txn.DeleteLock(k)
	}
	// write to storage func (rs *RaftStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify)
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		// A. region error
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CommitResponse{
				RegionError: regionErr.RequestErr,
			}, nil
		}
		// B. not region error, not key error, not value, not not found
		return nil, err
	}

	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	/*
		type ScanResponse struct {
			RegionError *errorpb.Error `protobuf:"bytes,1,opt,name=region_error,json=regionError" json:"region_error,omitempty"`
			// Other errors are recorded for each key in pairs.
			Pairs                []*KvPair `protobuf:"bytes,2,rep,name=pairs" json:"pairs,omitempty"`
		}
		// Either a key/value pair or an error for a particular key.
		type KvPair struct {
			Error                *KeyError `protobuf:"bytes,1,opt,name=error" json:"error,omitempty"`
			Key                  []byte    `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
			Value                []byte    `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
			XXX_NoUnkeyedLiteral struct{}  `json:"-"`
			XXX_unrecognized     []byte    `json:"-"`
			XXX_sizecache        int32     `json:"-"`
		}
	*/

	// check limit, if 0 return empty response
	// check if it is valid, if not return empty response
	// find write, but for each check existence of default
	// list value conditions - inserted and still there, inserted and deleted

	return nil, nil
}

func (server *Server) KvCheckTxnStatus(ctx context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	/*
		KvCheckTxnStatus checks for timeouts, 
		removes expired locks and returns the status of the lock.
	*/
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		// A. region error
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CheckTxnStatusResponse{
				RegionError: regionErr.RequestErr,
			}, nil
		}
		// B. not region error, not key error, not value, not not found
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.LockTs)

	// get lock & write
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	write, ts, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	if write != nil {
		// A. committed if write with kind!=WriteKindRollback
		if write.Kind != mvcc.WriteKindRollback {
			if lock != nil {
				txn.DeleteLock(req.PrimaryKey)
			}
			return &kvrpcpb.CheckTxnStatusResponse{
				CommitVersion: 	ts,
				Action: 		kvrpcpb.Action_NoAction,
			}, nil
		} else {
		// B. rolled back if write with kind=WriteKindRollback
			if lock != nil {
				txn.DeleteLock(req.PrimaryKey)
			}
			return &kvrpcpb.CheckTxnStatusResponse{
				// rolled back: lock_ttl == 0 && ts == 0,
				LockTtl: 		0,
				CommitVersion: 	0,
				Action: 		kvrpcpb.Action_NoAction,
			}, nil
		}
	}

	// no writes: process still running / blocked
	if lock == nil {
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: 			req.LockTs,
			Kind:				mvcc.WriteKindRollback, 
			// needs both fields even when there is no lock to rollback
		})

		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			// A. region error
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.CheckTxnStatusResponse{
					RegionError: regionErr.RequestErr,
				}, nil
			}
			// B. not region error, not key error, not value, not not found
			return nil, err
		}

		return &kvrpcpb.CheckTxnStatusResponse{
			Action: 		kvrpcpb.Action_LockNotExistRollback,
		}, nil
	}
	// C. locked, lock still alive
	if (req.CurrentTs - req.LockTs)/1000 < lock.Ttl { // mind the UNIT
		return &kvrpcpb.CheckTxnStatusResponse{
			LockTtl: 		lock.Ttl - (req.CurrentTs - req.LockTs),
			Action: 		kvrpcpb.Action_NoAction,
		}, nil
	} else {
	// D. lock expired, needs to roll back txn
		// rollback all rows for this txn
		klPair, err := mvcc.AllLocksForTxn(txn)
		if err != nil {
			return nil, err
		}
		var keys [][]byte
		for _, p := range klPair {
			keys = append(keys, p.Key)
		}
		resp, err := server.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
			Context:			req.Context,
			StartVersion:		req.LockTs,
			Keys:				keys,
		})
		if err != nil {
			return nil, err
		}
		if resp.RegionError != nil {
			return &kvrpcpb.CheckTxnStatusResponse{
				RegionError:	resp.RegionError,
			}, nil
		}

		// txn.DeleteLock(req.PrimaryKey)
		// txn.DeleteValue(req.PrimaryKey)

		// txn.PutWrite(req.PrimaryKey, lock.Ts, &mvcc.Write{
		// 	StartTS: 	lock.Ts,
		// 	Kind:		mvcc.WriteKindRollback,
		// })

		// err = server.storage.Write(req.Context, txn.Writes())
		// if err != nil {
		// 	// A. region error
		// 	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		// 		return &kvrpcpb.CheckTxnStatusResponse{
		// 			RegionError: regionErr.RequestErr,
		// 		}, nil
		// 	}
		// 	// B. not region error, not key error, not value, not not found
		// 	return nil, err
		// }

		return &kvrpcpb.CheckTxnStatusResponse{
			LockTtl: 		0,
			Action: 		kvrpcpb.Action_TTLExpireRollback,
		}, nil
	}

	// no write, no lock
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	/*
		KvBatchRollback checks that a key is locked by the current transaction, 
		and if so removes the lock, deletes any value and leaves a rollback indicator as a write.
		
		type BatchRollbackRequest struct {
			Context              *Context `protobuf:"bytes,1,opt,name=context" json:"context,omitempty"`
			StartVersion         uint64   `protobuf:"varint,2,opt,name=start_version,json=startVersion,proto3" json:"start_version,omitempty"`
			Keys                 [][]byte `protobuf:"bytes,3,rep,name=keys" json:"keys,omitempty"`
		}
		type BatchRollbackResponse struct {
			RegionError          *errorpb.Error `protobuf:"bytes,1,opt,name=region_error,json=regionError" json:"region_error,omitempty"`
			Error                *KeyError      `protobuf:"bytes,2,opt,name=error" json:"error,omitempty"`
		}
	*/
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		// A. region error
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.BatchRollbackResponse{
				RegionError: regionErr.RequestErr,
			}, nil
		}
		// B. not region error, not key error, not value, not not found
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// for each key
	for _, key := range req.Keys {
		// if already committed, do nothing, implies duplicate rollback req
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				return &kvrpcpb.BatchRollbackResponse{
					Error: 	&kvrpcpb.KeyError{
						Abort:	"KvRollback: trying to rollback key already committed",
					},
				}, nil
			}
			continue
		}
		// check missing prewrite : if missing insert write anyway but don't delete anything
		lock, _ := txn.GetLock(key)
		if lock != nil && lock.Ts == req.StartVersion {
			txn.DeleteLock(key)
		}
		val, _ := txn.GetValuePrewritten(key)
		if val != nil {
			txn.DeleteValue(key)
		}
		
		// enter Writer entry, use startversion as commitTS because nothing moved
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: 	req.StartVersion,
			Kind:		mvcc.WriteKindRollback,
		})
	}
	// write to storage func (rs *RaftStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify)
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		// A. region error
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.BatchRollbackResponse{
				RegionError: regionErr.RequestErr,
			}, nil
		}
		// B. not region error, not key error, not value, not not found
		return nil, err
	}

	return &kvrpcpb.BatchRollbackResponse{}, nil
}

func (server *Server) KvResolveLock(ctx context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).

	/*
		type ResolveLockRequest struct {
			Context              *Context `protobuf:"bytes,1,opt,name=context" json:"context,omitempty"`
			StartVersion         uint64   `protobuf:"varint,2,opt,name=start_version,json=startVersion,proto3" json:"start_version,omitempty"`
			CommitVersion        uint64   `protobuf:"varint,3,opt,name=commit_version,json=commitVersion,proto3" json:"commit_version,omitempty"`
		}
		// Empty if the lock is resolved successfully.
		type ResolveLockResponse struct {
			RegionError          *errorpb.Error `protobuf:"bytes,1,opt,name=region_error,json=regionError" json:"region_error,omitempty"`
			Error                *KeyError      `protobuf:"bytes,2,opt,name=error" json:"error,omitempty"`
		}
	*/
	// create txn
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		// A. region error
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.ResolveLockResponse{
				RegionError: regionErr.RequestErr,
			}, nil
		}
		// B. not region error, not key error, not value, not not found
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// get all locks
	klPairs, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		return nil, err
	}

	// A. no locks left, nothing to do
	if len(klPairs) == 0 {
		return &kvrpcpb.ResolveLockResponse{}, nil
	}
	
	// collect all keys
 	var keys [][]byte
    for _, pair := range klPairs {
		keys = append(keys, pair.Key)
    }

	// count writes
	var counter int
	for _, key := range keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil { 
			return nil, err 
		}
		if write == nil { 
			continue 
		}
		counter++
	}

	// B. roll back when primary lock is still here, meaning it has not started committing
	// or when no writes found
	if counter == 0 && req.CommitVersion == 0 {
		resp, err := server.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
			Context:			req.Context,
			StartVersion:		req.StartVersion,
			Keys:				keys,
		})
		if err != nil {
			return nil, err
		}
		if resp.RegionError != nil {
			return &kvrpcpb.ResolveLockResponse{
				RegionError:	resp.RegionError,
			}, nil
		}
	} 
	// C. primary lock gone, but some keys have no Write, means there are values to clean up
	// roll forward for each
	if counter < len(keys) && req.CommitVersion != 0 {

		resp, err := server.KvCommit(ctx, &kvrpcpb.CommitRequest{
			Context:		req.Context,
			StartVersion:	req.StartVersion,
			Keys:			keys,
			CommitVersion:	req.CommitVersion,
		})
		if err != nil {
			return nil, err
		}
		if resp.RegionError != nil {
			return &kvrpcpb.ResolveLockResponse{
				RegionError:	resp.RegionError,
			}, nil
		}
		if resp.Error != nil {
			return &kvrpcpb.ResolveLockResponse{
				Error:	resp.Error,
			}, nil
		}
	}

	// return empty resp if the lock is resolved successfully
	return &kvrpcpb.ResolveLockResponse{}, nil
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