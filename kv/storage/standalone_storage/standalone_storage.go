package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config" // where path to db is
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/Connor1996/badger" // revised badger version used by TinyKV
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util" // functions to define engine
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).

	// the struct contains the standalone storage object >> look for API code that creates new storage
	// type DB, in package badger
	// should be a pointer to avoid value copying
	db *Engines 
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	// function to create new DB and return StandAloneStorage object defined in engine_util/engines.go
	db := engine_util.CreateDB(/*path string*/conf.DBPath, /*raft bool*/false) /* returns *badger.DB */

	// func NewEngines(kvEngine, raftEngine *badger.DB, kvPath, raftPath string) *Engines in engines.go
	e := NewEngines(db, nil, conf.DBPath, nil)
	return &StandAloneStorage{e}
}


/*--------------------Start()--------------------*/
func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	// main.go : 	if err := storage.Start(); err != nil {
	// 					log.Fatal(err)
	// 				}

	// Start() only returns mem_storage.go only returns nil. Why is that so? 
	// Because call to storage.Start() is only successful if db is created succesfully?
	return nil
}
/*--------------------Start()--------------------*/


/*--------------------Close()--------------------*/
func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).

	// function to remove all stored values for an engine defined in engines.go
	s.db.Destroy()
	return nil
}
/*--------------------Close()--------------------*/


/*--------------------Reader()--------------------*/
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	/*
		Instructions:
		Reader should return a StorageReader that supports key/value's point get and scan operations on a snapshot.
		You should use badger.Txn to implement the Reader function because the transaction handler provided by badger 
		could provide a consistent snapshot of the keys and values.
		ref: https://pkg.go.dev/github.com/dgraph-io/badger#section-readme

		receiver 	StandAloneStorage = engine
		input 		kvrpcpb.Context but can be ignored for now
		output 		storage.StorageReader >> nothing in TinyKV returns this type >> what to create?
		task		create new struct to use with StorageReader interface
	*/
	txn := s.Kv.NewTransaction(false)
	return TxnStorageReader{txn}, nil
}


type TxnStorageReader struct {
	txn *badger.Txn
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	/*
		receiver 	StandAloneStorage = engine
		input 		cf string, key
		output 		values
		task		get cf

		Function defined to get CF from txn in engine/util.go
			func GetCFFromTxn(txn *badger.Txn, cf string, key []byte) (val []byte, err error) 
	*/
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound { // err handling ref region_reader.go
		return nil, nil
	}
	return val, err
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	/*
		receiver 	r.txn
		input 		cf string
		output 		DBIterator interface value >> implemented as BadgerIterator in engine_util/cf_iterator.go 
		task		cf, r.txn >> NewCFIterator >> DBIterator

		type BadgerIterator struct {
			iter   *badger.Iterator
			prefix string
		}

		Function defined to create iterator from txn and cf string in engine_util/cf_iterator.go 
			func NewCFIterator(cf string, txn *badger.Txn) *BadgerIterator
	*/
	return &NewCFIterator(cf, r.txn)
}

func (r *StandAloneReader) Close() {
	/*
		Refer to transaction handling notes re need to discard txn 
		at https://pkg.go.dev/github.com/dgraph-io/badger#section-readme.
	*/

	r.txn.Discard()
}
/*--------------------Reader()--------------------*/


/*--------------------Write()---------------------*/
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	/*
		receiver	StandAlongStorage = engine
		input		kvrpcpb.Context, storage.Modify array/slice
		output		err
		task		[]storage.Modify >> WriteBatch >> WriteKV

		function defined to write to engine in engine_util/engines.go
			func (en *Engines) WriteKV(wb *WriteBatch) error

		type WriteBatch struct {
			entries       []*badger.Entry 	>> func (wb *WriteBatch) SetCF(cf string, key, val []byte)
			size          int				>> func (wb *WriteBatch) SetCF(cf string, key, val []byte)
			safePoint     int				>> func (wb *WriteBatch) SetSafePoint()
			safePointSize int				>> func (wb *WriteBatch) SetSafePoint()
			safePointUndo int				>> ??
		}
	
		type Modify struct { >>> any data structure
			Data interface{}
		}
	*/
	wb := new(engine_util.WriteBatch)

    for _, m := range batch {
        switch m.Data.(type) {
        case storage.Put:
            put := m.Data.(storage.Put)
            wb.SetCF(put.Cf, put.Key, put.Value)
        case storage.Delete:
            delete := m.Data.(storage.Delete)
            wb.DeleteCF(delete.Cf, delete.Key)
        }
    }

	wb.SetSafePoint()
	s.Kv.WriteKV(wb)
	return nil
}
/*--------------------Write()---------------------*/