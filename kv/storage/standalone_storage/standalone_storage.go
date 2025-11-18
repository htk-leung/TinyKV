package standalone_storage

import (
	"github.com/Connor1996/badger"                  // revised badger version used by TinyKV
	"github.com/pingcap-incubator/tinykv/kv/config" // where path to db is
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util" // functions to define engine
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
)

// questions
// called by?
// purpose?

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).

	db   *badger.DB
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	return &StandAloneStorage{
		db:   nil,
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = s.conf.DBPath
	opts.ValueDir = s.conf.DBPath

	if err := os.MkdirAll(opts.Dir, os.ModePerm); err != nil {
		return err
	}

	// function Open() initializes variables and allocates space for db if path does exist yet, or open existing db if it does
	var err error
	s.db, err = badger.Open(opts)
	return err
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	// function Close() closes db
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	txn := s.db.NewTransaction(false)
	return &StandAloneReader{txn}, nil
}

type StandAloneReader struct {
	txn *badger.Txn
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {

	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound { // err handling ref region_reader.go
		return nil, err
	}
	return val, err
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneReader) Close() {
	// must call Discard() for transactions tp clear it
	r.txn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	wb := new(engine_util.WriteBatch)

	for _, m := range batch {
		// before := len(wb.entries)
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			wb.SetCF(put.Cf, put.Key, put.Value) // how to handle error????
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			wb.DeleteCF(delete.Cf, delete.Key) // how to handle error????
		}
		// if before == len(wb.entries) {
		// how to define error?
		// }
	}
	if err := wb.WriteToDB(s.db); err != nil {
		return err
	}
	return nil
}
