package mvcc

import (
	"encoding/binary"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"bytes"
	"math"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
// An MvccTxn should know the start timestamp of the request it is representing.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	/*
		Write is a representation of a committed write to backing storage.
		A serialized version is stored in the "write" CF of our engine when a write is committed. That allows MvccTxn to find
		the status of a key at a given timestamp.
		
		type Write struct {
			StartTS uint64
			Kind    WriteKind
		}
	*/
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key: EncodeKey(key, ts), // commit timestamp included in key for mvcc
			Value: write.ToBytes(),
			Cf: engine_util.CfWrite,
		},
	})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	lockBytes, err := txn.Reader.GetCF(engine_util.CfLock, key)

	if lockBytes != nil {
		lock, parerr := ParseLock(lockBytes)
		return lock, parerr
	}

	return nil, err
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	/*
		calling PutLock should stage a single serialized lock entry 
		in the CfLock column family at the specified key
	*/
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key: key, // locks don't need versions so no ts needed
			Value: lock.ToBytes(),
			Cf: engine_util.CfLock,
		},
	})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: key, 
			Cf: engine_util.CfLock,
		},
	})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	/*
		func singleEntry(m *storage.MemStorage) {
			m.Set(engine_util.CfDefault, EncodeKey([]byte{16, 240}, 40), []byte{1, 2, 3})
			write := Write{
				StartTS: 40,
				Kind:    WriteKindPut,
			}
			// func (s *MemStorage) Set(cf string, key []byte, value []byte) 
			m.Set(engine_util.CfWrite, EncodeKey([]byte{16, 240}, 42), write.ToBytes())
		}
		func TestGetValueSimple4A(t *testing.T) { // 7
			txn := testTxn(43, singleEntry)

			value, err := txn.GetValue([]byte{16, 240})
			assert.Nil(t, err)
			assert.Equal(t, []byte{1, 2, 3}, value)
		}
		const (
			WriteKindPut      WriteKind = 1
			WriteKindDelete   WriteKind = 2
			WriteKindRollback WriteKind = 3
		)
	*/

	// first find the commit entry
	// get BadgerIterator
	it := txn.Reader.IterCF(engine_util.CfWrite) // CfWrite because only searching for committed values
	defer it.Close()

	// position iterator
	it.Seek(EncodeKey(key, txn.StartTS))
	for ; it.Valid(); it.Next() {
		item := it.Item()

		// check key is still the same
		userKey := DecodeUserKey(item.Key())
        if !bytes.Equal(userKey, key) {
            return nil, nil
        }

		// get value in item
		valBytes, err := item.Value()
        if err != nil {
            return nil, err
        }
		write, err := ParseWrite(valBytes)
        if err != nil {
            return nil, err
        }

		// for each key the latest one must be of kind WriteKindPut, otherwise it's rolled back/deleted
		// if Kind == WriteKindRollback find next
		if write.Kind == WriteKindRollback {
			continue
		}
		// if Kind == WriteKindDelete return nil because entry no longer exists
		if write.Kind == WriteKindDelete {
			return nil, nil
		}

		// return getCF
		return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
	}

	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key: EncodeKey(key, txn.StartTS), // start timestamp included in key for mvcc
			Value: value,
			Cf: engine_util.CfDefault,
		},
	})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).

	// The most challenging methods to implement are likely to be GetValue and the methods for retrieving writes. 
	// You will need to use StorageReader to iterate over a CF. 
	// Bear in mind the ordering of encoded keys, and remember that when deciding when a value is valid 
	// depends on the commit timestamp, not the start timestamp, of a transaction.

	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: EncodeKey(key, txn.StartTS), 
			Cf: engine_util.CfDefault,
		},
	})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).

	// The most challenging methods to implement are likely to be GetValue and the methods for retrieving writes. 
	// You will need to use StorageReader to iterate over a CF. 
	// Bear in mind the ordering of encoded keys, and remember that when deciding when a value is valid 
	// depends on the commit timestamp, not the start timestamp, of a transaction.

	// get BadgerIterator
	it := txn.Reader.IterCF(engine_util.CfWrite) // CfWrite because only searching for committed values
	defer it.Close()

	// position iterator
	// don't know commit ts, only know start ts
	// but max timestamp is input max of uint64
	// then seek will find the most recent write
	it.Seek(EncodeKey(key, uint64(math.MaxUint64))) 

	// call Next() until found
	for ; it.Valid(); it.Next() {
		item := it.Item()

		// check key is still the same
		userKey := DecodeUserKey(item.Key())
        if !bytes.Equal(userKey, key) {
            return nil, 0, nil
        }

		// get value in item
		valBytes, err := item.Value()
        if err != nil {
            return nil, 0, err
        }
		write, err := ParseWrite(valBytes)
        if err != nil {
            return nil, 0, err
        }

		// each write has a startTS
		// if startTS matches then return write
		if write.StartTS == txn.StartTS {
			return write, decodeTimestamp(item.Key()), nil
		}
	}

	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).

	// The most challenging methods to implement are likely to be GetValue and the methods for retrieving writes. 
	// You will need to use StorageReader to iterate over a CF. 
	// Bear in mind the ordering of encoded keys, and remember that when deciding when a value is valid 
	// depends on the commit timestamp, not the start timestamp, of a transaction.

	// get BadgerIterator
	it := txn.Reader.IterCF(engine_util.CfWrite) // CfWrite because only searching for committed values
	defer it.Close()

	// position iterator
	it.Seek(EncodeKey(key, uint64(math.MaxUint64))) 

	// call Next() until found
	for ; it.Valid(); it.Next() {
		item := it.Item()

		// check key is still the same
		userKey := DecodeUserKey(item.Key())
        if !bytes.Equal(userKey, key) {
            return nil, 0, nil
        }

		// get value in item
		valBytes, err := item.Value()
        if err != nil {
            return nil, 0, err
        }
		write, err := ParseWrite(valBytes)
        if err != nil {
            return nil, 0, err
        }

		return write, decodeTimestamp(item.Key()), nil
	}

	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
