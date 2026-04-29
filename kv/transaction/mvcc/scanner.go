package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	// "github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	// "github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"

	// "fmt"
	"bytes"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).

	StartKey 	[]byte
	CurrKey 	[]byte
	Version		uint64
	Reader		storage.StorageReader
	it			engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iterator := txn.Reader.IterCF(engine_util.CfWrite)
	iterator.Seek(EncodeKey(startKey, TsMax))

	return &Scanner{
		StartKey: 	startKey,
		Version:	txn.StartTS,
		Reader:		txn.Reader,
		it:			iterator,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).

	if scan.it != nil {
        scan.it.Close()
    }
}

// Next returns the next key/value pair from the scanner. 
// If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).

	if scan.StartKey == nil || !scan.it.Valid() {
        return nil, nil, nil
    }

	var key []byte
	var val []byte

	for scan.it.Valid() {
		// if not get item
		item := scan.it.Item()
		rawKey := item.KeyCopy(nil)
		key = DecodeUserKey(rawKey)
		ts := decodeTimestamp(rawKey) 
		// ts is CommitTs because reading from CfWrite, must be < StartTs

		// check key
		if bytes.Compare(key, scan.StartKey) < 0 {
            scan.it.Next()
            continue
		}
		if bytes.Compare(key, scan.StartKey) > 0 {
			// already at next key, no value avail for this key
			scan.StartKey = key
			return key, nil, nil
		}

		if ts <= scan.Version {
			// val : now we have right key right version, must return
			writeVal, err := item.ValueCopy(nil)
			if err != nil {
				// err, adv to next key and return
				scan.advanceToNextKey()
				return nil, nil, err
			}
			write, err := ParseWrite(writeVal)
			if err != nil {
				// err, adv to next key and return
				scan.advanceToNextKey()
				return nil, nil, err
			}

            scan.advanceToNextKey()
			if write.Kind == WriteKindPut { // found it
				// value found, adv to next key and return
				val, err = scan.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
				if err != nil {
					return nil, nil, err
				}
				return key, val, nil
			} else {
				return key, nil, nil
			}
		}
		// version too new
		scan.it.Next()
	}

	// version too new
	return nil, nil, nil
}

// helper function to get the next key
func (scan *Scanner) advanceToNextKey() {
    // EncodeKey with TsMax on the next byte sequence advances past current key
    scan.it.Seek(EncodeKey(scan.StartKey, 0))

    if scan.it.Valid() {
        item := scan.it.Item()
        rawKey := item.KeyCopy(nil)
        scan.StartKey = DecodeUserKey(rawKey)
    } else {
        scan.StartKey = nil
    }
}