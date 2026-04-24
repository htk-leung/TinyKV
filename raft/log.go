// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"
	// "github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

/*
	Log replication
	To implement log replication, you may want to start with

	- handling MsgAppend and MsgAppendResponse on both the sender and receiver sides.
	- Checkout raft.RaftLog in raft/log.go which is a helper struct that helps you manage the raft log,
	  in here you also need to interact with the upper application by the Storage interface defined in raft/storage.go
	to get the persisted data like log entries and snapshot.

	You can run make project2ab to test the implementation and see some hints at the end of this part.
*/

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).

	// note stabled, committed and applied DON'T start counting from 0
	// they reflect actual index value in log
	// but ents[i] has raft log position i+snapshot.Metadata.Index (ref type MemoryStorage struct)

	hardstate, _, err := storage.InitialState()
	if err != nil {
		panic(err.Error())
	}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err.Error())
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err.Error())
	}

	// Get the term for the dummy entry 
	dummyTerm, err := storage.Term(firstIndex - 1)
	if err != nil {
		dummyTerm = 0  // If we can't get it, use 0
	}
	// entries never returns the dummy entry
	// entriesLastInd + 1 because ents[hi] is excluded from slice being returned
	// only fetch entries if there are any
	var entries []pb.Entry

	if firstIndex <= lastIndex {
        // get entries
        storageEntries, err := storage.Entries(firstIndex, lastIndex+1)
        if err != nil {
            panic(err.Error())
        }
        
        // Create entries with dummy entry at position 0
        entries = make([]pb.Entry, 0, len(storageEntries)+1)
        entries = append(entries, pb.Entry{Term: dummyTerm, Index: firstIndex - 1})
        entries = append(entries, storageEntries...)
    } else {
        // No entries in storage, just create dummy at index of first entry
        entries = []pb.Entry{{Term: dummyTerm, Index: firstIndex - 1}}
    }

	return &RaftLog{
		storage: 			storage, 			// storage contains all stable entries since the last snapshot.
												// refer to section "To restart a node from previous state:" in raft/doc.go
		stabled: 			lastIndex, 			// index of last entry that exists in storage
		committed: 			hardstate.GetCommit(), 	// index of last entry replicated to a quorum of peers
		applied: 			0, 					// index of last entry applied locally, not available here, but available in config, needs to be applied when calling newRaft
		entries:			entries, 			// all entries retrieved from storage
		pendingSnapshot: 	nil,				// (Used in 2C)
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).

	// A. use snapshot index : raftlog will follow if storage is snapshotted
	// 	but function does not always follow snapshots

	// B. use applied just like raftLogGCTaskHandler :
	// 	applied is a local state updated after calling handleRaftReady
	// 	should match applied in applystate
	//	should be the correct approach because the only entries needed are the ones after applied - committed & stabled & unstable
	// 	but needs to update entry return function s.t. it returns entries from .storage.ents if not found in raft.entries
	//	may break many things, try later

	// C. use storage.FirstIndex : always matching with persisted storage
	// 	can grow quite big, perm storage only shrinks when snapshotted
	logFirst := l.entries[0].Index
	storageFirst, _ := l.storage.FirstIndex()
	storageFirst -= 1 // FirstIndex returns index of ent[0]+1

	if logFirst < storageFirst { // only compact if starting index is different
		i := storageFirst - logFirst
		if i < uint64(len(l.entries)) { // < not <= because you always need 1 entry to become the dummy entry
			// Normal case: local truncation
            l.entries = l.entries[i:]
        } else {
            // Extreme case: full reload from storage
			lastIdx, _ := l.storage.LastIndex()
            ents, _ := l.storage.Entries(storageFirst, lastIdx+1)
			term, _ := l.storage.Term(storageFirst-1)
            dummy := pb.Entry{Index: storageFirst-1, Term: term}
            l.entries = append([]pb.Entry{dummy}, ents...)
		}
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	
	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).

	offset := l.entries[0].Index
	return l.entries[l.stabled-offset+1 : ]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	offset := l.entries[0].Index
	return l.entries[l.applied-offset+1 : l.committed-offset+1]
}

// LastIndex return the last index of the log entries
// INCLUDING UNSTABLE ONES
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).

	return l.entries[0].Index + uint64(len(l.entries)) - 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	
	// get vars
	offset := l.entries[0].Index
    lastind := l.LastIndex()

	// return if within bounds
	if i >= offset && i <= lastind {
		return l.entries[i-offset].Term, nil
	}
	// out of bounds error
	return 0, fmt.Errorf("Term(i): Index(%d) out of bounds of entries currently available in RaftLog", i)
}
