// // Copyright 2015 The etcd Authors
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.

package raft

import (
	"errors"
	// "bytes"
	"fmt"
	"sort"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	// "github.com/pingcap-incubator/tinykv/log"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64 // << this must be the vote cast for this term, which means that every time term changes Vote must become 0

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records : for the current term this other peer voted yes/no
	// [peer]voted yes/no
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	baseElectionTimeout int
	electionTimeout int // actual timeout used
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// randomize election timeout
func (r *Raft) getRandElectionTimeout() int {
	// gives range [electionTimeout, 2*electionTimeout)
    return r.baseElectionTimeout + rand.Intn(r.baseElectionTimeout)
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	// DUMMY PRINT
	fmt.Printf("")
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	// Your Code Here (2A).
	log := newLog(c.Storage)
	log.applied = c.Applied

	hardstate, confstate, _ := c.Storage.InitialState() // error is always nil

	prs := make(map[uint64]*Progress)
	var configpeers []uint64
	if len(c.peers) > 0 {
		configpeers = c.peers
	} else {
		configpeers = confstate.Nodes
	}
	for _, p := range configpeers {
		prs[p] = &Progress{
			Match:	log.LastIndex(),
			Next:	log.LastIndex() + 1,
		}
	}

	// Your Code Here (2A).
	return &Raft{
		id:					c.ID,
		Term:				hardstate.Term,
		Vote:				hardstate.Vote,
		RaftLog:			log,
		Prs:				prs,
		State:				StateFollower,
		votes: 				make(map[uint64]bool),
		msgs:				make([]pb.Message, 0),	
		Lead:				0,
		heartbeatTimeout:	c.HeartbeatTick,
		baseElectionTimeout:c.ElectionTick,
		electionTimeout:	c.ElectionTick + rand.Intn(c.ElectionTick),
		heartbeatElapsed:	0,
		electionElapsed :	0,
		leadTransferee:		0,
		PendingConfIndex:	0,
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).

	// If leader advance heartbeatElapsed
	if r.State == StateLeader {
		r.heartbeatElapsed++
		// if times up
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			// send heartbeat by sending self
			r.heartbeatElapsed = 0  // Reset after timeout
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From: r.id, 
				To: r.id, 
			})
		}
		return
	}
	// Advance electionElapsed in any other role
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
        r.electionElapsed = 0
        r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup, 
			To: r.id, 
			From: r.id, 
			Term: r.Term,
		})
		r.electionTimeout = r.getRandElectionTimeout()
    }
}

// Function reverts peer into a follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).

	// when leader/candidates become followers a new term started >> don't casually call becomeFollower
	r.State = StateFollower
	r.Term = term
	r.Lead = lead

	r.Vote = 0
	r.votes = make(map[uint64]bool) 
	r.electionElapsed = 0
	r.electionTimeout = r.getRandElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).

	// State changes
	r.State = StateCandidate
	r.Term++
	r.Lead = 0 // EDIT

	// Votes for itself
	r.Vote = r.id
	r.votes[r.id] = true

	// Reset electionTimeout
	r.electionElapsed = 0
	r.electionTimeout = r.getRandElectionTimeout()

	// Edge case : self is the only member! must count your own vote and become leader here
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).

	// NOTE: Leader should propose a noop entry on its term
	// log.Infof("[%d] became leader, term=%d", r.id, r.Term)

	// Update state
	r.State = StateLeader
	// EDIT: leader should not need to update this, becomeFollower will reset
	// r.Vote = 0
	r.votes = make(map[uint64]bool)
	r.Lead = r.id
	r.heartbeatElapsed = 0

	// Propose noop entry
	lastIndex := r.RaftLog.LastIndex()

	entries := make([]*pb.Entry, 0)
	entries = append(entries, &pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     lastIndex + 1,
	})

	// Save entry locally and broadcast
	r.AppendEntries(entries)
	r.bcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	// Do not handle message terms here. 
	// Stale RequestVoteRPCs can be used to send updates to crazy servers 

	switch m.MsgType {

	// V MessageType_MsgHup >> start new election
	case pb.MessageType_MsgHup:
		switch r.State { // case if statemachine in this state receives this type of msg
		case StateFollower, StateCandidate:
			r.campaign(m)
		case StateLeader:
			// leader remains leader until it fails, it does not just become a candidate
		}

	// V 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
	// of the 'MessageType_MsgHeartbeat' type to its followers.
	case pb.MessageType_MsgBeat:
		switch r.State {
		case StateFollower, StateCandidate:
		case StateLeader:
			r.handleBeat(m)
		}

	// V 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
	case pb.MessageType_MsgPropose:
		switch r.State {
		case StateFollower, StateLeader:
			r.handlePropose(m)
		case StateCandidate:
			// requirement from doc.go: when passed to candidate, 'MessageType_MsgPropose' is dropped.
		}

	// V 'MessageType_MsgAppend' >> AppendEntriesRPC
	case pb.MessageType_MsgAppend:
		switch r.State {
		case StateFollower:
			r.fhandleAppendEntries(m)
		case StateCandidate:
			r.chandleAppendEntries(m)
		case StateLeader:
			r.lhandleAppendEntries(m)
		}

	// V 'MessageType_MsgAppendResponse' >> AppendEntriesRPC response
	case pb.MessageType_MsgAppendResponse:
		switch r.State {
		case StateFollower:
			r.handleAppendEntriesResponse(m)
		case StateCandidate:
			r.handleAppendEntriesResponse(m)
		case StateLeader:
			r.handleAppendEntriesResponse(m)
		}

	// V 'MessageType_MsgRequestVote' >> RequestVoteRPC
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)

	// V 'MessageType_MsgRequestVoteResponse' >> RequestVoteRPC response
	case pb.MessageType_MsgRequestVoteResponse:
		switch r.State {
		case StateFollower, StateLeader:
			// Only meaningful to a candidate, ignore
		case StateCandidate:
			r.handleRequestVoteResponse(m)
		}

	// 'MessageType_MsgSnapshot' requests to install a snapshot message.
	case pb.MessageType_MsgSnapshot:
		switch r.State { 
		case StateFollower:
			r.handleSnapshot(m)
		case StateCandidate:
			r.handleSnapshot(m)
		case StateLeader:
		}

	// V 'MessageType_MsgHeartbeat' >> AppendEntries RPC
	case pb.MessageType_MsgHeartbeat:
		switch r.State { 
		case StateFollower:
			r.handleHeartbeat(m)
		case StateCandidate:
			r.handleHeartbeat(m)
		case StateLeader:
			r.handleHeartbeat(m)
		}

	// V 'MessageType_MsgHeartbeatResponse' >> AppendEntries RPC
	case pb.MessageType_MsgHeartbeatResponse:
		switch r.State { 
		case StateFollower, StateCandidate:
			// normally received by leader, forward to leader as r.handleHeartbeat(m)
		case StateLeader:
			r.handleHeartbeatResponse(m)
		}

	// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
	case pb.MessageType_MsgTransferLeader:
		switch r.State {
		case StateFollower:
		case StateCandidate:
		case StateLeader:
		}

	// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
	// the transfer target timeout immediately and start a new election.
	case pb.MessageType_MsgTimeoutNow:
		switch r.State { 
		case StateFollower:
		case StateCandidate:
		case StateLeader:
		}
	}

	return nil
}

// Function used to kickstart an election campaign for a follower
func (r *Raft) campaign(m pb.Message) {
	/* from raft/doc.go
		When a node is a follower or candidate and 'MessageType_MsgHup' is passed to its Step method,
		then the node calls 'campaign' method to campaign itself to become a leader. Once 'campaign'
		method is called, the node becomes candidate and sends 'MessageType_MsgRequestVote' to peers
		in cluster to request votes.
	*/
	/* from Raft paper
		Arguments:
		term			candidate’s term
		candidateId		candidate requesting vote
		lastLogIndex	index of candidate’s last log entry (§5.4)
		lastLogTerm		term of candidate’s last log entry (§5.4)
	*/

	// Node becomes candidate
	r.becomeCandidate()

	// Sends 'MessageType_MsgRequestVote' to peers
	lastEntryIdx := r.RaftLog.LastIndex()
	lastEntryTerm, err := r.RaftLog.Term(lastEntryIdx)
	if err != nil {
		panic("lastEntryIdxN out of bounds for Term")
	}
	
	for p := range r.Prs {
		if p != r.id {			
			r.msgs = append(r.msgs, pb.Message{
				MsgType: 	pb.MessageType_MsgRequestVote,
				To:      	p,
				From:    	r.id,
				Term:    	r.Term,
				LogTerm: 	lastEntryTerm,
				Index:   	lastEntryIdx,
			})
		}
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	/* From raft paper
		Receiver implementation: grant vote if
		1. Reply false if term < currentTerm (§5.1)
		2. If votedFor is null or candidateId, and 
		3. candidate’s log is at least as up-to-date as receiver’s log (§5.2, §5.4)

		* Each server can only vote for 1 candidate per term
	*/

	// 1. If term < local : reject
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	} else if m.Term > r.Term {
		r.becomeFollower(m.Term, 0)
	} else {
		if r.State == StateCandidate {
			r.becomeFollower(m.Term, 0)
		} else if r.State == StateLeader {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})
			return
		} // Follower keeps going as usual
	}

	// 2. If votedFor is not null and not equal to request candidateId : reject
	// If voted for cand already send response again
	if r.Vote == m.From {	
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  false,
		})
		return
	} else if r.Vote != 0 { // Already voted for someone else
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}

	// 3. If candidate log is *at least as* up to date as the local log
	lastEntryInd := r.RaftLog.LastIndex()
	lastEntryTerm, err := r.RaftLog.Term(lastEntryInd)
	if err != nil {
		panic("lastEntryInd out of bounds for use as r.RaftLog.Term(lastEntryInd)")
	}

	var candUpToDate bool
	if m.LogTerm != lastEntryTerm {
		// If different last term, later term is more up to date
		candUpToDate = m.LogTerm > lastEntryTerm
	} else { 
		// If same last term, larger entry index is more up to date
		candUpToDate = m.Index >= lastEntryInd
	}

	// Record and respond
	if candUpToDate {
		r.Vote = m.From
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  !(candUpToDate), // EDIT : deal with condition 3 only
	})
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	/* From raft/doc.go
		Candidate
		1. calculates how many votes it has won
		2. if it's more than majority (quorum),
			- it becomes leader and
			- calls 'bcastAppend'.
		3. if candidate receives majority of votes of denials, it
			- reverts back to follower.
	*/

	if m.Term < r.Term || r.State != StateCandidate {
		return
	}
	
	// Save response
	r.votes[m.From] = !m.Reject

	// Count votes
	var votedFor, votedAgainst, quorum int
	quorum = len(r.Prs)/2 + 1
	for _, vote := range r.votes {
		if vote == true {
			votedFor++
		}
	}
	votedAgainst = len(r.votes) - votedFor
	// log.Infof("[%d] votes: for=%d against=%d quorum=%d", r.id, votedFor, votedAgainst, quorum)

	if votedFor >= quorum {
		r.becomeLeader()
	} else if votedAgainst >= quorum {
		r.becomeFollower(r.Term, r.Lead)
	} // Else wait for more votes
}

// Helper function for leader to append entries locally
func (r *Raft) AppendEntries(entries []*pb.Entry) {
	// Append
	for _, entry := range entries {
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	// If leader is the only Raft member then also update Committed
	if len(r.Prs) == 1 {
		r.RaftLog.committed += uint64(len(entries))
	}
	// Regardless, update Match and Next
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
}

// Helper function for leader to broadcast AppendEntriesRPC
func (r *Raft) bcastAppend() {
	/*
		Use case A.
		- leader receives proposal to append entries to log
		- leader adds entries
		- leader calls bcastappend to send entries to peers

		Use case B.
		- candidate becomes leader
		- leader calls bcastappend to send entry to peers

		* always sent from leader to followers to ask followers to append entries
	*/

	// Must be called by leader only
	if r.Lead != r.id {
		panic("bcastAppend called by non-Leader")
	}

	// Send requests
	for p := range r.Prs {
		if p != r.id {
			// If sendAppend returns false (snapshot not ready)
			// then wait for the next heartbeat response to trigger another sendAppend call via handleHeartbeatResponse
			// by then the snapshot will likely be ready
			r.sendAppend(p)
		}
	}
}

// Helper function called by leader through bcastappend to 
//   send an AppendEntriesRPC with new entries (if any) and
//   the current commit index to the given peer. 
// Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	/* from doc.go
		If you need to send out a message, just push it to raft.Raft.msgs and
		all messages the raft received will be passed to raft.Raft.Step()

		'MessageType_MsgSnapshot' requests to install a snapshot message. When a node has just
		become a leader or the leader receives 'MessageType_MsgPropose' message, it calls
		'bcastAppend' method, which then calls 'sendAppend' method to each
		follower. In 'sendAppend', if a leader fails to get term or entries,
		the leader requests snapshot by sending 'MessageType_MsgSnapshot' type message.

		** only called by leader
	*/

	if to == r.Lead {
		return false
	}

	offset :=  r.RaftLog.entries[0].Index
	prevIdxi := r.Prs[to].Match-offset

	// If peer is too behind send snapshot
	// Too behind means matchIdx < r.RaftLog.entries[0].Index
	if prevIdxi < 0 {
		snapshot, err := r.RaftLog.storage.Snapshot()
		if err != nil {
			return false // if not ready yet try again later
		}

		r.msgs = append(r.msgs, pb.Message{
			MsgType: 	pb.MessageType_MsgSnapshot,
			To:      	to,
			From:    	r.id,
			Term:    	r.Term,
			Snapshot:	&snapshot,
		})
		return true
	}

	// If not, continue
	prevLogEntry := r.RaftLog.entries[prevIdxi]

	// If there are things to send
	if r.RaftLog.LastIndex() > r.Prs[to].Match { 
		entriesptrs := make([]*pb.Entry, 0)
		for i := range r.RaftLog.entries[r.Prs[to].Next - offset : ] {
			entriesptrs = append(entriesptrs, &r.RaftLog.entries[r.Prs[to].Next + uint64(i) - offset])
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: 	pb.MessageType_MsgAppend,
			To:      	to,
			From:    	r.id,
			Term:    	r.Term,
			LogTerm: 	prevLogEntry.Term,
			Index:   	prevLogEntry.Index,
			Entries: 	entriesptrs,
			Commit:  	r.RaftLog.committed,
		})
	} else { // Nothing to send
		r.msgs = append(r.msgs, pb.Message{
			MsgType: 	pb.MessageType_MsgAppend,
			To:      	to,
			From:    	r.id,
			Term:    	r.Term,
			LogTerm: 	prevLogEntry.Term,
			Index:   	prevLogEntry.Index,
			Commit:  	r.RaftLog.committed,
		})
	}
	return true
}

// Function called by leader when leader receives log entry proposal
func (r *Raft) handlePropose(m pb.Message) {
	// Your Code Here (2A).

	// If stale or there's nothing, drop
	if m.Term < r.Term || m.Entries == nil {
		return
	}

	// EDIT: If not leader save and return
	if r.State != StateLeader { 
		// panic("Non-leader receiving MsgPropose\n")
		if r.Lead == 0 {
			return
		}
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
		return
	} 

	// Assign term and index
	eInd := r.RaftLog.LastIndex()
	for i, entry := range m.Entries {
		entry.Term = r.Term
		entry.Index = eInd + uint64(i) + 1
	}

	// Append and broadcast
	r.AppendEntries(m.Entries)
	r.bcastAppend()
}

func (r *Raft) fhandleAppendEntries(m pb.Message) {
	// Only restart when incoming message has higher term
	// But always handles RPC
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	r.handleAppendEntries(m)
}
func (r *Raft) chandleAppendEntries(m pb.Message) {
	// Only handles AppendEntriesRPC when message term is at least as high as local term
	// Means another candidate had won the election
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	}
}
func (r *Raft) lhandleAppendEntries(m pb.Message) {
	// Leader receiving AppendEntriesRPC of same term is byzantine error
	// Only handles message if term is higher
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	}
}
// Function called to actually handle the content of AppendEntriesRPC
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	offset := r.RaftLog.entries[0].Index
	prevLogIdx := m.Index - offset

	// 1. If log doesn’t contain an entry at prevLogIndex : reply false with local last index
	if m.Index > r.RaftLog.LastIndex() {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      	m.From,
			From:    	r.id,
			Term:    	r.Term,
			Index:		r.RaftLog.LastIndex(),
			Reject:  	true,
		})
		return
	}

	// 2. If entry within bounds but term doesn't match prevLogTerm (§5.3)
	// EDIT : if prevLogIdx < uint64(len(r.RaftLog.entries)) &&
	if m.Index <= r.RaftLog.LastIndex() &&
       m.LogTerm != r.RaftLog.entries[prevLogIdx].Term {
        r.msgs = append(r.msgs, pb.Message{
            MsgType: 	pb.MessageType_MsgAppendResponse,
            To:      	m.From,
            From:    	r.id,
            Term:    	r.Term,
            Index:   	m.Index - 1,
            Reject:  	true,
        })
        return
    }

	// Sort entries
	sort.Slice(m.Entries, func(i, j int) bool {
		return m.Entries[i].Index < m.Entries[j].Index
	})

	// Append new entries
	logEntriesAppended := false
	if m.Index == r.RaftLog.LastIndex() {
		// if prevlogindex == lastlogindex, just append the whole thing
		for j := 0; j < len(m.Entries); j++ {
			r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
		}
		logEntriesAppended = true
	} else { 
		// if prevlogindex < lastlogindex,
		raftlogEntriesLen := uint64(len(r.RaftLog.entries))

		for i, mEntry := range m.Entries {
			// Current log array position (NOT actual log index)
			logIdx := mEntry.Index - offset

			// Case when two arrays still overlap
			if logIdx < raftlogEntriesLen {
				// And something doesn't match, start appending from here and replace the rest
				if r.RaftLog.entries[logIdx].Term != mEntry.Term {
					// Truncate raftlog entries to just before current entry
					r.RaftLog.entries = r.RaftLog.entries[ : logIdx]
					// Update stabled if truncated
					if r.RaftLog.stabled > r.RaftLog.entries[logIdx-1].Index {
						r.RaftLog.stabled = r.RaftLog.entries[logIdx-1].Index
					}
					// Append entries
					for j := i; j < len(m.Entries); j++ {
						r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
					}
					logEntriesAppended = true
					break
				}
			} else if logIdx == raftlogEntriesLen { 
				// New entry just at the end of existing entries, append all and done
				for j := i; j < len(m.Entries); j++ {
					r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
				}
				logEntriesAppended = true
				break
			}
		}
	}

	// Update commitIndex if new entries appended 
	// EDIT : != instead of ==
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())

		if logEntriesAppended {
			r.RaftLog.committed = min(m.Commit, m.Index)
		}
	}

	// Done
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  false,
	})
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {

	// Only handled by leader
	if r.id != r.Lead {
		if r.Lead == 0 {
			return
		}
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
	} 

	// For each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	r.Prs[m.From].Next = m.Index + 1
	// For each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	r.Prs[m.From].Match = m.Index

	// If accepted, update r.Prs, committed, and send update to everyone
	if m.Reject == false {
		// Only update committed if the last entry replicated belongs to the current term
		mIndexTerm, err := r.RaftLog.Term(uint64(m.Index))
		if err != nil {
			panic("m.Index out of bounds to be used as r.RaftLog.Term(uint64(m.Index))")
		}
		if mIndexTerm == r.Term {
			// Count replication
			var count int
			for _, progress := range r.Prs {
				if progress.Match >= m.Index {
					count++
				}
			}
			// If count is a majority and has increased, update committed and tell everyone
			if count >= len(r.Prs)/2 + 1 && r.RaftLog.committed < m.Index {
				r.RaftLog.committed = m.Index
				r.bcastAppend()
			}
		}
	} else {
		// If rejected retry (Next and Match updated based on message content)
		r.sendAppend(m.From)
	}
}

// Handles request to send heartbeat
func (r *Raft) handleBeat(m pb.Message) {
	// Only leader gets here
	if r.State != StateLeader {
		panic("Non-leader handling MsgBeat")
	}

	for p := range r.Prs {
		if p != r.id {
			r.sendHeartbeat(p)
		}
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).

	// Same as above but Entries is always empty
	r.msgs = append(r.msgs, pb.Message{
		MsgType: 	pb.MessageType_MsgHeartbeat,
		To:      	to,
		From:    	r.id,
		Term:    	r.Term,
		Commit: 	r.RaftLog.committed,
	})
}

// Handles HeartbeatRPC, called by both candidate and follower
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).

	/* From raft/doc.go
		When 'MessageType_MsgHeartbeatResponse' is passed to the leader's Step method, 
		the leader knows which follower responded.
	*/

	// Stale
	if m.Term < r.Term {
		return
	}

	// Become follower if
	if r.State == StateFollower || r.State == StateLeader {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
	} else if r.State == StateLeader {
		// Leader should not receive heartbeat unless from new leader
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		} else {
			return 
		}
	} else { 
		// Candidate becomes follower regardless, stale term taken care of above
		r.becomeFollower(m.Term, m.From)
	}

	// Reset heartbeat timeout
	r.electionElapsed = 0

	// EDIT: updated var from Index to Commit
	// Update committed and apply if m.Commit > r.RaftLog.committed 
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}

	// Respond with last log index
	r.msgs = append(r.msgs, pb.Message{
		MsgType: 	pb.MessageType_MsgHeartbeatResponse,
		To:      	m.From,
		From:    	r.id,
		Term:    	r.Term,
		Index: 		r.RaftLog.LastIndex(),
	})
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	/* From raft_test.go
		func TestCommitWithHeartbeat2AB(t *testing.T) {}
		TestCommitWithHeartbeat tests leader can send log
		to follower when it received a heartbeat response
		which indicate it doesn't have update-to-date log
	*/

	if r.State == StateLeader && m.Index < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

// Handles SnapshotRPC to install a snapshot
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).

	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	if m.Term < r.Term {
		return
	}

	// Raft
	prs := make(map[uint64]*Progress)
	if len(m.Snapshot.Metadata.ConfState.Nodes) > 0 {
		peers := m.Snapshot.Metadata.ConfState.Nodes
		for _, p := range peers {
			prs[p] = &Progress{}
		}
	}
	r.Prs = prs
		
	// Raftlog
	r.RaftLog.pendingSnapshot = m.Snapshot // don't apply here, save to pendingSnapshot and apply in handleReadyxxx
	r.RaftLog.applied = m.Snapshot.Metadata.Index
	r.RaftLog.committed = m.Snapshot.Metadata.Index
	r.RaftLog.stabled = m.Snapshot.Metadata.Index
	r.RaftLog.entries = []pb.Entry{
		{
			Index: 	m.Snapshot.Metadata.Index, 
			Term: 	m.Snapshot.Metadata.Term,
		},
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
