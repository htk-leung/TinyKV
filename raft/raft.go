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
	"errors"
	"bytes"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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

	// votes records << in this term this other server voted for this candidate
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
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

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}


	// Your Code Here (2A).

	return &Raft{
		id: c.ID,
		RaftLog: &RaftLog{
			storage: c.Storage,
			applied: c.Applied,
		},
		State:            StateFollower,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
}

// sendAppend is called by leader through bcastappend to send an append RPC with new entries (if any) and
// the current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	/* 	from doc.go
	If you need to send out a message, just push it to raft.Raft.msgs and
	all messages the raft received will be passed to raft.Raft.Step()
	*/
	if to != r.Lead {
		return false
	}

	if r.RaftLog.committed > r.RaftLog.applied {
		entriesptrs := make([]*pb.Entry, r.RaftLog.committed - r.RaftLog.applied)
		for _, entry := range r.RaftLog.entries[r.RaftLog.applied+1 : ] {
			entriesptrs = append(entriesptrs, &entry)
		}

		r.msgs = append(r.msgs, pb.Message{
			MsgType: 	pb.MessageType_MsgAppend,
			To:      	to,
			From:    	r.id,
			Term:    	r.Term,
			LogTerm: 	r.RaftLog.entries[r.RaftLog.applied].Term, // prevLogTerm - term of prevLogIndex entry
			Index:   	r.RaftLog.applied,                         // prevLogIndex - index of log entry immediately preceding new ones
			Entries: 	entriesptrs,
			Commit:  	r.RaftLog.committed, // committed must be updated before calling sendappend
		})
	} else {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: 	pb.MessageType_MsgAppend,
			To:      	to,
			From:    	r.id,
			Term:    	r.Term,
			LogTerm: 	r.RaftLog.entries[r.RaftLog.applied].Term,
			Index:   	r.RaftLog.applied,
			Commit:  	r.RaftLog.committed,
		})
	}

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).

	// Same as above but Entries is always empty
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: r.RaftLog.entries[r.RaftLog.applied].Term,
		Index:   r.RaftLog.applied,
		Commit:  r.RaftLog.committed,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).

	// If leader advance heartbeatElapsed
	if r.State == StateLeader {
		r.heartbeatElapsed++
		return
	}
	// Advance electionElapsed in any other role
	r.electionElapsed++
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).

	r.State = StateFollower
	r.Term = term
	r.Lead = lead

	r.Vote = 0
	r.votes = make(map[uint64]bool) // when leader/candidates become followers a new term started >> don't casually call becomeFollower
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).

	// transitions to candidate state
	r.State = StateCandidate
	// follower increments its current term
	r.Term++
	// votes for itself and
	r.Vote = r.id
	r.votes[r.id] = true
	// reset electionTimeout
	r.electionElapsed = 0
	// issues RequestVote RPCs in parallel to each of the other servers in the cluster.
	for p := range r.Prs {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      p,
			From:    r.id,
			Term:    r.Term,
			LogTerm: r.RaftLog.entries[r.RaftLog.committed].Term,
			Index:   r.RaftLog.applied, // should this be committed or applied?
		})
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	// update state
	r.State = StateLeader
	r.Vote = 0
	r.votes = make(map[uint64]bool)
	r.Lead = r.id
	r.heartbeatElapsed = 0

	// propose noop entry = new empty entry in log
	entries := make([]*pb.Entry, 1)
	entries = append(entries, &pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     0,
	})
	r.appendEntries(entries)

	// broadcast
	r.bcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	switch r.State { // case if statemachine in this state receives this type of msg
	case StateFollower:

		switch m.MsgType {

		// MessageType_MsgHup >> start new election
		case pb.MessageType_MsgHup:
			// issued when there is an electionTimeout, server starts campaign
			r.campaign(m)

		// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
		// of the 'MessageType_MsgHeartbeat' type to its followers.
		case pb.MessageType_MsgBeat:
			// if a follower somehow gets this message it means that this server was a leader but had stepped down
			// and either the server hadn't updated its info or the message was delayed
			// forward to leader
			r.handleBeat(m)

		// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
		case pb.MessageType_MsgPropose:
			// normally recevied by leader
			// if a follower receives it maybe some server is mistaken
			// save to msgs to be forwarded to leader later
			m.From = r.id
			m.To = r.Lead
			r.msgs = append(r.msgs, m)

		// 'MessageType_MsgAppend' >> AppendEntries RPC
		case pb.MessageType_MsgAppend:
			// message sent with entries to append to log, call function to save entries
			r.handleAppendEntries(m)

		// 'MessageType_MsgAppendResponse' >> AppendEntries RPC
		case pb.MessageType_MsgAppendResponse:
			// MsgAppend is sent from leader to follower, and response from follower to leader
			// doc.go: 	When 'MessageType_MsgAppend' is passed to candidate or follower's Step method, it responds by
			// 			calling 'handleAppendEntries' method, which sends 'MessageType_MsgAppendResponse' to raft mailbox.
			r.handleAppendEntries(m)

		// 'MessageType_MsgRequestVote' >> RequestVoteRPC
		case pb.MessageType_MsgRequestVote:
			r.fHandleRequestVote(m)

		// 'MessageType_MsgRequestVoteResponse' >> RequestVoteRPC
		case pb.MessageType_MsgRequestVoteResponse:
			// if a follower somehow gets a response it means the candidate lost the election and has become a follower again
			// vote is obsolete, ignore

		// 'MessageType_MsgSnapshot' requests to install a snapshot message.
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)

		// 'MessageType_MsgHeartbeat' >> AppendEntries RPC
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)

		// 'MessageType_MsgHeartbeatResponse' >> AppendEntries RPC
		case pb.MessageType_MsgHeartbeatResponse:
			// normally received by leader, forward to leader
			r.handleHeartbeat(m)

		// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
		case pb.MessageType_MsgTransferLeader:

		// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
		// the transfer target timeout immediately and start a new election.
		case pb.MessageType_MsgTimeoutNow:

		}

	case StateCandidate:

		switch m.MsgType {
		// MessageType_MsgHup : condidate continues to be candidate
		case pb.MessageType_MsgHup:
			// if cand receives this it means there's election timeout during election, start again
			r.campaign(m)

		// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
		// of the 'MessageType_MsgHeartbeat' type to its followers.
		case pb.MessageType_MsgBeat:
			// not leader, no leader to forward to, ignore

		// 'MessageType_MsgPropose' >> appendEntry RPC from server to leader
		case pb.MessageType_MsgPropose:
			// doc.go: When passed to candidate, 'MessageType_MsgPropose' is dropped.

		// 'MessageType_MsgAppend' >> AppendEntries RPC
		case pb.MessageType_MsgAppend:
			// received from leader, means someone is elected
			// become follower and append entries
			r.handleAppendEntries(m)

		// 'MessageType_MsgAppendResponse' >> AppendEntries RPC
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntries(m)

		// 'MessageType_MsgRequestVote' >> RequestVoteRPC
		case pb.MessageType_MsgRequestVote:
			r.clHandleRequestVote(m)

		// 'MessageType_MsgRequestVoteResponse' >> RequestVoteRPC
		case pb.MessageType_MsgRequestVoteResponse:
			// only a candidate needs to handle the response
			r.handleRequestVoteResponse(m)

		// 'MessageType_MsgSnapshot' requests to install a snapshot message. << (2C)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)

		// 'MessageType_MsgHeartbeat' >> AppendEntries RPC
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)

		// 'MessageType_MsgHeartbeatResponse' >> AppendEntries RPC
		case pb.MessageType_MsgHeartbeatResponse:
			// technically should not receive this
			// but if it does,

		// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
		case pb.MessageType_MsgTransferLeader:
		// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
		// the transfer target timeout immediately and start a new election.
		case pb.MessageType_MsgTimeoutNow:
		}

	case StateLeader:

		switch m.MsgType {
		// MessageType_MsgHup
		case pb.MessageType_MsgHup:
			// ignore

		// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
		// of the 'MessageType_MsgHeartbeat' type to its followers.
		case pb.MessageType_MsgBeat:
			for p := range r.Prs {
				r.sendHeartbeat(p)
			}

		// 'MessageType_MsgPropose' >> AppendEntries RPC from server to leader
		case pb.MessageType_MsgPropose:
			// calls appendEntry to append to entries
			// calls bcastAppend to call sendAppend
			r.handleAppendEntries(m)

		// 'MessageType_MsgAppend' >> AppendEntries RPC
		case pb.MessageType_MsgAppend:
			// should only be sent by leader, ignore
			// unless term is higher? then become follower and respond
			r.handleAppendEntries(m)

		// 'MessageType_MsgAppendResponse' >> AppendEntries RPC
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendEntriesResponse(m)

		// 'MessageType_MsgRequestVote' >> RequestVoteRPC
		case pb.MessageType_MsgRequestVote:
			r.clHandleRequestVote(m)

		// 'MessageType_MsgRequestVoteResponse' >> RequestVote RPC
		case pb.MessageType_MsgRequestVoteResponse:
			// if a leader somehow gets a response it means the candidate has won and the response has no use
			// ignore

		// 'MessageType_MsgSnapshot' requests to install a snapshot message.
		case pb.MessageType_MsgSnapshot:
		// 'MessageType_MsgHeartbeat' >> AppendEntries RPC
		case pb.MessageType_MsgHeartbeat:
		// 'MessageType_MsgHeartbeatResponse' >> AppendEntries RPC
		case pb.MessageType_MsgHeartbeatResponse:
		// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
		case pb.MessageType_MsgTransferLeader:
		// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
		// the transfer target timeout immediately and start a new election.
		case pb.MessageType_MsgTimeoutNow:

		}
	}
	return nil
}

// campaign method used to kickstart an election campaign for a follower
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

	// node becomes candidate
	r.becomeCandidate()
	// sends 'MessageType_MsgRequestVote' to peers
	for p := range r.Prs {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      p,
			From:    r.id,
			Term:    r.Term,
			LogTerm: r.RaftLog.entries[r.RaftLog.committed].Term, // term of candidate’s last log entry (§5.4)
			Index:   r.RaftLog.committed,                         // index of candidate’s last log entry (§5.4)
		})
	}
}
func (r *Raft) clHandleRequestVote(m pb.Message) {
	/* from raft/doc.go
	When passed to the leader or candidate's Step method if:
	- message's Term is lower than leader's or candidate's,
	'MessageType_MsgRequestVote' will be rejected
	('MessageType_MsgRequestVoteResponse' is returned with Reject true).
	- receives 'MessageType_MsgRequestVote' with higher term, it will
	revert back to follower.
	*/
	/* from Raft paper
	Results:
	term			currentTerm, for candidate to update itself
	voteGranted		true means candidate received vote
	*/

	if r.State != StateCandidate && r.State != StateLeader {
		return
	}

	// become follower if term is higher, handle as follower
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		r.fHandleRequestVote(m)
		return
	}

	// A. 	reject if term is smaller
	// B. 	if terms are the same msg is received from a fellow competitor
	// 		candidate : it's coming from its competition, but a candidate had voted for itself, so reject vote
	// 		leader : means this server won the election and the sender lost. But leader should have sent heartbeat. ignore
	if m.Term < r.Term || r.State == StateCandidate {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
	}
}
func (r *Raft) fHandleRequestVote(m pb.Message) {
	/* from raft/doc.go
	When 'MessageType_MsgRequestVote' is passed to follower, it
	- votes for the sender only when
	- sender's last term is greater than MessageType_MsgRequestVote's term or
	- sender's last term is equal to MessageType_MsgRequestVote's term but sender's last committed
	index is greater than or equal to follower's.
	*/
	/* from raft paper
	voter denies its vote if its own log is more up-to-date than that of the candidate.
	Raft determines which of two logs is more up-to-date
	by comparing the index and term of the last entries in the
	logs. If the logs have last entries with different terms, then
	the log with the later term is more up-to-date. If the logs
	end with the same term, then whichever log is longer is
	more up-to-date.

	Receiver implementation:
	1. 	Reply false if term < currentTerm (§5.1)
	2. 	If votedFor is null or candidateId, and candidate’s log is at
		least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

	Also :
	- each server can only vote for 1 candidate per term
	*/

	// which term is it referring to?
	// log term or leadership term?
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
	}

	// if candidate log is *at least as* up to date as the local log
	var candUpToDate bool

	// if different last term, later term is more up to date
	localLogInd := r.RaftLog.committed
	if m.LogTerm != r.RaftLog.entries[localLogInd].Term {
		candUpToDate = m.LogTerm > r.RaftLog.entries[localLogInd].Term

	} else { // if same last term, larger entry index is more up to date
		candUpToDate = m.Index >= localLogInd
	}

	// if votedFor is null or candidateId then it can still vote
	var votedForValid bool

	// voter must not have voted yet
	// if it has then it should have voted for candidate
	votedForValid = (r.Vote == 0 || r.Vote == m.From)

	// return
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  candUpToDate && votedForValid,
	})
}
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	/*	from raft/doc.go
		Candidate
		- calculates how many votes it has won
		- if it's more than majority (quorum),
			- it becomes leader and
			- calls 'bcastAppend'.
		- if candidate receives majority of votes of denials, it
			- reverts back to follower.
	*/

	// save response, invert response so now true = note granted
	result := !m.Reject

	// add to map if not exist yet
	_, ok := r.votes[m.From]
	if !ok {
		r.votes[m.From] = result // when becomes leader clear slice
	}

	// count to see where we are now
	var votedFor, votedAgainst, quorum int

	for _, vote := range r.votes {
		if vote == true {
			votedFor++
		}
	}
	votedAgainst = len(r.votes) - votedFor
	quorum = len(r.Prs)/2 + 1

	if votedFor >= quorum {
		r.becomeLeader()
	} else if votedAgainst >= quorum {
		r.becomeFollower(r.Term, r.Lead) // update again when first heartbeat from leader received
	}
	// else do nothing
}
func (r *Raft) appendEntries(entries []*pb.Entry) {
	// append
	for _, entry := range entries {
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		r.RaftLog.committed++
	}
	// anything else to update?
}

// broadcast append
func (r *Raft) bcastAppend() {
	/*
		'MessageType_MsgPropose' proposes to append data to its log entries. This is a special
		type to redirect proposals to the leader. Therefore, send method overwrites
		eraftpb.Message's term with its HardState's term to avoid attaching its
		local term to 'MessageType_MsgPropose'. When 'MessageType_MsgPropose' is passed to the leader's 'Step'
		method, the leader first calls the 'appendEntry' method to append entries
		to its log, and then calls 'bcastAppend' method to send those entries to
		its peers. When passed to candidate, 'MessageType_MsgPropose' is dropped. When passed to
		follower, 'MessageType_MsgPropose' is stored in follower's mailbox(msgs) by the send
		method. It is stored with sender's ID and later forwarded to the leader by
		rafthttp package.

		'MessageType_MsgAppend' contains log entries to replicate. A leader calls bcastAppend,
		which calls sendAppend, which sends soon-to-be-replicated logs in 'MessageType_MsgAppend'
		type. When 'MessageType_MsgAppend' is passed to candidate's Step method, candidate reverts
		back to follower, because it indicates that there is a valid leader sending
		'MessageType_MsgAppend' messages. Candidate and follower respond to this message in
		'MessageType_MsgAppendResponse' type.

		A.
		- leader receives proposal to append entries to log
		- leader adds entries
		- leader calls bcastappend to send entries to peers

		'MessageType_MsgRequestVoteResponse' contains responses from voting request. When 'MessageType_MsgRequestVoteResponse' is
		passed to candidate, the candidate calculates how many votes it has won. If
		it's more than majority (quorum), it becomes leader and calls 'bcastAppend'.
		If candidate receives majority of votes of denials, it reverts back to
		follower.

		B.
		- candidate becomes leader
		- leader calls bcastappend to send entry to peers

		'MessageType_MsgSnapshot' requests to install a snapshot message. When a node has just
		become a leader or the leader receives 'MessageType_MsgPropose' message, it calls
		'bcastAppend' method, which then calls 'sendAppend' method to each
		follower. In 'sendAppend', if a leader fails to get term or entries,
		the leader requests snapshot by sending 'MessageType_MsgSnapshot' type message.

		>> always sent from leader to followers to ask followers to append entries
		>> assumes entries already in r.RaftLog.entries
	*/

	for p := range r.Prs {
		r.sendAppend(p)
	}
}

// handleAppendEntries handles AppendEntries RPC request
// called by follower & candidate
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	/*	from Raft paper
		Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
		input m Arguments:
			term			leader’s term
			leaderId		so follower can redirect clients
			prevLogIndex 	index of log entry immediately preceding new ones
			prevLogTerm		term of prevLogIndex entry
			entries[]		log entries to store (empty for heartbeat;
							may send more than one for efficiency)
			leaderCommit	leader’s commitIndex
		Results:
			term		currentTerm, for leader to update itself
			success		true if follower contained entry matching
						prevLogIndex and prevLogTerm
		Receiver implementation:
		1. Reply false if term < currentTerm (§5.1)
		2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		3. If an existing entry conflicts with a new one (same index
		   but different terms), delete the existing entry and all that
		   follow it (§5.3)
		4. Append any new entries not already in the log
		5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	*/
	// assume m.entries have increasing index
	// first entry should have smallest index

	// detect that it's forwarding a response by type
	if m.MsgType == pb.MessageType_MsgAppendResponse {
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
		return
	}

	// Reply false if term < currentTerm (§5.1)
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: 	pb.MessageType_MsgAppendResponse,
			To:      	m.From,
			From:    	r.id,
			Term:    	r.Term,
			Reject:  	true,
		})
		return
	} else if m.Term >= r.Term && (r.State == StateLeader || r.State == StateCandidate) { 
		r.becomeFollower(m.Term, m.From)
	} else if m.Term > r.Term && r.State == StateFollower {
		// update term for follower only when term has changed
		r.becomeFollower(m.Term, m.From)
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if (r.RaftLog.committed < m.Index) || r.RaftLog.entries[m.Index].Term != m.LogTerm {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}

	// append new entries to log. How to find from where to start:
	// if entry exists && everything matches, check next
	var matches bool
	var rEndInd, mStartInd, i uint64

	for _, mEntry := range m.Entries {
		// reset
		matches = false

		// if entry exists && everything matches
		if r.RaftLog.committed >= mEntry.Index {
			if r.RaftLog.entries[mEntry.Index].EntryType == mEntry.EntryType &&
				r.RaftLog.entries[mEntry.Index].Term == mEntry.Term &&
				r.RaftLog.entries[mEntry.Index].Index == mEntry.Index &&
				bytes.Equal(r.RaftLog.entries[mEntry.Index].Data, mEntry.Data) {
				matches = true // mark match
			}
		}
		// and check until record doesn't match
		// record i to start appending from here
		if matches == false {
			rEndInd = mEntry.Index
			mStartInd = i
			break
		}
		i++
	}
	// entry doesn't exist / doesn't match, remove entries from that entry onwards
	r.RaftLog.entries = r.RaftLog.entries[:rEndInd]

	// append new ones
	var rLastInd uint64
	for _, mEntry := range m.Entries[mStartInd:] {
		r.RaftLog.entries = append(r.RaftLog.entries, *mEntry)
		rLastInd = mEntry.Index
	}

	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, rLastInd)
	}

	// done
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   rLastInd,
		Reject:  false,
	})
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	// if successful
	// update nextindex and matchindex for follower
	// >> progress struct Prs map[uint64]*Progress
	if m.Reject == false {
		// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
		r.Prs[m.From].Next = m.Index + 1
		// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
		r.Prs[m.From].Match = m.Index
	} else {
		// if rejected decrement nextIndex and retry
		// this means that the reply needs to carry the same entries as the request? Only if rejected? No because leader has raw info.
		// details to be carried out in sendAppend
		r.sendAppend(m.From)
	}
}

// handle request to send heartbeat
func (r *Raft) handleBeat(m pb.Message) {
	// if follower, forward to leader
	// if candidate -- nothing could be done because election is under way and there is no leader?
	// if leader -- send heartbeat
	m.To = r.Lead
	r.msgs = append(r.msgs, m)
}

// handleHeartbeat handle Heartbeat RPC request
// function called by both candidate and follower
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).

	/* 	raft/doc.go
	'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'. When 'MessageType_MsgHeartbeatResponse'
	is passed to the leader's Step method, the leader knows which follower responded.
	*/

	// if message is stale ignore
	if m.Term < r.Term {
		return
	}
	// if we are forwarding response message
	if m.MsgType == pb.MessageType_MsgHeartbeatResponse {
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
	}
	// message should only be heartbeat from here
	if m.MsgType != pb.MessageType_MsgHeartbeat {
		return
	}

	// becomeFollower updates Term and Lead
	// there cannot be >1 leader per term, so only update when m.Term > r.Term
	// but candidate should become follower if heartbeat received >> but still, only when m.Term > r.Term
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	// reset election timeout
	r.electionElapsed = 0

	// return
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    m.To,
		Term:    r.Term,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
