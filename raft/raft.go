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
	"log"
	"math/rand"
	"time"

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
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout           int
	randomizedElectionTimeout int
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
	// Your Code Here (2A).
	if err := c.validate(); err != nil {
		log.Panic("config is invalid")
		return nil
	}
	var r *Raft = new(Raft)
	r.id = c.ID
	r.Term = 0
	r.Vote = 0
	r.RaftLog = newLog(c.Storage)
	r.Prs = map[uint64]*Progress{}
	r.State = 0
	r.votes = map[uint64]bool{}
	r.msgs = []pb.Message{}
	r.Lead = 0
	r.heartbeatTimeout = c.HeartbeatTick
	r.electionTimeout = c.ElectionTick
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.leadTransferee = 0
	r.PendingConfIndex = 0

	for _, id := range c.peers {
		r.Prs[id] = &Progress{Match: 0, Next: 0}
		r.votes[id] = false
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
// 递增逻辑时间，follower和
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State != StateLeader {
		r.electionTick()
	} else {
		r.heartbeatTick()
	}

}

func (r *Raft) heartbeatTick() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {

		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

// follower 和 candidate的逻辑时间推进函数，更新election elapsed
func (r *Raft) electionTick() {
	r.electionElapsed++
	// candidate electionTimeout也可再次发起选举，
	// 即上一次选举未成功，且当前也没有其他节点当选leader，所以继续选举
	if r.electionElapsed > r.randomizedElectionTimeout {
		r.electionElapsed = 0
		// TODO(shy):随机初始化electionTimeout
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.electionElapsed = 0

	rand.Seed(time.Now().UnixNano())
	r.randomizedElectionTimeout = r.electionTimeout + int(rand.Float32()*float32(r.electionTimeout))
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	for id := range r.votes {
		r.votes[id] = false
	}

	rand.Seed(time.Now().UnixNano())
	r.randomizedElectionTimeout = r.electionTimeout + int(rand.Float32()*float32(r.electionTimeout))

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.Lead = r.id

	lastIndex := r.RaftLog.LastIndex()
	for id, pr := range r.Prs {
		if id == r.id {
			pr.Match = lastIndex
		} else {
			pr.Match = 0
		}
		pr.Next = lastIndex + 1

		// 因为r.Prs和r.votes的key相同，都是集群节点id，所以可以直接使用
		r.votes[id] = false
	}

	noop := pb.Entry{Data: nil}
	// noop 这里只加入消息，之后bcast
	// prs 信息更新
	if err := r.appendEntry([]pb.Entry{noop}); err != nil {
		log.Panic("append no-op error")
	}
}

func (r *Raft) appendEntry(es []pb.Entry) error {
	if len(es) == 0 {
		return nil
	}
	if len(es) == 1 && es[0].Data == nil {
		// 说明为noop日志项
		lastIndex := r.RaftLog.LastIndex()
		es[0].Index = lastIndex + 1
		es[0].Term = r.Term
		return r.RaftLog.AppendEntries(es)
	}

	return nil
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.MsgType == pb.MessageType_MsgHup && r.State != StateLeader {
		r.becomeCandidate()
		r.bcastRequestVote()
		if len(r.Prs) == 1 {
			r.becomeLeader()
		}
		return nil
	}
	if m.MsgType == pb.MessageType_MsgBeat && r.State == StateLeader {
		r.bcastHeartbeat()
		return nil
	}
	if m.Term < r.Term {
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgHeartbeatResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
			})
		}
		return nil
	}
	if m.Term == r.Term {
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			msg := pb.Message{
				To:      m.From,
				From:    r.id,
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				Term:    r.Term,
			}
			if r.State == StateLeader || r.State == StateCandidate {
				msg.Reject = true
			} else {
				if r.Vote == None || (r.Vote != None && r.Vote == m.From) {
					r.Vote = m.From
					msg.Reject = false
				} else {
					msg.Reject = true
				}
			}
			r.msgs = append(r.msgs, msg)

		case pb.MessageType_MsgHeartbeat:
		case pb.MessageType_MsgAppend:
			if r.State == StateCandidate {
				r.becomeFollower(m.Term, m.From)
			}
			if r.Lead != m.From && r.Lead != None {
				log.Panic(" two leader in one term")
			}
			r.Lead = m.From
			r.electionElapsed = 0
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgHeartbeatResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
			})
		}
	}
	if m.Term > r.Term {
		r.Term = m.Term
		if (r.State == StateLeader || r.State == StateCandidate) && m.From != None {
			if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend {
				r.becomeFollower(m.Term, m.From)
			} else {
				r.becomeFollower(m.Term, None)
			}
		}
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			msg := pb.Message{
				To:      m.From,
				From:    r.id,
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				Term:    r.Term,
			}
			if r.logUpToDate(m.LogTerm, m.Index) {
				msg.Reject = false
				r.Vote = m.From
			} else {
				msg.Reject = true
			}
			r.msgs = append(r.msgs, msg)
		case pb.MessageType_MsgHeartbeat:
			r.Lead = m.From
			r.electionElapsed = 0
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgHeartbeatResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
			})
		}
	}

	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}
func (r *Raft) stepFollower(m pb.Message) error {

	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgHeartbeatResponse:
		r.votes[m.From] = true
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Reject {
			return nil
		}
		r.votes[m.From] = true
		if r.checkVoteResult() {
			r.becomeLeader()
		}
	}
	return nil
}

func (r *Raft) checkVoteResult() bool {
	count := 0
	for _, v := range r.votes {
		if v {
			count++
		}
	}
	return count >= (len(r.votes)/2 + 1)
}

func (r *Raft) logUpToDate(c_term uint64, c_index uint64) bool {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	if lastIndex == ^uint64(0) {
		lastIndex = 0
	}

	if c_term < lastTerm {
		return false
	}
	if c_term == r.Term {
		if c_index < lastTerm {
			return false
		}
	}
	return true
}

func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id == r.id {
			r.electionElapsed = 0
			r.Lead = id
			continue
		}
		msg := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			To:      id,
			From:    r.id,
			Term:    r.Term,
		}
		r.msgs = append(r.msgs, msg)
	}
}

func (r *Raft) bcastRequestVote() {

	for id := range r.Prs {
		if id == r.id {
			r.votes[id] = true
			r.Vote = r.id
			continue
		}

		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			From:    r.id,
			Term:    r.Term,
		}
		lastIndex := r.RaftLog.LastIndex()
		if lastIndex == ^uint64(0) {
			msg.Index = 0
		} else {
			msg.Index = lastIndex
		}
		msg.LogTerm, _ = r.RaftLog.Term(r.RaftLog.LastIndex())
		r.msgs = append(r.msgs, msg)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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
