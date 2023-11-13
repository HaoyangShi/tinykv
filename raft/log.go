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

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("Storage must not be nil")
	}
	var l *RaftLog = new(RaftLog)
	l.storage = storage

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		log.Panic("Failed to get storage firstIndex")
	}
	// lastIndex为logical_index
	lastIndex, err := storage.LastIndex()
	if err != nil {
		log.Panic("Failed to get storage lastIndex")
	}

	l.pendingSnapshot = &pb.Snapshot{}

	stable_ents, _ := storage.Entries(firstIndex+1, lastIndex+1)
	l.entries = []pb.Entry{}
	l.entries = append(l.entries, stable_ents...)

	offset := uint64(0)
	raftLog_storage_firstIndex := uint64(0)
	raftLog_storage_lastIndex := uint64(0)
	if len(stable_ents) != 0 {
		offset = stable_ents[0].Index
		raftLog_storage_firstIndex = stable_ents[0].Index
		raftLog_storage_lastIndex = stable_ents[len(stable_ents)-1].Index
	}

	// firstIndex 和lastIndex都是logical index，基于ms.entries[0].Index
	l.committed = raftLog_storage_firstIndex - 1 - offset // committed为physical, 根据nextEntries判断
	l.applied = raftLog_storage_firstIndex - 1 - offset   //applied为physical，根据nextEntries判断
	l.stabled = raftLog_storage_lastIndex                 // stabled为logical

	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
	// return nil
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// l.stabled 为logical
	// Your Code Here (2A).
	offset := uint64(0)
	if len(l.entries) != 0 {
		offset = l.entries[0].Index
	}
	phy_stabled := l.stabled - offset

	if phy_stabled+1 <= (uint64(len(l.entries))) {
		return l.entries[phy_stabled+1:]
	} else if l.stabled == ^uint64(0) {
		return l.entries
	}
	return nil
	// return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.applied == l.committed {
		return nil
	}
	return l.entries[l.applied+1 : l.committed+1]
	// return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return uint64(len(l.entries) - 1)
	// return 0
}

// Term return the term of the entry in the given index
// Term中传入的是physical index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return ^uint64(0), nil
	}
	if i >= uint64(len(l.entries)) {
		return ^uint64(0), nil
	}
	return l.entries[i].Term, nil
	// return 0, nil
}

func (l *RaftLog) AppendEntries(es []pb.Entry) error {
	esFirstIndex := es[0].Index
	if len(l.entries) == 0 {
		l.entries = append(l.entries, es...)
	} else {
		offset := l.entries[0].Index
		physical_lastIndex := l.LastIndex()
		// logical_lastIndex := l.entries[physical_lastIndex].Index
		physical_esFirstIndex := esFirstIndex - offset

		physical_match_index := physical_esFirstIndex - 1
		es_match_index := -1
		for _, ent := range es {
			phy_ent_index := ent.Index - offset
			l_term, _ := l.Term(phy_ent_index)
			if l_term == ^uint64(0) || l_term != ent.Term {
				break
			} else {
				physical_match_index++
				es_match_index++
			}
		}
		left_length := physical_esFirstIndex + uint64(len(es)) - physical_match_index - 1
		if left_length == 0 {
			return nil
		}

		if physical_esFirstIndex > physical_lastIndex+1 {
			return errors.New("entry hole")
		}

		if physical_match_index == ^uint64(0) && l.stabled != ^uint64(0) {
			l.stabled = ^uint64(0)
		} else if physical_match_index+offset < l.stabled {
			l.stabled = physical_match_index + offset
		}

		l.entries = append(l.entries[:physical_match_index+1], es[es_match_index+1:]...)
	}
	return nil
}
