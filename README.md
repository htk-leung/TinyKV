# Building a KV Store on Raft

### Abstract

This project follows the TinyKV course to build a key-value storage system with the Raft consensus algorithm and a multi-version concurrency control(MVCC) layer based on Percolator. It is built with goals to understand the interaction/boundaries between consensus protocols and database internal operations. This gives context to distributed systems and database concepts, such as validity, isolation levels.

### 0. Outline

<ol start="1">
  <li>Motivation</li>
  <li>Introduction</li>
  <li>Implementation & correctness
    <ol start="0">
      <li><em>Architecture Overview</em></li>
      <li><em>MVCC & Spanner Comparison</em></li>
      <li><em>Raft</em></li>
    </ol>
  </li>
  <li>Tests & Accuracy</li>
  <li>Discussion & Conclusion</li>
</ol>

***Detailed Raft correctness reasoning in ```writeups/p2a.md```.*

### 1. Motivation

A desire to understand distributed systems and database internals motivated a series of trials involving different study methods. Reading textbooks and papers[1][2][3] offered foundational knowledge but remained rather theoretical and did not allow for in-depth understanding of implementation details. Later the study was extended by reading the codebase of existing database systems, including Spanner, Calvin and FoundationDB to study timestamping methods. However, the extensiveness of the codebases made it hard to focus as each function and each step inspires many questions. Eventually, implementing a distributed database system emerged as the best option, as TinyKV provides a structured framework for focused study. These studies were carried out under guidance of Professor Aurojit Panda.

### 2. Introduction

TinyKV is a simplified emulation of TiKV, a distributed, transactional key-value database management system (DBMS), with architecture stripped down to capture the essence of state-machine replication and multi-version concurrency control implementation[5]. At a high level, TinyKV takes requests from clients, duplicates requests to all servers, processes them, and returns results to clients.

Each section below implements a layer in this process. Skeleton code is provided to facilitate focused learning. Part 1 implements a standalone version of this framework as an introduction to database internal operations. Part 2 implements the Raft consensus protocol with storage persistence, compaction and snapshot capabilities. Part 3 implements re-confi guration and is skipped since the mechanism is overly complex for the timeframe of the course. Part 4 build atop Part 2 to add MVCC complexities. Section 3 describes implementation details in relation to guarantees for Raft and MVCC modules, section 4 a subset of tests the project passed, and section 5 lessons learned.

### 3. Implementation & Correctness

#### 3.0. Architecture Overview

Code base is organized into 2 main directories, ```kv``` and ```raft```. ```kv``` is the directory for all files in the database system, whereas ```raft``` only hosts files pertaining to the implementation of the Raft module.

```main.go``` is the starting point of each state machine, and is saved inside ```kv```. It sets up a ```gRPC``` server to listen for incoming requests from clients, and a Raft server that listens for RPCs for replication between state machines and state management. As such, each request from the client is first received by the gRPC server, then the corresponding database function is called, and replication through Raft executed.

#### 3.1. MVCC & Spanner Comparison

TinyKV adapts the principles from Percolator to implement MVCC with snapshot isolation. Percolator[6] is a system built by Google principally to solve the problem of incremental processing. Snapshot isolation is an isolation level that prevents dirty reads, non-repeatable reads, phantom reads and write-write-conflict, but does not protect against write skew[7].

In turn, Percolator precedes Spanner. All 3 of TinyKV, Percolator and Spanner model data storage after Bigtable, a key-value and wide-column store, but add timestamps to keys to implement multiversion storage[4][5][6]. Comparisons are made between TinyKV and Spanner to understand implementation details that led to TinyKV promising only MVCC with snapshot isolation and the other MVCC with strict serializability despite comparable data storage structure.

#### 3.1.1. Functions & Locking mechanism

TinyKV implements optimistic locking. Because of this approach, the design also requires functions to roll back commits when conflicts are detected. TinyKV provides the following methods to be called when gRPC receives a request from client:

- ```KvGet()``` - the read function
- ```KvPreWrite()``` - commit function 1, acquires locks, writes data to local database server and sends request to raft for replication
- ```KvCommit()``` - commit function 2, after entry is replicated and agreement reached, request is committed
- ```KvScan()``` - reads multiple values from database, selecting only the appropriate versions for each key
- ```KvBatchRollback()``` - removes locks held by the current transaction, delete values and write a rollback commit entry
- ```KvResolveLock()``` - inspects the given set of keys and decides to roll forward to roll back
- ```KvCheckTxnStatus()``` - checks for transaction timeout and removes expired locks

2 kinds of locks are in use in TinyKV. Type 1 is a latch for a state machine's local database server, each key has a separate latch. Acquiring the latch prevents concurrent access and modification of database values. Type 2 is a lock implemented as a database entry for each key in the shared, replicated database table across state machines. When a state machine "acquires" a lock for a particular key, it writes a lock entry into the database, which is replicated to other state machines. This ensures that all state machines agree which keys to not write to.

For simplicity, type 1 is referred to as a latch, and type 2 as a lock. Type 1 guards only the local server, whereas Type 2 is used to guarantee consistent access pattern across  replicas. In the following descriptions, "no locks acquired" means type 2 lock is not acquired.

#### 3.1.2 Transaction Execution

#### *3.1.2.1 Read-Only(RO) Transactions*

TinyKV and Spanner are equals in this regard. Both systems retrieve the latest committed value of a key earlier than the request timestamp. TinyKV has 1 read method, Get(), to be used by both read-only(RO) and read-write(RW) transactions, and no locks are implemented for Get(). Spanner also has a lock-free read method ReadOnlyTransaction::Read().[9] This method simply acquires locks for the local data items, then waits for the appropriate version to become readable. This ensures repeatable reads.

#### *3.1.2.2. Read-Write(RW) Transactions*

This is where the systems' promises diverge. TinyKV does not separate RO and RW transactions. It uses the same transaction module for both, and thus calls the same read function Get() for both kinds of transactions. Only Write() implements locks for each key to write to. This means that while reads are guaranteed to be up-to-date, multiple transactions can read from the same keys for computation, allowing write-skews with concurrent transactions.

Spanner, however, separates RO reads from RW reads. While RO reads do not acquire locks, RW reads do. This guarantees that no two transactions can read from the same keys if at least one is writing values, thereby preventing write skew. This is enforced by separating RO transactions from RW transactions into classes of their own. This guarantees strict serializability[10][11].

#### 3.1.3. Concurrency : Locks and Rollback

Both Spanner and TinyKV read-write transactions hold locks to all write keys from beginning to end. Spanner acquires locks before writing to database with a single call to commit[11]. TinyKV acquires locks before pre-writing data to the database which are only released after writes are committed. When state machines run normally, there should be no write conflict.

However, in the event that state machines crash, or are suspected to have crashed due to network delay, locks acquired and incomplete commits need to be rolled forward or rolled back. For Spanner, this can only occur if the state machine crashes mid-function. For TinyKV, because the commit process is split into 2 function calls, the window for incomplete commit is much bigger. The functions ```KvBatchRollback()```, ```KvResolveLock()```, and ```KvCheckTxnStatus()``` are implemented to handle such incidents to prevent crashed processes from holding onto locks indefi nitely and leaving uncommitted writes in the database.

#### 3.2. Raft

Data is replicated to ensure fault-tolerance, and TinyKV implements the Raft consensus algorithm. The Raft module is treated as a black box inside each DBMS state machine, where it must guarantee validity and agreement to ensure requests passed in remain unchanged and consistent between replications. Raft algorithm guarantees linearizability.

In addition to the logic outlined in the Ongaros paper[8], TinyKV also implements the KV server with support of data persistence to disk memory, compaction and snapshot capabilities. Part 2A implements the Raft algorithm, 2B persistence to memory, and 2C compaction and snapshotting.

#### 3.2.1 Raft Command Proposal

The ```Write()``` and ```Reader()``` functions in RaftStorage serve as the connections between the MVCC and Raft layer. Each function calls ```SendRaftCommand()``` to send a request of type ```message.MsgTypeRaftCmd``` to leader's ```RaftWorker```, which is the receiver of gRPCs, and thus the starting point of Raft. Each time an RPC is received, leader ```RaftWorker``` first creates a new ```peerMsgHandler```. It then calls ```HandleMsg()``` to decide how to handle which type of message. Messages are then "unpackaged" by removing headers and passed into the Raft module by calling ```Propose()```. Such a process is illustrated in (*fig1*).

Only the leader will ever receive or propose new entries. When it proposes new entries, the leader assigns each request a term and an index that increments at each assignment. This ensures the uniqueness of the timestamp(index) assigned to a particular log entry.

Messages containing log entry proposals and commands for Raft are passed into ```Step()``` for handling. 12 types of messages are defined to cover the needs of log entries handling and leader election. ```Step()``` identifies the incoming message type and calls corresponding handling functions. New log entries are received as ```MessageType_MsgPropose```, which triggers ```handlePropose()```. Entries from the leader are received as type ```MessageType_MsgAppend```, and triggers ```handleAppendEntries()```. Similarly, heartbeat triggers are received as ```MessageType_MsgBeat``` and triggers ```handleBeat()```. New log entries and new messages to be sent are saved locally before being processed. Once this happens, ```HandleMsg()``` returns and the proposal is complete.

#### 3.2.2 Proposal Handling

When ```HandleMsg()``` returns, ```RaftWorker``` calls ```HandleRaftReady()``` to handle changes in the log. It applies committed entries to the database, sends locally saved messages to the designated peer, updates local hardstate and softstate. This is where the followers receive the leader's locally saved proposed entries. As such, followers' functions begin when follower's ```RaftWorker``` receives a message of type ```message.MsgTypeRaftCmd```, with ```HandleMsg()``` calling ```onRaftMsg()```, which calls ```Step()``` without going through ```Propose()```(*fig2*).

The leader also handles callbacks to clients. Each time a new proposal is made, the corresponding callback information is saved in peer.proposals in the same order they are received by the leader. Therefore, after each entry is committed, response to client can be made through callback by matching the entry with the proposal record.

Multiple events are involved - leader receiving requests, leader sending out RPCs, leader waiting for responses from followers to update the committed index, followers listening for messages from leader and sending responses, and ```RaftWorker``` in each peer calling ```HandleMsg()``` and ```HandleRaftReady()``` to handle requests. However, since a single ```RaftWorker``` at each state machine handles a single "processing round", request-processing is a serial process in which what is being applied is determined by the changes the received message triggers. Other goroutines handle network I/O and message-sending concurrently at the boundary.

#### 3.2.3 Snapshot and Compaction

Compaction discards old log entries from storage that are no longer needed since they have already been applied to the state machine. This reduces memory usage but causes possible inconsistencies between replications. A leader will have problems pulling a slow follower up to speed if the required entries had been compacted. In this case, snapshots are sent to the follower to force consistency.

Snapshots are handled following the same pipeline as any other log entries, but snapshot files are sent via a different channel, and are saved separately from the log entry. Each Snapshot contains the term and index of an applied log entry as well as the full database state that corresponds to this applied log entry. Log entries are not included, however. They will be supplemented by subsequent ```AppendEntriesRPCs```.

Each snapshot must be checked for staleness before being saved as a ```pendingSnapshot```, by ensuring that the snapshot term is at least as great as, and index greater than, that of the last log entry saved locally.

#### 3.2.4 Leader election

Leader elections are also implemented through RPCs following steps outlined in the Ongaro paper[8], with messages being received, processed and responded to in the same pipeline described above. Since all actions involved stay within the Raft module, all code is in ```raft.go```.

The correctness of this part of the implementation largely falls on counting votes, recording log state of each follower in leader and updating commit, applied and stabled variables correctly.

### 4. Tests & Accuracy

TinyKV provides a comprehensive set of tests to verify correctness. Implementation has passed tests including leader election during timeout, multiple candidate resolution, leader returning to follower state if receives message of higher term, randomized timeout to minimize re-elections, ignoring stale entries, ignoring stale snapshots, state machine recovery from snapshot, read-only transactions, read-write transactions and rollbacks, and lock resolutions. Refer to Appendix for full list of tests.

Parts 1, 2A and 4 are straightforward, needing only to follow protocols outlined in papers. Debugging is difficult for 2B and 2C, when concurrency, partition and unreliable networks are tested, and subsequent changes could cause failures in previously passed tests. Debugging strategies include using print statements to read state changes between message processing, tracing index changes by stepping through code, and considering different network delay/crash scenarios. The only way to move forward in this part is to follow each state variable in Raft, commit, applied, stabled, etc, and apply each scenario covered in the code, including election, append entries, snapshot, compaction, and consider all factors including null entries, delayed message delivery, unsigned integer underflow, etc. Up till this point, performance is unstable and bugs show up occasionally.

### 5. Discussion & Conclusion

Implementing these layers lent a full view of DBMS internals in action, illuminating how various guarantees come to live in which parts of the codebase. By following the course, I learned to understand the input assumptions and output expectations for a function by tracing the functions up- and down-stream, map out codebase architecture by following data structures, and implement code following algorithms defined in academic papers. The process brought out details I was not aware of by reading algorithms and highlighted the factors to consider when reading code.

As a result of implementing TinyKV, reading Spanner's emulation code and seeing design intentions became a manageable task. It is a valuable learning experience that I highly recommend. However, part 3 and implementing a consensus algorithm from scratch remain goals to be achieved.

<br>

___

#### References

[1] M. T. Özsu and P. Valduriez, Principles of Distributed Database Systems, 4th ed. Cham: Springer, 2020.

[2] J. Zhou et al., "FoundationDB: A distributed key value store," ACM SIGMOD Record, vol. 51, no. 1, pp. 24–31, Mar. 2022.

[3] A. Thomson, T. Diamond, S.-C. Weng, K. Ren, P. Shao, and D. J. Abadi, "Calvin: Fast distributed transactions for partitioned database systems," in Proc. ACM International Conference on Management of Data (SIGMOD), Scottsdale, AZ, USA, May 2012, pp. 1–12.

[4] J. C. Corbett et al., "Spanner: Google's globally distributed database," ACM Transactions on Computer Systems, vol. 31, no. 3, pp. 1–22, Aug. 2013.

[5] Talent Plan, "TinyKV," GitHub. [Online]. Available: https://github.com/talent-plan/tinykv. [Accessed: May 8, 2026].

[6] D. Peng and F. Dabek, "Large-scale incremental processing using distributed transactions and notifi cations," in Proc. USENIX Symposium on Operating Systems Design and Implementation (OSDI), Vancouver, BC, Canada, Oct. 2010, pp. 251–264.

[7] H. Berenson, P. Bernstein, J. Gray, J. Melton, E. O'Neil, and P. O'Neil, "A critique of ANSI SQL isolation levels," in Proc. ACM International Conference on Management of Data (SIGMOD), New York, NY, USA, May 1995, pp. 1–10.

[8] D. Ongaro and J. Ousterhout, "In search of an understandable consensus algorithm," in Proc. USENIX Annual Technical Conference (ATC), Philadelphia, PA, USA, Jun. 2014, pp. 305–319.

[9] Google Cloud Platform, "read_only_transaction.cc," GitHub, commit 58be04a. [Online]. Available: https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/blob/58be04a36b5e95b427476c4f9210ddc6427607f1/backend/transaction/read_only_transaction.cc. [Accessed: May 8, 2026].

[10] Google Cloud Platform, "read_write_transaction.cc," cloud-spanner-emulator, GitHub, commit 58be04a. [Online]. Available: https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/blob/58be04a36b5e95b427476c4f9210ddc6427607f1/backend/transaction/read_write_transaction.cc. [Accessed: May 8, 2026].

[11] Google, "Transactions," Google Cloud Spanner Documentation. [Online]. Available: https://docs.cloud.google.com/spanner/docs/transactions. [Accessed: May 8, 2026].