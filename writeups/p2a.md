## Project 2A

Project 2A implements the basic Raft algorithm. Algorithm is defined in the Ongaro paper, but implementation involves details not specified in the paper and must be reasoned about based on the algorithmic framework. The reasoning is the content of this writeup.

---

### RequestVoteRPC

---

#### Q : What states to consider?

- normal state with leader and followers
- 1 or more nodes become candidate
- 1 node becomes new leader, but hasn't updated all followers yet
- 1 node freshly becomes new leader and recognized by all nodes


---

#### Q : What requests can be lost and does not make Raft incorrect?

*Safety requirement:*

A log entry is only considered safe once it is replicated to a majority. If it never reached a majority before the old leader died, that **uncommitted entry can disappear, but that is not a Raft safety violation**. Clients must retry and the system may legitimately drop uncommitted work.

*Implications:*

Committed entries must survive leader failure.
Uncommitted entries on the old leader: may vanish.
Client-visible writes: should be retried until they succeed or fail definitively.


---

#### Q : Defined candidate behavior?

From paper section 5.2:

> "A candidate continues in this state until one of three things happens: 
> (a) it wins the election, 
> (b) another server establishes itself as leader, or 
> (c) a period of time goes by with no winner."


---

#### Q : What happens if an election fails? Can it go on forever? What if the candidate loses the election, but keeps trying?

*Causes:*

1. Timeout (network issues, it's isolated) 
If an election keeps failing due to timeout, then it just keeps trying and inflating the term
2. Stale log (So it just keeps trying blind?)
If an election keeps failing due to its stale log, it will keep trying and incrementing its term but can never be updated by the old leader. 

If only a minority doesn't hear from the leader, then yes they can be weird and keep hosting elections.

But both of these are technically safe and can continue in the normal mode up until the point where the majority of nodes become candidates, where the system definitely needs a new leader. Key is **quorum intersection**.

When the majority of nodes become candidates, due to the quorum-based policy one of the candidates must have the most updated log, and that node wins the election. This does not guarantee that the latest commit index has been updated in the followers that became candidates. If the latest commit index is up to date, all is good. If not, the new leader can just commit them in the new term.

---

#### Q : When an election fails, what term does the candidate have when it reverts to a follower?

It reverts to the current term, which is the term it was already in when it started that election. In Raft, a candidate increments its term when it becomes a candidate, and if the election fails it steps back to follower without rolling the term backward.

This prevents “time travel” in Raft, when a server forgets that it has already advanced to a newer term. That way, stale leaders and stale candidates cannot keep acting as if an older election was still valid. 

It does mean that some servers will remain useless until there is a higher term leader. Vote request is denied if its term is older than others, other have already voted for someone else, or its log is stale. In any case it should just wait. 

---

#### Q : What about term inflation?

When a follower becomes a candidate and sends RequestVoteRPC with a low term, the crazy nodes reject it and include their current term in the response. The new node updates to that term and retries. 

*From the perspective of network isolation:*

If a minority of nodes become candidates because of network isolation from leader only, the results depend on if they are still connected to a quorum of nodes. If not, then they can be ignored. Otherwise, if they have an updated log and RequestVoteRPC arrives before any more updates from the old leader, then one of them can become the new leader. But if not, then elections will keep failing and be handled as mentioned above.

*Because elections can fail, local term should NOT be updated by RequestVoteRPCs except for candidates.

---

#### Q : What happens when a RequestVoteRPC of higher term is received?

When a candidate receives a higher term request vote RPC, it reverts to follower. (1) What term does it revert into? However, there is no clear leader yet, and the higher term election may fail. (2) Also, if it were to forward MsgPropose, it does not know whom to forward it to. (3) If it saves it locally, then forward to leader when there is a leader, then what prevents the client from sending another proposal because it hasn't heard back and thus duplicating the request? 

1. it takes the term of the incoming RequestVoteRPC
1. if there is no leader, then drop the message. Only forward when there is a leader
1. it is a known issue called client retry deduplication, and it is explicitly out of scope for the core Raft protocol.

#### Q : What happens in the interim between RequestVoteRPC and the first heartbeat?

*Follower:*

- Appends entries as usual

*Leader :*

- After leader receives RequestVoteRPC, if the term is higher than local term then it immediately becomes a follower
- When a follower receives proposal it is forwarded to the leader, but there's a while when it wouldn't know who the leader is.
- In this case, the lab from class solves it by keeping resending it to itself until there is a leader. Otherwise, it can be buffered and forwarded to the new leader when the new heartbeat comes in.

*Candidate:*

- if minority vote request arrives after all AErpcs, then election fails
- if minority vote request arrives before all AErpcs, election succeeds, but new entries from old leader are ignored and lost, client needs to resend
- if majority vote request arrives after all AErpcs, then election fails
- if majority vote request arrives before all AErpcs, then election succeeds but no committed entries lost

---

#### Q : What needs to be marked/changed when a RequestVoteRPC is received?

If each peer can only vote for a term once, and a delusional(well, isolated) peer cut off from the leader is recklessly incrementing terms and sending out requestvoteRPC, then nobody else can win subsequent elections? 

No, because :

1. if new AppendEntryRPC arrived before RequestVoteRPC, his log will be stale and request will be rejected.

2. if new AppendEntryRPC arrives after RequestVoteRPC, then it may succeed. But as long as it is connected to a quorum where nobody has a more updated log then it does not violate safety. If leader has more entries but are not replicated to a quorum, then it's not committed and there are no guarantees

---

#### Q : Therefore, when should a follower change term?

1. when receiving RequestVoteRPC >> NO.
2. when receiving first heartbeat from new leader >> YES

---

#### Actions after receiving RequestVoteRPC:

1. check log and vote
1. local term should NOT be updated by RequestVoteRPCs except for candidates
1. leader keeps processing proposals
1. leader only becomes follower and followers update term and leader when higher-term heartbeat received, meaning new leader has been elected. 
1. after election succeeds the old leader may still receive proposals because client hasn't been updated yet. In this case forward proposal to new leader.
Otherwise, a follower should not be receiving proposals


---

### AppendEntriesRPC

---

#### How to tell from where to start appending entries?

Raft guarantees that each term only has 1 leader with an authoritative log, so entries within the same term must be identical. But if entries are not committed, they can be overwritten by incoming entries of a higher term. This means that we only need to check for term equivalence to tell if log entries match.

