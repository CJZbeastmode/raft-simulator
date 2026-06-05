Sure. The whole thing in one flow.

---

## The Problem

Before this change, on every rejection the leader did:
```
nextIndex[peer]--
```

If a follower is 100 entries behind, that's 100 heartbeat rounds × 100ms = 10 seconds to converge. Too slow.

---

## The Core Idea

Instead of the leader blindly guessing, the **follower tells the leader exactly where the divergence is**. The leader then jumps directly there.

---

## Follower Side — "here's where we disagree"

When the follower rejects, it fills in two hint fields:

**Case 1 — log too short:**
```
Follower log: [S, A, B]          length=3
Leader says:  PrevLogIndex=5     doesn't exist

ConflictTerm  = -1               signal: "I don't even have that index"
ConflictIndex = 3                "my log ends here, start from here"
```

**Case 2 — wrong term at PrevLogIndex:**
```
Follower log: [S, A, B, B, B]    indices 0-4, all term 2 from index 2
Leader says:  PrevLogIndex=4, PrevLogTerm=3   term mismatch

ConflictTerm  = 2                "the entry I have there is term 2"
ConflictIndex = 2                "term 2 starts at index 2"
```

The follower walks back to find where that conflicting term *starts*, so the leader can skip the entire term at once.

---

## Leader Side — "let me jump there"

**Case 1 — `ConflictTerm == -1`:**
```
Follower log is too short → just set nextIndex to where their log ends
nextIndex[peer] = ConflictIndex
```
No point sending entries before that — they don't have the preceding entries yet.

**Case 2 — `ConflictTerm != -1`:**

The leader searches its own log for that term:

```
Leader log: [S, A, A, C, C, C]
                        ^^^
                      term 3, indices 3-5

ConflictTerm=2, meaning follower has term 2 where leader has something else
Leader searches: does it have term 2?
```

- **Leader has that term** → set `nextIndex` to just past the leader's **last** entry of that term. Both sides agree up to there, so start sending from just after.

- **Leader doesn't have that term** → the follower has entries the leader never had. Jump directly to `ConflictIndex` — the follower needs to be overwritten starting from there.

---

## Why last occurrence matters

```
Leader log: [S, A2, B2, C2, D3, E3]
                 ^^^^^^^^^^^
                 term 2: indices 1,2,3

ConflictTerm = 2
```

If you stop at the **first** match (index 1), you set `nextIndex=2` — you'd resend B2, C2 unnecessarily.

If you find the **last** match (index 3), you set `nextIndex=4` — you jump directly to where term 2 ends and start sending from D3. One round trip instead of three.

---

## The net result

```
Before: 100 entries behind = 100 heartbeat rounds
After:  100 entries behind = 1-2 heartbeat rounds
```

The follower does the diagnostic work once, the leader jumps directly to the right spot.