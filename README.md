# Raft Simulator

A from-scratch implementation of the Raft consensus algorithm in Go, built through MIT 6.5840 (Distributed Systems) Labs 3A–3D. Covers leader election, log replication, persistence, and log compaction via snapshots.

## What is Raft

Raft is a consensus algorithm that allows a cluster of servers to agree on a sequence of values even in the presence of failures. It is designed to be equivalent to Paxos in fault tolerance but easier to understand and implement. A Raft cluster elects one leader who accepts all writes, replicates them to followers, and commits once a majority confirms.

## Labs completed

| Lab | Topic | What it implements |
|-----|-------|--------------------|
| 3A | Leader election | Randomised timeouts, RequestVote RPC, term management |
| 3B | Log replication | AppendEntries RPC, commit index, accelerated conflict resolution |
| 3C | Persistence | Crash recovery, persistent state via gob encoding |
| 3D | Snapshots | Log compaction, InstallSnapshot RPC, snapshot/state atomic save |

## Repo structure

```
raft-simulator/
├── Makefile
├── README.md
└── src/
    ├── labgob/        ← gob encoder wrapper (checks for unexported fields)
    ├── labrpc/        ← simulated RPC network (packet loss, delay, reorder)
    └── raft/
        ├── raft.go        ← consensus implementation
        ├── persister.go   ← disk persistence abstraction
        ├── config.go      ← test harness (cluster setup, chaos injection)
        └── util.go        ← debug logging
```

## Key design decisions

**Bitmask-free log indexing** — after snapshotting, absolute log indices are translated to array positions via `logIndex(abs)` and `lastLogIndex()` helpers, keeping the rest of the code free of off-by-one arithmetic.

**Accelerated conflict resolution** — on `AppendEntries` failure, followers return the conflicting term and its first index. The leader jumps backward in bulk rather than decrementing one entry at a time, reducing round trips on diverged logs.

**Serialised apply loop** — a single background goroutine drains `commitIndex → lastApplied`, sending `ApplyMsg` values on `applyCh`. Snapshots are queued as `pendingSnapshot` rather than sent inline, preventing races between snapshot delivery and normal log application.

**Atomic snapshot + state save** — `persistWithSnapshot()` writes Raft state and the snapshot in a single `persister.Save()` call so they can never fall out of sync across a crash.

## Running the tests

```bash
cd src/raft

# run all labs
go test -v -race ./...

# run a specific lab
go test -v -race -run 3A
go test -v -race -run 3B
go test -v -race -run 3C
go test -v -race -run 3D

# run repeatedly to catch rare races
go test -race -count 10 -run 3A
```

All tests pass under the `-race` detector.

## What comes next

This implementation is ported into [`raft-scheduler`](../raft-scheduler) as `internal/raft/`, where `labrpc` is replaced with a real TCP transport and `labgob` is replaced with standard `encoding/gob`. The scheduler layer sits above Raft and submits cron job commands via `Start()`, reading committed results from `applyCh`.