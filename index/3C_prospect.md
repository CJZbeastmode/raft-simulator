## 2C — Persistence

**What it is:** If a Raft server crashes and restarts, it needs to restore its state. 2C adds saving and loading of the three fields that must survive a crash: `currentTerm`, `votedFor`, and `log`.

---

## Sessions

### Session 11 — Implement `persist()` and `readPersist()` (1 session)

**Goal:** Encode and decode persistent state. Call `persist()` everywhere the persistent fields change.

The work:
- Implement `persist()` using `labgob` encoder — encode `currentTerm`, `votedFor`, `log`
- Implement `readPersist()` — decode them back on restart
- Find every place these three fields are mutated and add a `persist()` call after

The mutation sites are:
```
currentTerm changes  → startElection, RequestVote handler, AppendEntries handler, stale reply handling
votedFor changes     → RequestVote handler, anywhere you reset to -1
log changes          → Start(), AppendEntries append loop
```

This session is mostly mechanical but requires careful attention — missing even one call site causes flaky failures that are hard to trace.

---

### Session 12 — Debug and stress test (1 session)

**Goal:** Pass all 2C tests reliably.

```bash
go test -run 3C -count 20 2>&1 | grep -E "PASS|FAIL|---"
```

The hard test is `TestFigure8Unreliable2C` — it simulates an unreliable network with crashes and is specifically designed to catch missing `persist()` call sites. If anything is flaky, the log will tell you exactly which state wasn't being saved.

---

## Summary

| Session | Focus | Done when |
|---------|-------|-----------|
| 11 | `persist()`, `readPersist()`, all call sites | Code compiles, basic tests pass |
| 12 | Stress test, fix flaky failures | `go test -run 3C -count 20` clean |

**2 sessions total.** 2C is the shortest lab — the implementation is small but the discipline of finding every call site is what trips people up.