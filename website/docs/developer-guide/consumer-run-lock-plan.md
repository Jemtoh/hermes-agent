---
title: Consumer run lock — revised spec and plan
---

# Consumer run lock

Status: independent Hermes audit PASS for the B1 prerequisite, 9 October 2026. Codex orchestrates
and reviews; Hermes implements. Supersedes B1 in the [blocked runtime proposal](./fundraising-runtime-extension-plan.md).
Source goal: serialize preflight → agent → saved report for scheduled/manual producers
of one consumer without changing configured models, providers, fallbacks or tools.
This is a runtime prerequisite only; fundraising token-bound consumer cutover, delivery
correlation and compact notifications remain incomplete and unactivated.

## Contract

Optional stored-job field `run_lock`, normalized using existing optional-text semantics.
Absent/blank means identical current behavior. Persist only when set; create/update/readback
must preserve it. No new core tool or speculative UI. Stored-job API is the activation seam.

One non-reentrant, nonblocking mutex per canonical profile home and consumer name:
process-local `threading.Lock` plus OS advisory lock. Hash the name with full SHA-256 for
a private lock path under that profile's cron dir. Do not unlink lock files. Different
profiles/consumers progress independently; threads and processes serving one consumer
collide. Same-thread recursion raises a distinct error before any claim/ledger mutation: it must
never retire the outer owner as ordinary contention. Process death releases the OS lock, no TTL.

Acquire within `run_one_job`, after external-worker handoff has resolved inside the worker,
inside its existing cleanup try/finally, before the fire-heartbeat/shared body can call
`claim_dispatch`, preflight or agent code. Parent never holds the lock across handoff.
The lock covers computation/saved output; deferred queue draining is outside it and it
proves no delivery. Preserve watchdog/cancellation and profile/secret/terminal scopes.

Contention runs no script/model/send and creates no notice/incident. Before returning,
retire only this owner's fire claim, manual stamp and one-shot run claim, with expected-owner
fencing under existing job/fire locks. Preserve outage-marker semantics, newer owners,
previous status/history/next schedule and finite repeat counters. Record the execution as
failed with a precise computation-not-started reason, without `mark_job_run` spending repeats.
Return through existing in-flight cleanup. Backend/I/O refusal is a precise recorded
computation failure, never silent contention or an unlocked run; perform the same unstarted
claim cleanup without consuming a dispatch budget.

POSIX reuses `_acquire_flock(fd, 0)` / `_release_flock` plus a nonblocking local guard.
Windows's existing helper uses blocking `LK_LOCK`; do not call it for this feature.
Until a native nonblocking Windows implementation is verified, opt-in jobs fail closed
there. Unmodified jobs remain unchanged. Record this support boundary explicitly.
No new environment variable, token file, database column, ledger or publication owner.

## Implementation plan

1. Add `cron/consumer_run_lock.py`: lock context yields false only for contention and
   raises a precise OSError for backend/I/O failure. Resolve profile home at call time;
   use existing flock helpers on POSIX, never the blocking Windows helper. Protect the
   keyed local guards with one mutex and do not remove a guard while another thread may
   hold a reference. Reuse the existing keyed-fence pattern. No generic lock framework.
2. Extend `cron.jobs.create_job`'s optional keyword, existing create/update normalizers
   and persisted/readback representation. Add an owner-fenced
   `release_unstarted_fire(job_id, expected_owner)` store helper using existing fire fence
   and jobs lock. Check the owner before atomically clearing its fire/manual/run claims;
   preserve one-shot outage markers and all unrelated state. No cleanup of a newer owner.
3. Wire a small sibling helper at the shared `run_one_job` seam, preserving facade size
   and complexity ratchets. Terminalize this execution and release owner-fenced claims
   on contention/backend error, always traversing existing in-flight finally. Do not move
   the computation body's lifecycle or worker handoff boundary. No changes to provider
   setup, delivery semantics, tokens or consumer/live job definitions.
4. First write failing behavior tests in `tests/cron/test_consumer_run_lock.py`: native
   POSIX subprocess contention and death-release; threads and recursion; separate homes
   and consumers; early-return/exception/cancel release; real preflight→save lock span;
   no notice/repeat/status/claim/manual-stamp leak on contention; newer-owner fencing;
   backend/I/O refusal; absent-field behavior; create/update/readback. Prove manual and
   provider producers cross this seam with fake invocations and model/send/worker launch
   disabled. Compare configured model/provider/fallback/tool resolution before/after.
   Invented identities and temp homes only. No source-text/symbol/dataclass-shape tests.
5. Run canonical `HERMES_PYTHON=<installed test interpreter> scripts/run_tests.sh` on
   new and existing claim, immediate-run, provider and worker-ownership suites found from
   callers. No dependency install, bare pytest, real adapters or live state. Run
   `python scripts/check` and normal hooks. Codex independently inspects full diff/tests,
   checks current base/main, then commits/integrates only verified work. Push only to the
   user's fork, never upstream origin. Prune merged worktree/branch after verification.

## Audit and activation

Hermes must independently PASS this lock-only spec/plan before production coding. Original
audit blocked B1 contention cleanup/nonblocking proof, B2 token binding and C receipts;
D's private operator-accessible evidence destination is not proven. Codex removed the
symbol-name absence probe and SendResult field snapshot; baseline probes are evidence,
not acceptance tests. Raw audit lives in the calling task's `fundraising-runtime-audit/`.

Do not claim fundraising is run-bound or activate its lock until token and ALL consumer
cutover changes are audited and shipped together. No live Notion, job registration,
Site publication or monetary migration in this prerequisite. B2/C/D remain blocked.

Audit pins: SHA-256 is explicit (do not inherit the fire fence UUID5 path); optional-text
normalizer maps None/blank to None; load/get round-trip the stored field. The heartbeat
wrapper calls its body inline, joining only its refresh thread in finally. Tests must
prove these facts and recursive refusal without retiring the outer owner.
