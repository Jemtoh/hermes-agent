# Cron delivery uncertainty correction

Owner: Codex reviews and integrates; Hermes independently audits supplied source
with external tools disabled. Runtime base: `f08afb8fc236` in the maintained fork.
Scope authorized by the operator on 9 October 2026: finish planned live cron fixes.

## Contract and implementation

A send that started on the live gateway loop but exceeds the confirmation wait
keeps running. Do not cancel it or resend through standalone transport. Record
its target in the existing `last_delivery_unverified` field, including when the
adapter returns success without delivery evidence.

The scheduler consumes that negative evidence for both ordinary completion and
crash notices. Execution `delivery_outcome` becomes `unverified`, while actual
errors, queued admission, suppression, missing configuration and local-only runs
retain their existing precedence. Job `ok` means successful execution; callers
must inspect delivery evidence separately.

The durable gateway queue consumes the same evidence from the claimed job.
Unverified sends become terminal `unknown`, with a reason, redacted payload and
existing idempotency tombstone. They never enter automatic replay. Existing
failure and suppression outcomes retain precedence. No database schema,
transport, provider configuration or second ledger is introduced.

Affected owners: `cron/scheduler_delivery.py`, both completion classifiers in
`cron/scheduler.py`, and `cron/delivery_queue.py`. Existing CLI status consumers
already display `last_delivery_unverified`; execution outcomes are stored as text.

## Verification and rollout

Regression checks on the base reproduced an in-flight send without the negative
flag, ordinary/crashed execution records marked delivered, and a queued
unverified send marked delivered. After correction, the four affected test files
passed 129 tests. The full cron suite passed 1,517 tests with 12 platform skips;
all 11 repository checks passed. Tests use fake clients and isolated stores.
Two additional durability/stale-state checks passed in 51 affected-file tests.
Independent offline Hermes review returned PASS after verifying those checks and
the production queue callback. Normal-hook commit precedes integration.
Push only the maintained fork, then verify local main equals remote fork/main.
A running gateway may retain imported old modules until its supported restart;
a source push alone does not establish live runtime activation.

## Remaining Phase C work

This correction does not correlate a sweep token and full artifact digest with
ordered provider chunk receipts or reconcile a receipt arriving after timeout.
Existing job-level evidence must never confirm an older artifact. Fundraising
send checkpoints remain emitted/offered state until that separately audited
runtime extension and all consumer bindings are implemented and verified.
