---
sidebar_position: 19
title: "Fundraising Run/Delivery Runtime Extension"
sidebar_label: Fundraising runtime extension
description: "The smallest opt-in cron lifecycle extension needed by an external run/report consumer: consumer-global run lock, sweep-token binding, receipt correlation"
---

> Independent review: **BLOCKED**. Historical proposal, not approved for coding.
> The [lock-only revision](./consumer-run-lock-plan.md) supersedes B1.

# Fundraising run/delivery runtime extension

Status: **proposal, unaudited**. This document describes a runtime extension that does
not exist yet. It is the prerequisite proof requested before Phases B, C and D of the
external run/delivery design are commissioned. No production code, no schema migration
and no live job definition changes ship with this document.

Plan: [Fundraising run/delivery runtime extension plan](./fundraising-runtime-extension-plan.md).
Consumer design this serves (read-only source contract, outside this repository):
`/Users/jeremytoh/.hermes/docs/superpowers/specs/2026-10-09-fundraising-run-delivery-design.md`.

Authorisation boundary: the consumer design forbids a new ledger, a second transport, a
new Sites publication, guessed plugin hooks, models/providers changed, and any production
edit before an independent spec/plan audit. This document obeys that: it extends the
existing cron execution ledger and the existing durable delivery queue.

## 1. What is proposed, in one paragraph

Three opt-in additions to the existing cron run path, each inert when unused:

1. **A consumer run lock** — one cross-process lock per consumer name, acquired inside the
   single shared run body (`cron/scheduler.py::_run_one_job_body`) and held across
   preflight script → agent → saved output → delivery. Opt-in per job via one job field.
2. **A sweep-token binding** — a run-scoped token file the pre-run script writes, which the
   runtime reads back onto the execution ledger row and the delivery payload, so a report,
   its review and its notification all name the same immutable evidence.
3. **Receipt correlation on the existing ledger and queue** — the delivered artifact digest,
   the exact transported payload digest and the ordered transport chunk ids are recorded
   where the send already happens, so `delivered` stops being an inference from "the
   adapter returned success".

Nothing else. No new table, no new transport, no hook registry, no new env var, no config
knob, no provider/model/fallback change.

## 2. The seam, measured

Every cron producer already funnels through one function. Callers of
`cron/scheduler.py::run_one_job` (line 2772):

| Producer | Call site | Lock held today |
|---|---|---|
| Gateway ticker | `cron/scheduler.py::_process_due_job` → `:4263` | per-job fire claim + per-job running registration |
| External / restart-safe worker | `cron/scheduler.py::_run_external_worker_payload` → `:3961` | per-job fire claim |
| External scheduler provider (`fire_due`) | `cron/scheduler_provider.py:236` | per-job fire claim |
| Manual `cronjob(action="run")` | `tools/cronjob_tools.py:382` (`_run_claimed_job`) | per-job fire claim + per-job running registration |

Everything below `run_one_job` runs in `_run_one_job_body` (`cron/scheduler.py:3304`),
which calls `run_job` (`:2526`), which calls `_prepare_job_prompt` (`:2224`) before the
agent exists, and `_save_compose_deliver` (`:3050`) after it.

Measured identity of today's mutual exclusion (`tests/cron/test_runtime_extension_capability.py`):

- `claim_job_for_fire` stamps `job["fire_claim"] = {"at": ..., "by": "<machine>:<uuid4>"}`
  (`cron/jobs.py:2741`); `fire_claim_fence` (`cron/jobs.py:405`) yields ownership only for
  that exact job id and owner.
- `try_register_running_job` (`cron/scheduler.py:806`) keys on `(profile home, job id)` —
  its own docstring says the fire claim's 300 s TTL cannot cover a real run.
- `_prepare_job_prompt` can return before any agent is built (empty payload, wake gate,
  monitor gate, injection block).

**Consequence, and the reason Phase B is blocked:** both mechanisms are keyed by *job id*.
Two different jobs — or one scheduled job plus one manual run of a *sibling* job — serving
one consumer collide on nothing, and neither spans preflight → saved output for a single
job. A per-job fire claim cannot express "one producer at a time for this consumer".

## 3. B — the consumer run lock

### 3.1 Opt-in field

One new optional job field, read nowhere else:

```
run_lock: "<consumer name>"     # e.g. "fundraising-daily"
```

Absent (every existing job) → no lock, no behaviour change, zero new code on the path.
There is no environment variable and no config key: non-secret behaviour belongs in the
job record (repository root `AGENTS.md` rejects new `HERMES_*` env vars for non-secret
config).

### 3.2 Implementation shape

New module `cron/run_lock.py` (small, one owner, no state of its own):

```python
@contextlib.contextmanager
def consumer_run_lock(name: str) -> Iterator[bool]: ...
```

- Lock file `<HERMES_HOME>/cron/locks/run-<sha1(name)[:16]>.lock`, opened `0o600`, locked
  with the same `flock` helper `cron/jobs.py::_fire_job_lock` already uses
  (`_acquire_flock` / `_release_flock`; a no-backend platform fails closed exactly as
  `fire_claim_fence` does).
- **Non-blocking.** Contention yields `False`; it never waits. Waiting inside a ticker
  thread or a manual tool call burns a tick and hides a wedged producer.
- Held by a `finally` in the caller, so every early return, `BaseException`, and timeout
  releases it.
- **Process death releases it.** This is an OS byte-range lock, not a lease record: a
  killed or restarted gateway releases it when the process dies. There is no stale-lock
  sweep and no lock TTL to get wrong. The file remains on disk; that is expected.

### 3.3 The one call site

In `cron/scheduler.py::run_one_job` (`:2772`), around the block that already exists at
`:2855-2874` (`with self_removal_delivery_scope(...) : _run_with_fire_claim_heartbeat(...)`),
enter the consumer lock:

```python
with consumer_run_lock(str(job.get("run_lock") or "")) as owns_consumer:
    if not owns_consumer:
        ...  # skip, see below
    try:
        with self_removal_delivery_scope(job["id"]):
            return _run_with_fire_claim_heartbeat(...)   # unchanged
    finally:
        ...                                              # unchanged
```

One lock acquisition spans, for every producer: `_run_one_job_body` → `claim_dispatch` and
`_start_owned_run` → `run_job` → `_prepare_job_prompt` (pre-run script / preflight) → the
agent build and run → `save_job_output` → `_deliver_result`. It is a 4-line insertion plus
the re-indent of an existing 15-line block; the large body is untouched.

Two boundaries are deliberately outside the lock, and both are honest: a pre-handoff
dispatch failure and the external-worker handoff return before this block. The first only
records a failure notice (no consumer work); the second is not a run at all — the worker
process re-enters `run_one_job` and takes the lock itself
(`_run_external_worker_payload` → `:3961`), so the handoff never leaves the consumer
unguarded.

The blank name short-circuits to an always-true no-op, so unmodified jobs keep today's
exact control flow.

### 3.4 Contention is a recorded skip, not a failure notice

On `owns_consumer is False`, mirror the existing "one-shot dispatch limit reached" arm
(`cron/scheduler.py:3335-3342`): log once at INFO naming the consumer, then

```python
finish_execution(execution_id, success=False,
                 error="Consumer '<name>' is already running; this fire was skipped.")
return True
```

`mark_job_run` is **not** called, so no failure streak, no incident and no operator
notification — a skipped duplicate is not a broken job. The ledger row records why the
attempt did nothing. Rejected alternative: blocking until the holder finishes (a manual
run would hold the tool call open for the length of a full agent run, and a wedged holder
would deadlock it); rejected alternative: a lock TTL (re-introduces the exact staleness
problem the fire claim already has).

### 3.5 What must not change

- `_load_cron_job_config`, `_resolve_cron_agent_setup` (`cron/scheduler.py:2423`) and
  `_construct_cron_agent` (`:2465`) are untouched: the lock adds no argument to the agent
  build and reads no model, provider, runtime or fallback field. A locked-out fire never
  reaches setup resolution at all.
- Job model/provider/fallback/reasoning/tool fields and the `cron` config section keep
  their current meaning. This extension deliberately has no configuration surface.

## 4. B — sweep-token contract (cross-repository)

The token is the consumer's identity for one sweep. The runtime's job is to carry it, not
to mint it.

### 4.1 Who mints, who appends

- **The consumer mints and durably appends.** Its preflight mints one opaque token and
  appends it to its own audit store before any write; a failed append stops the run. The
  runtime makes no consumer-specific write and owns no second audit trail.
- **The runtime provides the hand-off slot.** Before running the pre-run script, the
  runtime adds `HERMES_CRON_TOKEN_FILE=<HERMES_HOME>/cron/tokens/<execution_id>.token`
  to that script's existing environment overlay. The pre-run script's env is assembled in
  `cron/scheduler_script.py::_run_job_script` (`:432`, via `_script_argv`'s
  `env_overlay` + `tools.environments.local.build_subprocess_env`), so this is one more key
  in a dict that already exists — no new process, no new env channel. The consumer
  preflight writes the minted token there — atomically, `0o600` — after its own durable
  append.
- **The runtime reads it back once**, immediately after the pre-run script returns and
  before the agent is built, into a run-scoped value that (a) is appended to the prompt as
  one line, (b) is passed to the agent through the prompt only — never as a mutable job
  field — and (c) is written to the execution ledger row.

Why a file and not prompt scraping: the token must survive the model. A token that exists
only as prompt text is unverifiable at delivery time; a file written by the minting
process is evidence.

### 4.2 Explicit token everywhere — the caller migration

Every consumer caller takes the token as an explicit argument; "newest preflight wins" is
abolished, and there is no token-less legacy path for a new run.

| Stage | Owner | Token handling |
|---|---|---|
| preflight (mint) | consumer | mint, durable append, write `HERMES_CRON_TOKEN_FILE` |
| runtime, post-preflight | `cron/scheduler.py` | read file → `execution.sweep_token` (see §5.1), append one prompt line |
| agent | prompt text only | passes the literal token to each driver command |
| write / outcome / extraction review | consumer, token argument | every call carries the token explicitly |
| report save | consumer | immutable per-token report + full SHA-256 manifest (§4.3) |
| delivery | runtime | `report_sha256` + token on the ledger row (§5) |
| page eligibility | consumer | reads bound computation/source/report evidence for the token, never `job.last_status` |
| batch linkage | consumer | token linked to the existing authorized batch identity and finalizer receipt |

The runtime side of this table is the whole runtime deliverable: a slot, a read-back, one
prompt line, one column. Everything else is consumer-side and is specified by the consumer
design, not here.

### 4.3 Midnight slice and immutable manifests (contract the runtime must not break)

- The token binds registration, write/outcome, the extraction-review marker and the report.
  A run spanning HKT midnight selects token-bound records from **all** audit files it
  touched, with UTC timestamps explicit. The runtime does not implement this; it must not
  interfere with it — concretely, the runtime never rewrites, rotates or names the
  consumer's audit files, and the token file is per execution id, never per day.
- Immutable per-token reports with full SHA-256; the daily "latest" pointer is a
  compatibility view only. The runtime records the digest of what it transported (§5.3) so
  a later audit can compare the two without trusting `job.last_status`.
- Activation and rollback: the consumer migrates path helpers, the daily pointer, the
  deliver stamp, the page-built stamp, review roots/markers/attempt caps and all consumers
  together as one atomic unit, with legacy artifacts left as history and a specific
  existing checkpoint preventing historical replay. The runtime contributes no rollback of
  its own — its three extensions are inert when the consumer stops passing `run_lock` and
  stops writing the token file.

## 5. C — receipt correlation on the existing ledger and queue

No second ledger and no second transport. Two existing stores gain columns; existing
status vocabularies keep their meaning.

### 5.1 Execution ledger (`cron/executions.py`)

Add, using the file's existing `add_column_if_missing` idiom (`:92`):

```sql
ALTER TABLE executions ADD COLUMN sweep_token TEXT;
ALTER TABLE executions ADD COLUMN report_sha256 TEXT;       -- immutable saved artifact
ALTER TABLE executions ADD COLUMN notification_sha256 TEXT; -- what the operator was sent
```

Written by `finish_execution` (`cron/executions.py:312`), which already takes
`delivery_outcome`: three new keyword arguments, defaulted `None`, so every existing
caller is unchanged. `delivery_outcome` keeps its current vocabulary —
`_classify_delivery_outcome` (`cron/scheduler.py:2902`) already distinguishes
`delivered` / `queued` / `not_configured` / `failed` / `suppressed` / `suppressed_acked`;
`mark_job_run` already projects `delivery_failed` (`cron/jobs.py:2378`) and
`_record_delivery_verification` (`cron/scheduler_delivery.py:1281`) already persists
`last_delivery_unverified` / `last_delivery_queued` for the negative cases. The extension
does not replace that vocabulary; it adds the artifact identity those fields lack.

### 5.2 Durable queue (`cron/delivery_queue.py`)

Add to `deliveries`:

```sql
ALTER TABLE deliveries ADD COLUMN payload_sha256 TEXT;  -- exact transported bytes
ALTER TABLE deliveries ADD COLUMN chunk_ids TEXT;       -- JSON list, send order
ALTER TABLE deliveries ADD COLUMN receipt_kind TEXT;    -- delivered|partial|ambiguous|unverified
```

and to the tombstone table the same `receipt_kind`, so a terminal row's kind survives
pruning.

The `status` column keeps its six-value vocabulary (`pending`, `delivering`, `delivered`,
`failed`, `unknown`, `suppressed`); `receipt_kind` is the finer-grained honesty field and
never promotes `failed`/`unknown` to `delivered`.

### 5.3 Where the evidence is captured (the actual change)

`cron/scheduler_delivery.py::_deliver_result` already mutates the job dict with per-target
receipts (`_bot_chat_delivery_receipts`, `_notification_all_targets_suppressed`) and the
queue drain already reads that dict back (`cron/delivery_queue.py:338`). Reuse that
pattern — one new key, `job["_delivery_receipt"]`:

```python
{"transport_sha256": ..., "chunk_ids": [...], "kind": "delivered"|"partial"|"ambiguous"|"unverified"}
```

- **chunk ids**: `SendResult.message_id` plus `SendResult.continuation_message_ids`
  (`gateway/platforms/base.py:1703`; the field exists today and `message_id` is documented
  as the LAST id when a payload was split). For a partial send the existing
  `PartialDeliveryError` path already carries
  `raw_response["delivered_chunks"] / ["total_chunks"]` (`:1529`); record those and set
  `kind="partial"`.
- **`kind="unverified"`**: exactly the case `_confirm_adapter_delivery`
  (`cron/scheduler_delivery.py:1193`) already detects and logs as UNVERIFIED — truthy
  `success` with no `message_id` and no `raw_response`. Today that is a log line and a
  `last_delivery_unverified` entry; the extension records it as a receipt kind so the
  ledger cannot answer "delivered" for it.
- **`kind="ambiguous"`**: a send that timed out in flight after dispatch (the existing
  "assuming delivered (skipping standalone fallback to avoid duplicate)" arm, `:1522`) or
  a Bot Chat receipt in `queued`/`claimed` state. It is neither delivered nor failed and
  must be reconciled before any retry.
- **`transport_sha256`**: SHA-256 over the exact string handed to the adapter (post-wrap,
  post-redaction — the bytes the transport actually received), not over the saved file.
  The saved artifact's digest travels separately as `report_sha256`, because the wrap
  header/footer and chunking make them different by construction (measured:
  `BasePlatformAdapter.truncate_message` appends `(n/N)` indicators, so a split payload is
  not byte-identical to its source).

Drain wiring: `cron/delivery_queue.py::drain` (`:318`) reads
`row["job"].get("_delivery_receipt")` after `send(...)` and passes it to
`_finish` (`:257`), which writes the three columns alongside the status it already
computes. The in-process path (`_save_compose_deliver` → `_deliver_result`) passes the same
dict to `finish_execution`.

**Honesty rule the extension enforces, not merely documents:** exit 0, stdout, a `queued`
or `pending` row, `suppressed`, `unknown`, a bare `success=True` with no evidence, and a
partial chunk set are all non-deliveries. Only `kind="delivered"` with a recorded
`transport_sha256` and a chunk list is a verified send, and even that proves the message
reached the transport — never that every byte of the saved artifact was reconstructed
correctly.

## 6. D — private evidence channel and measured limits

### 6.1 Inventory measured from code

| Capability | Value | Evidence |
|---|---|---|
| Direct message text | 4096 UTF-16 code units, split by `truncate_message` | `plugins/platforms/telegram/adapter.py:520`, `:522` |
| Rich message text | 32768 chars | `plugins/platforms/telegram/adapter.py:523` |
| Attachment caption | 1024 chars, silent truncation | `plugins/platforms/telegram/adapter.py:5137` |
| Local-file attachment | `send_document(chat_id, file_path)` — native upload | `plugins/platforms/telegram/adapter.py:5358`, `gateway/platforms/base.py:3217` |
| Attachment size limit | **inbound only**: 20 MiB public API / 2 GiB with a local `base_url` | `plugins/platforms/telegram/adapter.py:700`, enforced on download at `:5112`, `:6231` |
| Outbound attachment size check | **none in code** — `_send_local_file` (`:5336`) opens the file and uploads | measured by reading the send path; no guard exists |
| Private authenticated link to a saved file | **none** | measured: no function in `cron/delivery_queue.py`, `cron/executions.py` or `cron/scheduler_delivery.py` has a link/url name, and `send_document(chat_id, file_path)` has no `url` parameter — the only artifact channel is a local-file send (`tests/cron/test_runtime_extension_capability.py`) |

### 6.2 What this means for Phase D

- **The capability Phase D needs does not exist.** The only operator-private artifact
  channel is a direct send to a configured target (text chunks, or a local-file
  attachment). Nothing mints or serves an authenticated link to a saved report. Per the
  consumer design, that **blocks D**: no service is to be created, and the public
  fundraising monitor must not be repurposed to manufacture a link.
- **The 20 MiB number must not be reused as an outbound limit.** It is derived from the
  inbound download path. Any attachment limit used for delivery must come from a verified
  source (the platform's documented upload limit, confirmed by the operator) and be
  enforced by new pre-send code; the extension must not cite `_max_doc_bytes` as if it
  bounded an upload.
- **Chunked text is a transformation, not a copy.** A split message is not byte-identical
  to its source (§5.3), so a compact notification and a full artifact cannot both be
  verified from one digest.
- Until a verified private destination exists, full transport stays as it is. The
  runtime-side work that D *does* need is only the digest binding of §5.1 and an explicit
  pre-send size/type check for any attachment path.

## 7. Compatibility, opt-in surface, and what is explicitly not proposed

| Item | Guarantee |
|---|---|
| Jobs without `run_lock` | identical control flow; the lock context manager is a no-op |
| Jobs that never write a token file | no prompt line, `sweep_token` stays NULL |
| Ledger schema | additive `add_column_if_missing` only; older gateways read the new columns as NULL |
| Queue status vocabulary | unchanged; `receipt_kind` is additive and nullable |
| Model/provider/fallback/reasoning/tools | untouched — no code path added to config resolution |
| Text delivery, wrapping, redaction, mirroring | untouched except that a receipt is recorded alongside |
| Failure lanes, incidents, `[CRON_FAILURE]` semantics | untouched |

Not proposed, deliberately: a plugin hook registry or any new `VALID_HOOKS` entry (none
exists for cron lifecycle, and inventing one is explicitly out of bounds); a second
transport or ledger; a new completion registry; an archive service or summary model for a
link; disabled-but-present code (nothing in this document ships before its phase is
commissioned); new `HERMES_*` env vars for non-secret config; any change to a live job
definition.

## 8. Acceptance criteria for the extension itself

The extension is complete only when, in an isolated worktree with fake seams and a
temporary `HERMES_HOME` (never a model, network call, live adapter or live job):

**B**
1. Two producers sharing one `run_lock` name: the second is skipped, the ledger row says
   so, and no failure notice is produced. Two producers with *different* names both run.
2. The lock is held across the pre-run script, the agent build and the saved output: a
   blocking fake pre-run script proves overlap is impossible; a raising one proves release.
3. Killing the holder process releases the lock with no sweep and no TTL wait.
4. Every early return of `_run_one_job_body` (dispatch limit, ownership lost, empty
   prompt) leaves the lock unheld.
5. A job with a model/provider/fallback override resolves byte-identically to today,
   locked and unlocked.

**C**
6. A drained send whose transport reports `partial`, `ambiguous` or no evidence never
   yields `status='delivered'` without a matching `receipt_kind`.
7. The recorded `chunk_ids` equal the adapter's `SendResult.message_id` +
   `continuation_message_ids` in send order, and `transport_sha256` matches the transported
   string exactly.
8. A dead delivery owner still terminalizes `unknown` and is never replayed (existing
   behaviour preserved with the new columns present).
9. Old rows written before the migration read back with NULL receipt columns and unchanged
   behaviour.

**D** — cannot pass: no private destination exists. Until the operator supplies one, D
remains uncommissioned and the fallback is full transport.

**Evidence module:** `tests/cron/test_runtime_extension_capability.py` pins the pre-extension
baseline these criteria extend; the plan says which probes its tests replace.
