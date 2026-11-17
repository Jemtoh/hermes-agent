---
sidebar_position: 20
title: "Fundraising Run/Delivery Runtime Extension — Plan"
sidebar_label: Fundraising runtime extension plan
description: "Concrete files, functions, schema migration and acceptance tests for the opt-in cron lifecycle extension; Phase D blocked"
---

> Independent review: **BLOCKED**. Historical proposal, not approved for coding.
> The [lock-only revision](./consumer-run-lock-plan.md) supersedes B1.

# Fundraising run/delivery runtime extension — plan

Status: **B and C are specified, not commissioned; D is BLOCKED.** Nothing in this plan is
implemented, committed or activated. Production edits wait on an independent audit of this
plan and of the [spec](./fundraising-runtime-extension-spec.md).

Spec: [Fundraising run/delivery runtime extension](./fundraising-runtime-extension-spec.md).
Consumer design (read-only source contract, outside this repository):
`/Users/jeremytoh/.hermes/docs/superpowers/specs/2026-10-09-fundraising-run-delivery-design.md`.

Owning repository: this checkout (`hermes-agent`). The extension is a runtime feature of
the cron scheduler, not configuration in a consumer repository: the consumer design states
that a needed run/lifecycle integration must be drafted and audited in its owning
repository first, and forbids plugin VALID_HOOKS inventions and upstream edits as
configuration.

## 0. Baseline evidence (already measured, this worktree)

Probe module: `tests/cron/test_runtime_extension_capability.py` (10 tests, fake seams, temp
`HERMES_HOME`, no model, no network, no adapter started).

```bash
cd <worktree>
/Users/jeremytoh/.hermes/hermes-agent/venv/bin/python \
  -m pytest tests/cron/test_runtime_extension_capability.py -p no:cacheprovider -q
# ..........                                                     [100%]
# 10 passed in 0.74s
```

What each probe pins, and the phase it supports, is listed in §5 (matrix). Two of them are
annotated `BASELINE GAP` in the module: they assert the pre-extension contract that blocks
a phase and must be replaced by that phase's own acceptance tests, not deleted.

## 1. Task B1 — the consumer run lock

**Files**

- New: `cron/run_lock.py` — `consumer_run_lock(name: str) -> ContextManager[bool]`, ~40
  lines, one owner. Reuses `cron/jobs.py::_acquire_flock` (`:254`) / `_release_flock`
  (`:275`) (imported, not copied) and the fail-closed message shape of
  `cron/jobs.py::_fire_job_lock` (`:343`).
  Lock path: `Path(HERMES_HOME) / "cron" / "locks" / f"run-{sha1(name.encode()).hexdigest()[:16]}.lock"`,
  created `0o600`, `Path` built at call time (never a module-level `~/.hermes` literal —
  `cron/AGENTS.md`).
- Modify: `cron/scheduler.py` — `run_one_job` (`:2772`). Wrap the existing block at
  `:2855-2874` (`self_removal_delivery_scope` + `_run_with_fire_claim_heartbeat`) in the
  lock context manager, with the contention branch returning before it. That one
  acquisition covers `_run_one_job_body` (`:3304`) end to end — `claim_dispatch`,
  `_start_owned_run`, `run_job`, `_prepare_job_prompt` (`:2224`), the agent run,
  `save_job_output` and `_deliver_result` — for all four producers, with a 4-line insertion
  plus a re-indent of the existing block. The pre-handoff dispatch-failure and external
  handoff arms return earlier and do no consumer work; the worker process re-enters
  `run_one_job` and takes the lock itself.
- Modify: `cron/jobs.py` — register `run_lock` in `_CREATE_FIELD_NORMALIZERS` (`:1751`)
  and `_UPDATE_FIELD_NORMALIZERS` (`:1766`) with the existing
  `_normalize_job_optional_text`, and persist it in the job record built in `create_job`
  (`:1900-1935`) as an optional key alongside `workdir`. Absent stays absent — no default
  is written into existing jobs.
- Test: `tests/cron/test_consumer_run_lock.py`.

**Exact semantics**

| Case | Behaviour |
|---|---|
| `job["run_lock"]` absent | context manager yields `True` immediately; no file opened |
| Lock free | yields `True`, held until the body exits (all exits, including `BaseException`) |
| Lock held by another process | yields `False`; caller logs at INFO and calls `finish_execution(execution_id, success=False, error="Consumer '<name>' is already running; this fire was skipped.")`, `return True` — no `mark_job_run`, no incident, no operator notice |
| Holder process killed | OS releases the byte-range lock; the next fire acquires immediately. No sweep, no TTL |
| Lock backend unavailable | fails closed (`False`), same as the fire fence |

**Rejected alternatives (recorded so they are not re-proposed)** — a blocking wait (a manual
run holds the tool call open for a whole agent run; a wedged holder deadlocks it); a lock
TTL or lease record (rebuilds the staleness problem the fire claim already has); a per-job
lock only (that is what exists today and it is the defect being fixed).

**Acceptance tests** (invented jobs, temp home, no adapters)

1. `test_two_producers_with_one_run_lock_name_serialize` — holder thread/process inside the
   context manager blocks a second acquisition, with a real second process (`subprocess`
   running a tiny inline script) so the test proves cross-process, not in-process, behaviour.
2. `test_second_producer_is_skipped_and_recorded_not_alerted` — drive `run_one_job` with a
   fake `run_job` patched at `cron.scheduler.run_job`; assert the second call does not
   invoke it, the execution row's `error` names the consumer, and `mark_job_run` was not
   called (patch `cron.scheduler.mark_job_run`).
3. `test_different_consumer_names_do_not_collide` — the same call twice with two names both
   reach `run_job`.
4. `test_lock_spans_preflight_and_save` — patch `_prepare_job_prompt` and `save_job_output`;
   assert both observe the lock held (a second acquisition inside them fails).
5. `test_lock_released_on_early_return` — jobs that hit `claim_dispatch` refusal, ownership
   loss, and an empty payload each leave the lock acquirable afterwards.
6. `test_lock_released_when_holder_dies` — kill the holder subprocess; the lock is
   immediately acquirable (no sleep beyond a bounded wait on process exit).
7. `test_untouched_job_resolves_identically` — with `HERMES_MODEL` and a job carrying
   `model`/`provider`/fallback overrides, `_resolve_cron_agent_setup(job, ...)` returns the
   same `model`/`runtime`/`fallback_model` values with and without `run_lock` present.

**Compatibility** — new file plus one `with` block plus two optional-field registrations.
No existing job changes behaviour; no signature changes; `cron/scheduler.py` gains no new
configuration read.

## 2. Task B2 — the sweep-token slot and caller migration (runtime side)

**Files**

- Modify: `cron/scheduler.py` — `run_one_job` (`:2772`): before delegating, create
  `<HERMES_HOME>/cron/tokens/` (`0o700`) and set `job["_token_file"] = <home>/cron/tokens/<execution_id>.token`.
- Modify: `cron/scheduler_script.py` — `_script_argv` (`:400`) / `_run_job_script` (`:432`):
  add `HERMES_CRON_TOKEN_FILE` to `env_overlay` when `job["_token_file"]` is set. It is a
  path, not a secret; the existing `build_subprocess_env` scrubber is unchanged.
- Modify: `cron/scheduler.py` — `_prepare_job_prompt` (`:2224`), immediately after the
  pre-run script returns (`:2271`): read the file if it exists (`0o600`, ≤128 chars, strip),
  set a run-scoped `token` value, and pass it into `_build_job_prompt` so exactly one
  `Sweep token: <token>` line is prepended. Missing file → no line, no error (opt-in).
- Modify: `cron/executions.py` — add `sweep_token TEXT` (§3.1); `run_one_job` passes it into
  `create_execution`/`finish_execution`.
- Consumer side (other repository, per the consumer design): preflight mints and appends
  durably, writes `$HERMES_CRON_TOKEN_FILE`, and every driver takes the token explicitly.
  This plan does not implement those files.

**Acceptance tests** — temp home, invented jobs, fake pre-run script that writes the file:

8. `test_token_file_is_exported_to_the_pre_run_script` — the fake script asserts
   `os.environ["HERMES_CRON_TOKEN_FILE"]` exists and writes a token; assert the prompt
   contains exactly one `Sweep token: <token>` line.
9. `test_missing_token_file_leaves_no_prompt_line_and_no_failure` — script writes nothing;
   the run proceeds and `sweep_token` stays NULL.
10. `test_token_is_recorded_on_the_execution_row_before_the_agent_is_built` — the ledger row
    carries it even when the agent build then fails.
11. `test_token_is_not_read_from_the_newest_file` — a stale token file from another
    execution id in the same directory is ignored.

## 3. Task C — receipt correlation on the existing ledger and queue

No new table, no second transport.

### 3.1 Schema migration

`cron/executions.py::_initialize_schema` (`:56`) — three `add_column_if_missing` calls
following the existing idiom at `:92`:

```python
add_column_if_missing(conn, "executions", "sweep_token", "sweep_token TEXT")
add_column_if_missing(conn, "executions", "report_sha256", "report_sha256 TEXT")
add_column_if_missing(conn, "executions", "notification_sha256", "notification_sha256 TEXT")
```

`cron/delivery_queue.py::_initialize_schema` (`:89`) — three on `deliveries` and one on
`delivery_tombstones`:

```python
add_column_if_missing(conn, "deliveries", "payload_sha256", "payload_sha256 TEXT")
add_column_if_missing(conn, "deliveries", "chunk_ids", "chunk_ids TEXT")
add_column_if_missing(conn, "deliveries", "receipt_kind", "receipt_kind TEXT")
add_column_if_missing(conn, "delivery_tombstones", "receipt_kind", "receipt_kind TEXT")
```

All nullable, no backfill, no `CHECK` constraint change (the existing `status` vocabularies
are load-bearing for the pruning SQL at `cron/delivery_queue.py:34`).

### 3.2 Writers

- `cron/executions.py::finish_execution` (`:312`) — add keyword-only
  `sweep_token=None, report_sha256=None, notification_sha256=None`, written in the existing
  `UPDATE`; every current caller is unchanged.
- `cron/scheduler_delivery.py::_deliver_result` (`:1973`) — after the per-target loop,
  set `job["_delivery_receipt"]` exactly as the function already sets
  `job["_notification_all_targets_suppressed"]` (`:2090`):
  `{"transport_sha256": sha256(delivery_content), "chunk_ids": [...], "kind": ...}`.
  - `delivered` — a target confirmed through `_confirm_adapter_delivery` **with**
    `message_id`/`raw_response` evidence; chunk ids = `message_id` +
    `continuation_message_ids` in send order.
  - `unverified` — the same function's existing "success with no delivery evidence" arm
    (`:1218-1228`, currently only a WARNING + `last_delivery_unverified`).
  - `partial` — the `PartialDeliveryError` arm (`:1529`) using
    `raw_response["delivered_chunks"] / ["total_chunks"]`.
  - `ambiguous` — the in-flight timeout arm (`:1522`, "assuming delivered … to avoid
    duplicate") and any Bot Chat receipt in `queued`/`claimed`.
  - The per-target summary is the weakest kind seen: one unverified target makes the run
    `unverified`, never `delivered`.
- `cron/delivery_queue.py::drain` (`:318`) — read `row["job"].get("_delivery_receipt")`
  after `send(...)` and pass it to `_finish` (mirrors the existing
  `_notification_all_targets_suppressed` read at `:338`).
- `cron/delivery_queue.py::_finish` (`:257`) — write `payload_sha256`, `chunk_ids`
  (JSON list), `receipt_kind`. `status` semantics unchanged; `receipt_kind` never promotes
  `failed`/`unknown`.
- `cron/delivery_queue.py::_prune_terminal_unlocked` (`:39`) — copy `receipt_kind` into the
  tombstone alongside `status`, so a pruned row's kind survives.

### 3.3 Consumer-supplied digests

`report_sha256` and `notification_sha256` come from the consumer (report manifest and the
compact-notification bytes). The runtime never computes them: it computes only
`transport_sha256` over the string it actually handed to the adapter, because wrap headers
and `(n/N)` chunk indicators make the transported payload differ from the saved file by
construction.

### 3.4 Acceptance tests — `tests/cron/test_delivery_receipts.py`

12. `test_partial_send_is_never_recorded_delivered` — fake transport raising
    `PartialDeliveryError` with `delivered_chunks=1, total_chunks=3`; assert
    `status != "delivered"` and `receipt_kind == "partial"` with the chunk ids recorded.
13. `test_unverified_send_records_unverified_kind` — `SendResult(success=True)` with no
    evidence through `_deliver_result` + `drain`; assert `receipt_kind == "unverified"`.
14. `test_delivered_send_records_chunk_ids_in_send_order` — `SendResult(success=True,
    message_id="3", continuation_message_ids=("1","2","3"))`; assert the stored JSON list is
    `["1","2","3"]` and `payload_sha256` equals the SHA-256 of the exact transported string.
15. `test_queued_and_unknown_rows_keep_their_status_with_new_columns` — re-run the existing
    deferral and abandoned-owner cases with the migration applied.
16. `test_tombstone_preserves_receipt_kind` — finish a row, force pruning, assert the
    tombstone carries the kind.
17. `test_pre_migration_rows_read_back_with_null_receipts` — insert a row with the old
    column set, reopen, assert NULLs and unchanged behaviour.

## 4. Task D — BLOCKED (no private evidence destination exists)

**Gap, measured (§6 of the spec):** the only operator-private artifact channel is a direct
send (text chunks or a local file via `send_document`); nothing mints or serves an
authenticated link, and the code's 20 MiB number is an **inbound** download cap
(`plugins/platforms/telegram/adapter.py:700`, enforced at `:5112`, `:6231`) with **no
outbound size guard at all** (`_send_local_file`, `:5336`).

**Therefore:** do not commission D. Do not create a link service, do not repurpose the
public fundraising monitor, do not cite `_max_doc_bytes` as an upload limit, and do not
ship a compact-notification template whose evidence link cannot be resolved. Until the
operator supplies a verified private destination, full transport stays as it is.

**Runtime work that D would additionally need** (only after that destination is approved):
the digest binding of §3 plus an explicit pre-send size/type check for the attachment path,
with its limit taken from the platform's documented upload limit (operator-verified) rather
than from `_max_doc_bytes`. That check is new code with no existing seam; it is not written
now, and nothing disabled or unused is left behind.

## 5. Matrix — existing seam proof, needed extension, gap

| Phase | Existing seam proof (measured) | Needed extension | Gap |
|---|---|---|---|
| **B** | One shared body for all producers (`run_one_job:2772` → `_run_one_job_body:3304` → `run_job:2526`, called from `scheduler.py:4263/:3961`, `scheduler_provider.py:236`, `cronjob_tools.py:382`). `_prepare_job_prompt:2224` runs pre-agent and early-returns. `claim_job_for_fire:2741` stamps `fire_claim{at,by}`; `fire_claim_fence:405` and `try_register_running_job:806` are both keyed by **job id** (probe: two jobs never collide) | `cron/run_lock.py` + one lock block in `run_one_job:2855` + optional `run_lock` job field (§1); token slot + prompt line + `sweep_token` column (§2) | no consumer-global lock exists; nothing carries a run token; `_prepare_job_prompt` proves any agent-only lock misses preflight |
| **C** | `executions` ledger has `delivery_outcome` only (`executions.py:60-92`, written by `finish_execution:312`); `_classify_delivery_outcome:2902` already separates `queued`/`delivered`/`failed`/`suppressed`; `deliveries` queue is durable, idempotent by execution id, and never replays an uncertain send (`enqueue:165`, `recover_abandoned:282`, `_terminalize_wait_timeout:346`); `_confirm_adapter_delivery:1193` already detects success-without-evidence; `SendResult` carries `message_id` + `continuation_message_ids` (`base.py:1703`) | `payload_sha256`/`chunk_ids`/`receipt_kind` on `deliveries` + tombstones; `sweep_token`/`report_sha256`/`notification_sha256` on `executions`; `job["_delivery_receipt"]` written in `_deliver_result`, read back in `drain`, persisted in `_finish` (§3) | no artifact digest, no token and no ordered-chunk correlation anywhere: classification can call a send delivered without proving the saved bytes, and a bare `success=True` is accepted as delivered |
| **D** | Private channel = `send_document(chat_id, file_path)`; text caps 4096/32768, caption 1024; inbound cap 20 MiB / 2 GiB local `base_url`. Measured: chunking appends `(n/N)`, so transported bytes ≠ saved bytes | none until a destination exists; then the §3 digest binding + an outbound size/type check | **no authenticated link capability at all**, and no outbound limit is encoded in code — this is what blocks D |

## 6. Compatibility and rollback

- Additive schema only, nullable, applied by the existing `add_column_if_missing`
  migrations; an older gateway running beside a newer one reads NULLs and behaves as today.
- The run lock is opt-in per job; rollback is removing the `run_lock` field. Nothing is
  written by the extension that a rollback must undo, and the consumer's own atomic
  activation/rollback (its design §Phase B) is unaffected by the runtime side.
- The token file lives under `cron/tokens/`, is per execution id, and is never read
  retroactively — no historical replay is possible from the runtime side.
- Model/provider/fallback/reasoning/tool resolution is untouched: no new read of those
  fields, no new config key, no new `HERMES_*` env var.

## 7. Verification for the integrator

```bash
cd <worktree>
# the baseline probes this plan extends
/Users/jeremytoh/.hermes/hermes-agent/venv/bin/python \
  -m pytest tests/cron/test_runtime_extension_capability.py -p no:cacheprovider -q

# the suites the three tasks touch
/Users/jeremytoh/.hermes/hermes-agent/venv/bin/python -m pytest \
  tests/cron/test_consumer_run_lock.py tests/cron/test_delivery_receipts.py \
  tests/cron/test_claim_job_for_fire.py tests/tools/test_cronjob_run_immediate.py \
  tests/tools/test_cronjob_run_delivery_notice.py -p no:cacheprovider -q
```

Test isolation rules for all of the above (`tests/AGENTS.md`): temporary `HERMES_HOME` via
the per-test fixture, invented job identities, fake transports (a callable, never an
adapter), no model call, no network, no live job, no `~/.hermes` write. The two new test
files are the only files that may be added by the B/C tasks besides the source changes.

Mode note for the integrator: this session ran `python -m pytest` directly with the
installed managed interpreter (the invocation the commissioning brief specified) rather
than `scripts/run_tests.sh`, whose per-file subprocess isolation is the repository default.
The affected files should be re-run through `scripts/run_tests.sh` before integration.

## 8. Verdict and blockers

- **B: specified, not commissioned.** The seam is proven, the extension is one lock, one
  job field, one token slot and one ledger column. It needs an independent audit of this
  plan before any code is written.
- **C: specified, not commissioned.** Additive columns on the existing ledger and queue,
  one new receipt key on the job dict following an existing pattern. Needs the same audit.
- **D: BLOCKED.** No private, operator-accessible evidence destination exists, and the spec
  forbids creating one. D stays uncommissioned; full transport remains the fallback.
- **No phase is complete.** No production code, config, job definition, schedule or
  consumer artifact was changed by this work. The only changes are the two documents above
  and the probe module.

### Evidence receipt

| Item | Value |
|---|---|
| Worktree | `/Users/jeremytoh/.codex/mcp/hermes-coder/runs/81d4b8e67858/worktree` (branch `codex/hermes-81d4b8e67858`, base `9d05e7ff92`) |
| Files added | `tests/cron/test_runtime_extension_capability.py`, `website/docs/developer-guide/fundraising-runtime-extension-spec.md`, `website/docs/developer-guide/fundraising-runtime-extension-plan.md` |
| Files modified | none |
| Invocation | `/Users/jeremytoh/.hermes/hermes-agent/venv/bin/python -m pytest tests/cron/test_runtime_extension_capability.py -p no:cacheprovider -q` |
| Result | `10 passed in 0.74s` |
| Neighbour suite re-run | `tests/cron/test_claim_job_for_fire.py` → `20 passed in 1.04s` |
| Adapters/models/network | none started, none called |
| Live state touched | none (temp `HERMES_HOME` only; the two read-only source contracts were opened with `read_file`) |
