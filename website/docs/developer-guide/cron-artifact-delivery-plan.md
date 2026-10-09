# Verified cron artifact delivery: design and execution plan

Status: independently audited PASS for extension implementation, 9 October 2026. No code or live
configuration is activated by this document. Runtime base: `f507014905` in the
maintained fork. Codex owns integration; Hermes audits/implements in isolation.
Operator authorization: complete remaining fundraising cron phases. Consumer B2
bindings and atomic cutover are separate prerequisites; no Notion property,
source/amount policy, monetary migration or Sites gate changes.

## Evidence and smallest supported extension

The shared `_run_no_agent_job` executes a trusted registered script and currently
returns stdout unchanged. `_deliver_result` uses live gateway adapters; external
workers hand the same job/content to the existing durable queue. Telegram
`_send_text_locked` formats and splits at 4,096 UTF-16 units, and `_send_chunks`
returns ordered `raw_response.message_ids`. Native `send_document` returns a
provider message ID; its error fallback can return text instead of the file.
`_send_media_via_adapter` currently discards positive evidence. No supported API
currently correlates a sweep token/artifact digest with these receipts.

C1 now records in-flight/no-evidence sends as unverified; queued unverified sends
become terminal unknown, redacted and never replayed. This design adds positive
correlation in the SAME execution/queue owners. Generic adapter success, stdout,
exit zero, local/suppressed/queued admission or a first chunk ID prove nothing.

## Request contract and scope

Add one optional registered job field `script_output_format=delivery-v1`, accepted
only with `no_agent=true` and a script. Absent means byte-for-byte legacy behavior.
Successful nonempty stdout is parsed BEFORE `_parse_wake_gate` for opted-in jobs.
Strict envelopes have no wakeAgent field; unknown keys including wakeAgent refuse.
Empty stdout or the exact `[SILENT]` marker remains silent and creates no request;
nonzero script exit follows existing failure-alert handling with no envelope.
Absent opt-in leaves existing wakeAgent and stdout handling byte-for-byte.
The trusted script emits JSON, never model-generated delivery instructions:

```
{"version":1,"token":"32 lowercase hex","purpose":"report|review",
 "message":"exact saved notification text","message_sha256":"64 hex",
 "artifacts":[{"kind":"report|review|review_result","path":"local path",
               "sha256":"64 hex","transport":"text|document"}]}
```

Runtime validates keys/types, token/digests, purpose, nonempty artifacts, no
unknown/duplicate kinds, and full request identity before any send. Notification
bytes must match their digest. Artifact paths reuse the existing media delivery
path policy, are local regular files and pass its realpath/allowlist checks; no
new archive/root bypass. At the ACTUAL sender, read bytes once, hash them and retain that immutable
snapshot for native upload. The worker queues JSON-safe identity/path metadata,
never bytes or file handles; the gateway revalidates paths/digests and captures
its snapshot before claiming/sending. Direct sends do the same. A changed queued
file fails before dispatch. Do not put byte snapshots into `job_json`; path changes cannot substitute later bytes. A document
request requires a native file receipt; text fallback cannot satisfy it.

For full report text, message digest equals artifact digest when no notification
is requested. Compact mode binds notification digest AND the full attachment
artifact digests. Request identity is token, purpose, destination and ordered
artifact/notification digests, never newest output/job state. Format parsing is
only at the trusted no-agent script seam; ordinary prose and failure alerts retain
the existing behavior. Invalid envelopes fail closed before any send.

Do not change model/provider/tool/fallback resolution, wrap policy for other jobs,
profiles, routing authorization, adapter media policy or general cron formats.
Opted-in jobs use their already-authorized private Telegram destination and exact
unwrapped saved notification. No secondary transport, public monitor or service.

## Durable receipt in the existing execution owner

Extend existing executions with ONE `delivery_receipt` JSON text column containing
versioned request, expected digests, destination, attempts and evidence. NULL
keeps old rows/callers; no separate key column duplicates canonical JSON identity.
Use SQLite JSON expression support, probed before optional index creation.
Catch unsupported JSON/index capability at initialization, preserve NULL/legacy
execution behavior, and explicitly fail opted-in request preparation before any
send; never make ordinary ledger initialization fail for this optional feature.
Literal DDL at `_initialize_schema`:

```sql
ALTER TABLE executions ADD COLUMN delivery_receipt TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_delivery_request_key
ON executions(json_extract(delivery_receipt, '$.request_key'))
WHERE delivery_receipt IS NOT NULL
  AND json_extract(delivery_receipt, '$.state') != 'failed_certain';
```

Use existing `add_column_if_missing` for migration, never raw repeated ALTER.
`request_key = purpose + ':' + token` is CODE-generated, not user-supplied.
`prepare_delivery_request(execution_id, job_id, request)` runs `BEGIN IMMEDIATE`
inside the existing `_transaction()`, looks up ALL prior matching keys (including
failed_certain), compares immutable request/destination/digests, and rejects any
identity conflict. Matching verified/sending/unknown requests are not resent.
Only proven failed_certain can permit a new attempt, retaining its old row.
Then UPDATE the existing owned execution row:

```sql
UPDATE executions SET delivery_receipt=?
WHERE id=? AND job_id=? AND process_id=? AND pid=?
  AND status IN ('claimed','running') AND delivery_receipt IS NULL;
```

Require exactly one changed row; unique-index conflict is an existing request,
not permission to dispatch. Queue transport claims `unsent -> sending` with a
new code-minted attempt nonce using a transaction and CAS on exact execution ID,
request digest and existing state. Capture owning profile and actual sender
PID/process-start-time in the receipt; late writes CAS on request+attempt nonce.
Any unsupported JSON/index/storage capability closes opt-in delivery before send.
Positive correlation never reads mutable `last_delivery_unverified` as proof.

States: unsent, sending, verified, failed_certain, unknown, suppressed. Evidence
contains attempt execution ID, request digest, actual logical target, per-artifact
full digest, expected chunk count and ordered chunk index/message-ID/hash receipts.
Store IDs/hashes/status, not credentials or report bodies. Change terminal-row selection in `executions._prune_unlocked` to exclude
receipt rows, BEFORE its LIMIT/OFFSET so the legacy 1000-row cap remains intact:
`SELECT id FROM executions WHERE status IN (...) AND delivery_receipt IS NULL
ORDER BY ... LIMIT -1 OFFSET ?`. Execution rows have no report body to redact;
receipt request JSON is ONLY canonical identity metadata (token/purpose/target,
notification digest and ordered kind/digest/transport/size), never envelope
message text, document bytes, credentials or local file paths. Queue payload
redaction remains owned by its existing prune helper. Receipt-bearing rows,
including unknown, remain durable until an independently approved archive/disposition
policy exists. This intentionally grows small ID/hash metadata with opted-in
runs; it cannot silently erase idempotency. No bodies/credentials are retained. Superseding/retrying is allowed only after a code-
proven never-sent/refused attempt; preserve its evidence and claim transaction.
No automatic retry on timeout, connection loss after dispatch or partial send.

Queue owners to change explicitly: `enqueue`, `enqueue_and_wait`, `drain`, `_finish`.
The existing execution ID remains its key. Before adopting any existing queue row
or tombstone, compare its request through the execution-store receipt anchor;
same execution ID/different token, purpose or digest refuses, never returns generic
terminal success. Queue handoff carries the validated request in its existing job payload. Gateway
records the receipt in the owning profile's execution store before queue payload
redaction; `drain`/`_finish` derive opted-in disposition from the exact carried receipt,
not mutable `last_delivery_unverified`. Verified -> delivered; suppressed remains
suppressed; unknown/partial/no-ID -> unknown; proven no-dispatch refusal -> failed.
Legacy queue disposition remains unchanged. Worker/`finish_execution` must reread
receipt in its completion transaction so a late verified receipt cannot be
clobbered by a stale timeout outcome. Existing queue owner/process fences remain. No accepted queue row becomes
verified until all required provider evidence is durably saved.

## Adapter correlation and late completion

The opted-in request metadata carries expected message/artifact digests through
DeliveryRouter to the actual transport. Explicit NEW extensions are required at `_send_chunks`, `_try_send_rich` and
`_send_local_file`; the existing adapter does NOT supply this evidence. Rich-path
capability is UNKNOWN until inspected/tested in step 1. Equivalent evidence or
unverified is required; no claimed capability from a first ID.
Extend Telegram to return receipt metadata naming
the incoming text digest, exact formatted chunks submitted in order, each chunk
hash and each actual provider message ID. Verify expected count, complete indices,
unique nonempty IDs and matching target; a single first ID is insufficient.
Rich or alternate formatting paths must provide equivalent complete evidence or
remain unverified; do not assume the Markdown chunk path ran. Preserve safe retry
and partial-send semantics. Receipts attest provider acceptance, not human reading.

Extend the owning `_send_local_file`/`send_document` path to accept the retained
byte snapshot for this opt-in contract (the current API opens a mutable path).
Ordinary file-path callers stay unchanged. Native document sends upload the SAME
immutable byte snapshot, attach its full
SHA-256 to the successful native provider message ID and actual target. A fallback
link/text, missing ID, changed file, policy drop or failed attachment cannot grant
verified. Track all attachments independently. Extend `_send_media_via_adapter` with an
optional receipt-collector argument; keep its existing list-of-errors return for
all callers. The collector receives structured per-artifact ID/hash/native-method
proof on opt-in, and its absence preserves legacy behavior. Thread the collector
through all actual callers; no global or mutable latest-job receipt. If adapter cannot retain snapshot
or positive evidence through an existing API, implementation must extend its
owning method explicitly; never trust `success=True` as file delivery.

For an in-flight timeout, retain future/no-resend behavior. Add a NEW `future.add_done_callback` at `_live_send_text` and the opted-in media
future owner; no callback exists today. Install it exactly once per claimed
attempt, retaining the validated request and captured profile context. Callback
validates and durably records late evidence for the exact request/attempt under
the captured owning profile scope. A dead process remains unknown; no callback
is proof. Catch callback/storage failure, retain uncertainty and expose reason.
Final confirmed evidence reconciles the exact execution outcome and opted-in
queue row/tombstone through the same receipt anchor and captured attempt nonce;
identity mismatch refuses, payload remains redacted, replay never resumes. A
callback dying before durable receipt leaves unknown. It it cannot
confirm a different/newer run. Consumers poll this receipt by exact identity.

## Consumer C and D integration boundary

Fundraising uses B2's token/full artifact digests and per-token saved review.
Report/review computed state is independent from receipt state. Delivery retries
reuse saved bytes; never pay for re-review. The existing consumption state stores
receipt references and expected identity, with runtime receipt as confirmation
owner; do not duplicate a positive proof. Unknown/partial sends fence retries and
produce an operator action. Timeout notices remain separate from report delivery.
Reuse the delivery/review jobs for bounded late/next-day/restart reconciliation;
change schedules only in the audited all-consumer activation with exact readback.

D may use deterministic notifications <=900 characters and native full evidence
attachments in the EXISTING private Telegram chat. This is the stated default
pending optional user preference; it creates no public link or archive. Retain
full report, full review and validated result. All blocking categories/counts must
fit; names are bounded to at most three plus remaining count. Compute full and
notification digests from saved bytes. Missing/unproven native file capability
retains full-text transport. Full evidence must be actually delivered alongside
the notice before compact notification is considered complete.

## Ordered implementation and verification

1. Locate ALL job-field validators/reference exports, no-agent callers, execution
   migrations/retention, queue drain/external-worker paths, DeliveryRouter metadata
   and Telegram rich/chunk/document paths. Confirm source ownership and actual
   supported methods. Record unknown capability as BLOCK, never invent hooks.
2. Stage fake RED tests for opt-in strict request/hash/path validation and unchanged
   legacy stdout, routing, models and suppressed/local/failed behavior. Implement
   one parsing seam in a topical sibling, preserve existing facade ratchets.
3. Stage real temp-SQLite claims/migration/retention tests: same token+purpose
   conflict, concurrent fires, existing positive/unknown request, queued restart,
   lost owner, redaction, no retained body/credential. Implement existing-store
   receipt helpers with transaction/process/profile fences.
4. Stage fake Telegram transport tests for 1/N chunks, UTF-16/long Markdown,
   rich path, partial/no-ID/missing chunk/wrong target, native document vs fallback,
   changed file and all attachments. Add receipt metadata at actual send owners.
5. Stage real-loop delayed completion tests: timeout stays unknown/no resend;
   exact future completing later records correct receipt; callback write failure
   remains uncertain; another run/profile cannot adopt it. Thread request and
   receipt through direct, queued and crashed paths; update ALL callers/docs.
6. Independent Codex review plus fresh offline Hermes spec-to-diff audit. Run the
   canonical runtime test runner, relevant cron/gateway/Telegram tests and every
   repository check, normal hooks. Push maintained fork only. No live test send.
7. Separately audited consumer plan binds saved report/review bytes, retry/receipt
   state, bounded schedules and D notification/attachment format. Source approval,
   monetary migration and Sites gates survive. Tests are invented/temp/offline.
8. Codex integrates only verified compatible runtime+consumer unit, drains current
   jobs, captures exact live definitions, activates supported job fields/schedule,
   restarts idle gateway if required and verifies readback. Keep compatible whole-
   unit rollback and all evidence. Retain full transport until capability passes.

Tests never write live caches/vault/Notion/broker or call real transports. No new
recurring job implements this one-time task. No dependency/provider/model change.

Independent offline Hermes re-audit passed all seven source-contract corrections;
Codex inspected the raw verdict and supplied source. No external tools or model
configuration changes. Native/rich positive receipts remain NEW implementation
requirements, not live capabilities already proven. Root source inspection of
`_try_send_rich` confirms one raw API request returning a message ID; its receipt
must hash the exact rich payload and incoming text with expected count one, or
remain unverified. The ordinary path emits multiple Markdown chunks. Extend
both at their actual send owners and test with fake provider responses before
calling the opt-in transport capable. `_transaction` uses a normal connection
context and does not begin IMMEDIATE itself; use the existing shared SQLite
primitive deliberately, without double-BEGIN. This is still unactivated work.

## Consumer API and dispatch-gap closure

NEW supported readback API to implement in the execution owner:
`get_artifact_delivery_receipt(token, purpose, *, expected_request_sha256=None)`.
Resolve under the caller's owning profile; validate token/purpose and reject
conflicting identity before returning decoded canonical metadata/status/evidence.
Return None only for no matching request, never as success. Consumers inject a
fake reader in tests. This is an extension contract, not an existing hook.

A worker may die between durable request preparation and queue insertion.
Reconciliation must not strand its `unsent` request forever: in BEGIN IMMEDIATE,
prove the owning execution PID/start-time is dead using the existing fail-safe
owner check, confirm state unsent with no attempt nonce or any chunk/file receipt,
then CAS unsent -> failed_certain with reason “owner exited before dispatch.”
Preserve that old receipt row. The partial unique index now permits a new exact-
identity claim on the next execution. Any live/unknown owner or sending/unknown
attempt stays fenced. An old pending queue row may not dispatch after this CAS;
its sender also CASes unsent -> sending against that exact anchor. Fake tests
cover preparation/queue crash gap, racing gateway claim and PID reuse.

For opted-in envelopes, explicit artifact metadata is the ONLY attachment source.
Bypass inline MEDIA extraction and response wrapper/whitespace rewriting for the
saved `message`; news/review prose containing MEDIA directives is literal text,
never a new file instruction. Ordinary job media extraction stays unchanged.
Preserve exact input bytes/digest before formatting; adapter receipts separately
hash actual transformed chunks. Test embedded directives and trailing newlines.

For opted-in envelopes, redact the saved notification through the existing cron
redaction helper BEFORE computing and saving `message_sha256`; the parser refuses
any message that would still change under that helper. Dispatch does not rewrite
it again. Provider receipts distinguish that saved input digest from formatted
chunk/payload digests. The full artifact remains separate private evidence under
the existing media policy. A live or unverifiable owner is fenced until an
operator resolves it; the dead-owner recovery does not reclaim a wedged live PID.

## Direct sender implementation ruling (9 October 2026)

The opted-in route in `scheduler_delivery._deliver_result` enters
`artifact_transport.deliver_artifact` before response wrapping, redaction and inline
MEDIA extraction. Its explicit artifact inventory is collected directly from
`adapter.send_document(snapshot=...)`. The orchestrator approved this direct owner
on 9 October 2026: `_send_media_via_adapter` remains the unchanged legacy MEDIA
owner; an unused receipt collector there is unnecessary. Every opted-in document
is collected independently from the actual native send result. A text artifact
must equal the exact saved notification bytes and reuses that notification's proof;
it causes no second text send or native upload.

Actual shared seams for the subsequent queue implementation:

- `artifact_transport.prepare_artifact_request(job)` returns
  `(validated_envelope, resolved_target, snapshots, receipt)` under the owned active
  execution. Snapshots are ephemeral byte data; serialize only canonical receipt
  identity/reference metadata. A returned prior execution anchor fences enqueue.
- `snapshot_request(envelope)` rechecks path policy and captures each artifact once.
  `build_request_identity(envelope, target, snapshots=None)` validates the ordered
  snapshot inventory and derives sizes from retained bytes, never later path stats.
- `executions.reconcile_delivery_request(execution_id, request_sha256, *,
  attempt_nonce=None, job_id=None)` reads that exact anchor under the owning profile.
  None means absent; conflicting job/digest/nonce raises. Gateway senders use this
  stored anchor and do not prepare a replacement under their process identity.
- `artifact_transport._dispatch(transport, config, chat_id, thread_id, message,
  snapshots, metadata)` sends once and returns notification/artifact results.
  `_settle(execution_id, request_sha256, attempt_nonce, profile_sha256, message,
  target, notification_result, artifact_results)` validates the full stored inventory
  and settles the exact attempt. Its tuple is internal; `deliver_artifact` exposes
  only None after durable verification, or an explicit disposition string.
- `_install_late_callback(future, *, execution_id, request_sha256, attempt_nonce,
  profile_sha256, message, target)` captures the actual Context, under the owning
  `_profile_cron_scope`, and retains the future through completion. Exceptions stay
  unknown. Exact late verification updates only delivery disposition, including a
  terminal execution row; it never changes run success or error.

`cron.artifact_proof` owns the closed evidence schema. Notification proofs carry
provider, native method (`send_message` or `sendRichMessage`), incoming digest/byte
size, actual target, complete count and ordered index/hash/provider IDs. Rich
proofs also bind the submitted payload hash. Document proofs carry provider,
`send_document`, full snapshot hash/size, actual target and provider ID. Stored
proof inventory must equal every ordered canonical kind/transport entry. IDs are
positive ASCII decimal strings; bool/float counts or indices and arbitrary extra
fields cannot verify. Evidence is bounded to 64 notification chunks; greater
counts remain unverified. Markdown plain fallback hashes the actual submitted
plain bytes. Thread fallback cannot attest the requested thread. Missing IDs,
failed SendResults, partial/omitted/extra proof and settlement without evidence
remain fenced. A corrupted receipt cannot grant positive finish disposition.

This ruling covers direct live delivery only. Queue adoption, drain, terminal and
late tombstone reconciliation remain the separately audited queue task. No live
activation, sends, source-policy or Sites-gate change is implied.

A missing chunk ID after earlier accepted chunks preserves the adapter's partial-send
metadata with no certain-unsent tail. Missing acceptance IDs are non-retryable;
provider acceptance may already have occurred.

## Actual queue implementation contracts (9 October 2026)

Implementation status: queue support implemented and locally verified in an isolated execution
worktree on `4f3a72c1b7`; NOT reviewed, NOT committed, NOT activated. There is still no live
queue send. This section records the contracts as BUILT, from source, because three of them are
corrections to the phrasing below.

- **Private serialized anchor name: `job["_artifact_anchor"]`** — the agreed reference the queue
  plan left open. It holds `{execution_id, job_id, request_sha256, request}` where `request` is
  the canonical identity `build_request_identity` produced. It carries no message text, no local
  path and no bytes, and it travels in the queue row's EXISTING `job_json` beside the already
  carried `_artifact_delivery` envelope. No delivery-queue schema change was made.
- **`artifact_transport.enqueue_artifact_request(job, content)`** — worker seam. It calls
  `prepare_artifact_request(job)` while the worker still owns its `running` execution row, refuses
  to enqueue when the returned receipt belongs to a different execution, mints the anchor, and
  then calls `delivery_queue.enqueue_and_wait`. `scheduler_delivery._deliver_result` reaches it
  only when `_HERMES_CRON_EXTERNAL_WORKER` matches the job's own `execution_id` and no adapter is
  present; every other caller keeps the direct sender.
- **`cron/delivery_queue.enqueue` adoption** validates an incoming anchor against the durable
  receipt under the CURRENT home before any existing row or tombstone is adopted: the anchor's
  canonical request must re-derive its own `request_sha256` (`executions._delivery_identity`),
  the stored receipt request must equal it, and the carried envelope plus queued content must
  restate its token/purpose/notification digest/ordered kind-digest-transport. A tombstone is
  adopted only when the receipt-derived status is settled and the tombstone does not contradict
  it; a refusal is returned as an error and is never inserted. Legacy jobs take the unchanged
  path, including the original tombstone-then-row query order.
- **Correction 1 — no `claim_next` preflight argument was needed.** Capture happens after queue
  ownership but BEFORE the receipt `unsent -> sending` claim, which is what the contract
  requires; `claim_next` still performs no filesystem read inside its SQLite transaction.
- **`artifact_transport._deliver_queued`** is the gateway adoption. It re-runs
  `validate_envelope(..., verify_files=False)`, re-resolves the single authorized Telegram target,
  captures every artifact path policy into immutable snapshots, rebuilds the full identity and
  compares its digest with the stored anchor, re-reads the stored receipt via
  `executions.reconcile_delivery_request`, and only then calls the existing
  `claim_delivery_request` CAS. It never calls `prepare_artifact_request`, and a receipt that is
  not `unsent` is refused by name. A proven refusal BEFORE the claim settles the anchor through a
  NEW guarded API.
- **Correction 2 — the existing settler could not express a proven pre-dispatch no-send.**
  `executions.settle_undispatched_delivery_request(execution_id, request_sha256, *, reason)`
  CASes `unsent -> failed_certain` with no attempt nonce and accepts only three code-owned
  reasons (`artifact snapshot or path policy refused before dispatch`, `artifact request
  identity conflict before dispatch`, `artifact transport unavailable before dispatch`); an
  arbitrary exception can never reach that state. `settle_delivery_request` is unchanged, so the
  existing gateway-loop refusal path and its tests still hold.
- **Queue terminal disposition is receipt-derived.** `delivery_queue._finish` ignores the drain
  callback's return value, its suppression flag and the legacy `last_delivery_unverified` marker
  for any row carrying an anchor, and maps the receipt instead: verified -> delivered,
  suppressed -> suppressed, failed_certain -> failed, anything else -> unknown. `drain`,
  `recover_abandoned` and the wait timeout `_terminalize_wait_timeout` apply the same
  receipt-derived outcome, and `get_status` reconciles on readback so a worker's poll sees a
  settled receipt.
- **Late completion reconciles RECEIPT-FIRST.** `artifact_transport._install_late_callback`
  settles the receipt and only then calls `delivery_queue.reconcile_late_delivery(execution_id,
  request_sha256, attempt_nonce=...)`, which requires a `verified` receipt whose
  `sender_profile_sha256` matches the current profile and upgrades an `unknown`/`delivering` row
  (same process owner) or an `unknown` tombstone. A wrong nonce, another profile, a
  failed/suppressed receipt or a missing queue row all refuse — a direct-send callback
  legitimately finds no queue row.
- **Correction 3 — receipt-first crash recovery needed a store reader.** `_reconcile_artifact_rows`
  runs at the top of `drain` and `delivery_queue._tombstone_settled` runs on readback; both use
  the NEW `executions.execution_delivery_receipt(execution_id)` because a redacted row or pruned
  tombstone no longer carries the anchor. Only a settled receipt may terminalize a row, so a row
  whose send is genuinely in flight is never touched.
- **Correction 4 — the refusal must precede EVERY adoption, and a pending row's own payload is
  validated too.** `enqueue` returns the validated refusal before it reads the tombstone or the
  existing row, so a changed message/envelope/target/size can no longer ride in on a pending row
  while only the anchor is compared. An existing pending row is additionally held to its OWN
  stored payload — envelope, queued content, canonical request (token, purpose, authorized target,
  ordered kind/hash/transport/size), execution id and job id — compared with the receipt, and a
  job whose own `id`/`execution_id` disagree with its anchor is refused. The incoming validation
  is unchanged in strength; it is now enforced on both sides of the comparison.
- **Correction 5 — admission metadata is REQUIRED, and lives in the existing receipt.**
  `executions.mark_queue_admitted(execution_id, request_sha256)` writes a single
  `queue_admitted: true` marker into the receipt JSON the row already has — no new column, table or
  store, and no token/digest copied into a queue tombstone. It is written only AFTER a validated
  new queue insert, or for a matching original pending opt-in, by CAS on the exact receipt, so it
  is durable only once the queue row is. A terminal row, pruned tombstone or redacted (`job_json
  = '{}'`) row is adopted or reconciled ONLY when the receipt carries that marker, and a
  `for_failure=True` delivery is the legacy route even when its job dict still carries a stale
  anchor. Consequences, all enforced in code: `enqueue` refuses opted-in adoption of an unadmitted
  legacy tombstone or terminal row even when a receipt now exists on that execution ID; a legacy
  incoming never adopts an admitted slot; and `reconcile_late_delivery` refuses an unadmitted
  receipt. Redaction is untouched — the marker is one boolean on the execution store's existing
  receipt, never identity or a digest on the queue side.
- **Correction 6 — settlement fences the recorded process incarnation, not just the PID.**
  `settle_delivery_request` already required the claiming PID and owning profile; it now also
  compares the `sender_started_at` fingerprint recorded at claim time with the live reading for
  this PID through `gateway.status.start_time_fingerprints_match`. A reused PID whose start time
  changed cannot settle a `verified` receipt, and a start time that either side cannot read leaves
  the attempt `sending` — unverifiable is fenced, never assumed equal.

Honest limitations of this implementation: a queue payload that does not carry an anchor (a
legacy-shaped row) can never be reconciled from the receipt alone — only rows this feature
ADMITTED can be, which the receipt's `queue_admitted` marker now enforces rather than assumes;
notification proof stays bounded to 64 chunks; the worker's enqueue waits at most
`DEFAULT_DELIVERY_WAIT_TIMEOUT_SECONDS` and a pending row is deferred to the next gateway rather
than failed; and receipt-bearing rows still retain small identity metadata indefinitely under the
approved retention contract.

Verification actually run (isolated worktree, fakes and temp SQLite only): the new
`tests/cron/test_artifact_queue_integration.py` is 2 passed / 19 failed against the pre-change
code and 21 passed after it; the artifact/queue target suites are 132 passed, 0 failed; and
`tests/cron` plus the Telegram receipt/native/chunk/rich/profile callers is 178 files,
1,803 passed, 0 failed, 12 skipped. `scripts/check` reports 11 checks ok with no new health
waiver and no hook bypass. No live cache, vault, Notion, broker, provider, model, schedule or
message was touched.

Bounded acceptance repair (9 October 2026), same isolated-worktree, fakes-only rules: the four
canonical root probes were 4 failed before and 4 passed after; `tests/cron` is 163 files,
1,633 passed, 0 failed, 12 skipped; the Telegram receipt/native/chunk/rich/profile callers are
141 passed, 0 failed; and `scripts/check` again reports 11 checks ok, 0 blocking and 0 advisory
findings, with no new health waiver and no hook bypass. The four probes are owned regressions in
`tests/cron/test_artifact_queue_integration.py`, alongside the additional cases for a corrupted
stored payload, a `for_failure=True` alert carrying a stale anchor, a contended claim, an old
`failed_certain` pending row after a fresh attempt, and the `failed_certain` no-send proof
requirement. No commit, merge, push, activation, model change or consumer edit was performed.


Admission ordering correction (Codex independent regression): the receipt marker
commits while the queue's admission transaction still holds its write lock, before
the row becomes visible. Matching pending retries repair absent provenance before
returning. A failed marker/readback rolls back admission rather than exposing work.
A marker without a committed queue row is permission only, never delivery proof.

Adoption independently validates the actual notification against its digest and
the current resolved authorized destination against the durable identity, even
for terminal rows. Failure alerts use a copy with artifact authority removed;
their legacy queue outcomes cannot borrow report receipts or affect the caller.

Sender settlement uses the existing fingerprint comparator with tolerance=0; its
liveness default tolerates drift and cannot prove the exact sending incarnation.
An independently reproduced one-unit changed start is refused before verification.
