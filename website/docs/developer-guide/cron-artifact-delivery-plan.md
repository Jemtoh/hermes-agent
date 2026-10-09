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
