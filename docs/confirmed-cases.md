# Confirmed incidents and code risks

The motivating examples were checked against a private user's actual OMP transcript and receipt records. Raw transcripts, channel/account details, payloads and credentials are not published in this repository.

## Observed: webhook instructions could not be edited

A real agent attempted `session_event_write` with `action=update`. The device's action enum rejected it. The agent then deleted and recreated the subscription, changing identity/address just to revise the instruction.

v0.5 implements `trigger_manage get/update` and a compatibility `session_event_write update`. Prompt changes increment a revision while retaining ID, URL, token and pending events. Stale-revision edits fail atomically. The real OMP integration test edits the device and verifies identity, credential and later dispatch revision.

## Observed: a completed reply was labeled uncertain after session disposal

A receipt was durably accepted, later delivered, and the saved transcript contained a final assistant analysis. The host then disposed the session; the old inbox recorded `uncertain / shutdown` because its terminal lifecycle acknowledgment was absent.

v0.5 records `responded` separately from terminal `settled` and retains response evidence across shutdown. Explicit reconciliation can verify a legacy reply from the saved parent chain without rerunning it. It does not retroactively assert external task success or invent a missing terminal event.

## Observed during implementation: leaf ID at message_end refers to the prior message

A real OMP RPC integration test checked the receipt's candidate native ID against saved JSONL and found a user entry. `message_end` occurs before the assistant entry is appended. The adapter now verifies timestamp and response digest against the persisted assistant branch at terminal/shutdown. The integration test asserts the linked entry has role `assistant`.

## Code risks, not demonstrated lost messages

The example Telegram listener treated every HTTP 409 as a duplicate; the receiver actually uses 409 for paused subscriptions and changed-content conflicts. It also advanced its message cursor after failed HTTP attempts, performed blocking HTTP inside an async handler, capped catch-up at 20 messages and attached its live handler after catch-up. Those paths can cause loss, delays or gaps, but the reviewed evidence did not establish an actual lost Telegram message caused by them.

The supplied adapter addresses these paths with an atomic durable outbox/cursor, correct receipt semantics, off-thread HTTP, paginated catch-up, handler-before-catch-up ordering and repeated gap recovery. Tests exercise service failure, paused intake, payload conflict, restart persistence and a catch-up range exceeding 20 messages. A live listener's ready status alone still does not prove delivery of a new real channel alert.

## Reproduced during coexistence testing: a live legacy webhook returned 404

A v0.4 runtime correctly retained its scheduler/inbox ownership, so v0.5.0 deferred that binding's migration. However, the new receiver only looked in SQLite and returned 404 for the deferred webhook. The legacy scheduler itself still ran.

v0.5.1 adds compatibility intake for unimported legacy routes. Migration and intake serialize on the same route lock; admission checks authority again inside that lock. After the old runtime releases ownership, the new runtime imports its definitions and receipt ledger before registration. A regression test verifies old timer execution, old webhook completion, pause/resume, a 20-event concurrent handoff, preserved token/receipt IDs, duplicate suppression and revocation without fallback to stale credentials.

## Observed in an ordinary-language OMP probe: inferred objective and mislabeled time

With no prior task description, an agent given only “set a check every five minutes” inferred a monitoring objective from a file in its working directory and created a task. It also presented a returned UTC due time as Moscow local time. Those are agent-facing behavior defects, distinct from queue execution correctness.

Routing guidance now requires one concise clarification when the objective is absent, prohibits inferring it from arbitrary filenames, and allows a generic current-chat webhook without demanding technical parameters. Prompt edits can identify a trigger from conversation instead of demanding its ID. Tool results include an explicit `nextRunAtLocal` label, with a timezone regression check for Moscow UTC+3. Ordinary-language probes must run in a clean working folder with test plans and controller artifacts outside the agent's readable scope; a contaminated exploratory probe is not accepted as evidence.

## Observed in the live Telegram source: edited message stopped catch-up

The source stopped with `ValueError: source event changed under the same ID`. A bounded readback of those channel messages confirmed Telegram edit timestamps and changed `data.text` compared with the durable outbox. Previously accepted events remained preserved, but the stopped source could not collect more events.

v0.5.2 treats observed Telegram message versions as distinct immutable events with deterministic revision IDs, preserving the initial ID for the first version. It also subscribes to live MessageEdited notifications. Identical observations of an edited version deduplicate; generic outbox callers still reject changed payloads under an immutable ID. The regression covers both repeated edited observations and unchanged generic conflict protection. This does not claim complete recovery of every old edit made while a source was disconnected.
