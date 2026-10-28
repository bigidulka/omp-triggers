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
