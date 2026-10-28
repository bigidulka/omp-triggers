# OMP Triggers

**Run a saved prompt in the same OMP chat when a signal arrives or a schedule fires.**

Ask your agent in plain language:

> When my Telegram listener sends an alert, analyze it here. Save that instruction so I can edit it later.
>
> Change the instruction for that trigger. Keep the existing webhook address.
>
> Every five minutes, check the build. Keep only one waiting check if this chat is busy.

The agent manages all of these through **`trigger_manage`**. A background Node service owns definitions, prompt revisions, incoming events, schedules and run history in SQLite. The OMP extension delivers one run at a time to its own saved chat. Source scripts only send events; they never select another chat or invoke an LLM.

**Execution requires that saved OMP chat to be open.** The service continues collecting events and scheduling work while OMP is closed, then delivers pending work after resume. It does not launch an unattended agent, change your model/provider, or interrupt an interactive turn.

Formerly `omp-session-bridge`; public package and repository: `omp-triggers`. Existing CLI names, devices, environment variables and state locations remain compatible.

## Install

Requires **Node.js 22.13+** with built-in SQLite, and [Oh My Pi](https://github.com/can1357/oh-my-pi). Integration tests use OMP 18.11.0. Python 3 is needed only for Python clients and source adapters.

```sh
git clone https://github.com/bigidulka/omp-triggers.git
omp plugin link ./omp-triggers
omp
```

For an existing chat, reload the extension. No additional skill or slash-command workflow is required: the plugin registers the device and provides routing guidance automatically. You can name it directly: **“Use OMP Triggers for this task.”**

See [upgrading v0.4](#upgrading-v04) before reloading an older installation.

## Unified agent interface

OMP mounts the tool as `xd://trigger_manage`. Invoke it with the `write` tool and JSON content. There is no session selector: the binding is taken from the executing agent's saved session.

| Action | Purpose |
| --- | --- |
| `create` | Save `name`, trusted `prompt`, `source`, optional queue `policy` |
| `list`, `get` | Inspect definitions; `get` includes prompt and revision |
| `update` | Edit prompt, name, source settings or policy without replacing identity |
| `pause`, `resume`, `delete` | Change admission/delivery state |
| `run` | Enqueue a manual event; optional `idempotencyKey` |
| `history` | Inspect actual run receipts and response evidence |
| `revisions` | Inspect the last 100 definition revisions |
| `reconcile` | Check saved OMP replies for uncertain runs; never replay them |
| `status` | Inspect service capabilities and errors |

Create a webhook:

```json
{"action":"create","name":"telegram-alerts","source":{"type":"webhook"},"prompt":"Analyze this alert in the context of this chat. Explain any action that requires my decision.","policy":"queue"}
```

The result includes `trigger.id`, `revision`, `webhookUrl` and `tokenFile`. The credential itself is never returned. Creating a webhook does **not** start a Telegram or wallet watcher; connect an authorized source adapter to it.

Edit it after reading `get`:

```json
{"action":"update","id":"RETURNED-ID","expectedRevision":1,"prompt":"Analyze the alert and report only meaningful changes."}
```

ID, URL, credential, existing events and history stay intact. A stale `expectedRevision` fails atomically. Queued events use the definition when the runtime claims them; an already-claimed run retains its original revision.

Schedule the same kind of saved prompt:

```json
{"action":"create","name":"build-check","source":{"type":"schedule","every":"5m","expires":"7d"},"prompt":"Check the current build and report a failure or completion.","policy":"coalesce"}
```

Sources: `webhook`, `schedule`, `manual`. A schedule takes exactly one of `at` (RFC3339 with timezone), `every` (minimum `1m`), or numeric five-field `cron` (host timezone). Recurring expiry defaults to seven days and is limited to one year. `jitter` and one-shot `grace` are supported. A source's type is immutable; its settings and prompt can be edited.

## Delivery contract

All sources share one durable queue and **one active dispatch lane per saved chat**. Interactive work has priority. Separate chats may run concurrently.

| Policy | Waiting signals |
| --- | --- |
| `queue` | Preserve each signal; default for webhooks/manual sources |
| `coalesce` | Keep the first waiting signal and attach later receipts to that run; default for schedules |
| `latest` | Mark older waiting runs coalesced and retain the newest signal |

Coalescing does not merge event payloads. Use `queue` when every alert or deposit matters. Missed recurring schedule ticks produce at most one catch-up event per service tick; they never produce an unbounded burst after downtime.

Receipts distinguish:

- `queued`: durably accepted, possibly waiting for the chat to reopen or become idle.
- `claimed`: reserved by one runtime, with a frozen prompt revision.
- `delivered`: OMP accepted the prompt.
- `responded`: a final assistant response was observed; terminal host acknowledgment may be absent.
- `settled`: OMP emitted its terminal `agent_end`.
- `failed`, `uncertain`, `cancelled`, `coalesced`: explicit outcomes, not successful execution.

Neither a model response nor terminal settlement proves an external action succeeded. Runs contain native saved-message identity when verified, timestamp and response digest. Shutdown after an observed response preserves `responded`; it does not silently replay the action. Unconfirmed inflight work becomes `uncertain` on a runtime/service failure. `reconcile` reads the saved session and checks the reply's parent chain, so a later unrelated reply cannot confirm an earlier run.

Defaults: 50 undeleted definitions/chat, 1,000 pending runs/trigger, 10,000 dedup receipts/trigger over seven days, 200 detailed runs/trigger, 24-hour maximum queue wait, five-minute delivered execution timeout. `maxQueueWait` changes the waiting deadline. HTTP intake is limited to 16 KiB; new trusted prompts to 12,000 bytes. No automatic retry of uncertain **agent execution** occurs.

## Source contract

```http
POST /v1/events/TRIGGER-ID
Authorization: Bearer <read privately from tokenFile>
Content-Type: application/json
```

```json
{"eventId":"telegram:channel:message","type":"telegram.alert","data":{"text":"an external signal"}}
```

Only `eventId`, `type`, `data` are accepted. HTTP **202** acknowledges durable intake. Resend identical data with the same stable ID after a timeout; a duplicate returns 202 with `duplicate: true`. Reusing an ID with changed data returns `409 event_conflict`. A paused trigger returns `409 subscription_paused`, **not** a duplicate; retain the event and retry later. Capacity errors return 429.

The receiver binds loopback only (default `127.0.0.1:7341`). Remote providers need an authenticated HTTPS reverse proxy. Do not expose the private `/v1/control` endpoint through that proxy. External JSON is explicitly separated from the trusted instruction in the delivered prompt.

Python:

```python
from omp_events import emit
receipt = emit(webhook_url, token_file,
               event_id="observer:stable-id", event_type="wallet.deposit",
               data={"transaction": "provider-reference"})
```

For a restart-safe source, use `clients/omp_outbox.py`. Capture the event and source cursor in one transaction. Its sender retries network/service/paused failures with the same event ID; authentication and payload conflicts remain blocked for reconciliation. HTTP runs in a worker thread. An advanced cursor means the event is captured **locally**, not acknowledged by OMP.

### Telegram adapter

`clients/telegram_source.py` uses an existing authorized Telethon session factory exposing `authorized_client(profile)`. It embeds no account credentials. Run it in your existing Telethon runtime with an owner-only config:

```json
{
  "profile":"YOUR-REGISTERED-PROFILE",
  "chat":-1000000000000,
  "sessionFactory":"/absolute/path/to/direct_session.py",
  "webhookUrl":"http://127.0.0.1:7341/v1/events/RETURNED-ID",
  "tokenFile":"/private/state/events/RETURNED-ID.token",
  "outbox":"/private/source-state/outbox.sqlite"
}
```

```sh
python3 clients/telegram_source.py --config /private/source-state/source.json
```

Keep the outbox directory `0700` and config `0600`. Optional `transformer` points to a Python file exporting `build_event(chat_id, message)` and returning the event envelope or `None` to filter a message. Domain-specific parsing stays in that adapter. Wallet sources use the same envelope and outbox, with their own chain confirmation/reorg policy.

On first connection the Telegram source starts at the current channel head. Set `startAfter` explicitly to request historical capture. Later starts recover all messages after the durable cursor, with pagination and no fixed 20-message cap. The live handler is attached before catch-up; overlap is deduplicated. Live arrivals never skip the contiguous catch-up cursor. Reconnect gaps are scanned every ten seconds. Structured status shows `ready`, cursor and pending/accepted/blocked counts. The listener is a separate long-running process: supervise it with your existing process manager; linking the OMP plugin alone does not start it at login. Use `--retry-blocked` only after correcting and reconciling a blocked source failure.

## Service and state

The extension autostarts the Node service. `omp-events start|serve|status|stop` provides local lifecycle control; `omp-events emit ID < event.json` sends an event without putting the token in shell arguments.

State root, in priority order:

1. `$OMP_SESSION_BRIDGE_STATE_DIR`
2. `$XDG_STATE_HOME/omp-session-bridge`
3. `~/.local/state/omp-session-bridge`

The root contains `triggers.sqlite` (WAL, synchronous FULL), private service credential, event credentials, migration snapshots and the receiver descriptor/log. Directories are private; credential files are `0600`. Back up the database with SQLite's backup mechanism or stop the service first; copying only the main file while WAL is active is insufficient.

`OMP_SESSION_BRIDGE_EVENT_PORT` changes the listener port (`0` chooses an ephemeral port for tests). `OMP_SESSION_BRIDGE_EVENT_AUTOSTART=0` disables autostart. `OMP_SESSION_BRIDGE_NODE` chooses the standalone Node executable. `OMP_SESSION_BRIDGE_DISABLE_SCHEDULER=1` disables schedule creation/run/claim in the current OMP runtime while preserving webhook/manual delivery.

The runtime Unix registry retains `$OMP_SESSION_BRIDGE_DIR`, `$XDG_RUNTIME_DIR/omp-session-bridge`, then `/tmp/omp-session-bridge-<uid>` as fallbacks. `omp-call list`, `omp-call <target> "prompt"`, and `omp-call <target> schedule ...` remain available. Legacy `session_schedule_*`, `session_event_*`, `/schedule` and `/loop` interfaces are compatibility adapters to the same trigger service.

## Upgrading v0.4

1. Close or reload old OMP runtimes before reattaching those chats. Already-running v0.4 schedulers retain their ownership; their files are deferred, rather than migrated underneath them.
2. Stop the old receiver with `omp-events stop`, wait for it to exit, then `omp-events start` using the new version. Port, webhook IDs and token files are retained.
3. Reload/resume each saved OMP chat. No model/provider/auth reset is required.
4. Inspect `trigger_manage status`, `list` and `history`. Use `reconcile` for ambiguous legacy runs with a saved final reply.

Nonempty legacy JSON is copied to an owner-only `migration-v1-backup` snapshot before transactional import. Imported files are recorded once. Empty schedule files are tracked by size/mtime; live ownership is rechecked for changed data. Old files remain for recovery, but are no longer authoritative for migrated definitions. Do not intentionally run a cached v0.4 extension against a migrated binding; reload it first. Existing events, receipt IDs, prompts, dedup fingerprints and tokens are preserved. Inflight legacy events without confirmation remain uncertain. The service will not kill unrelated OMP runtimes to perform migration.

## Verification and design

```sh
npm test
npm pack --dry-run
```

The suite includes real OMP RPC processes with a deterministic local OpenAI-compatible model stub: agent tool creation/editing, webhook delivery, saved reply ID verification, offline queue, offline schedule, same-session resume and duplicate suppression. It also tests migration, source outbox failures, binding isolation, revisions and lifecycle interruption. These checks do not establish third-party Telegram/wallet delivery or production LLM/provider availability.

See [architecture](docs/architecture.md), [public project comparison](docs/related-projects.md), and [confirmed incident distinctions](docs/confirmed-cases.md).

MIT licensed.
