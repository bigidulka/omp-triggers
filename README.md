# OMP Triggers

**Wake your OMP agent in the same chat — on a schedule or when an external event arrives.**

`omp-triggers` adds durable prompt schedules and incoming webhooks to [Oh My Pi](https://github.com/can1357/oh-my-pi). A Telegram listener, wallet observer, build script or another service can send a signal for the agent to analyze using the context already in that chat.

- **Time triggers:** one-shot, interval and cron prompts.
- **Event triggers:** authenticated webhooks with a persistent inbox, stable event IDs and delivery receipts.
- **Same-chat binding:** subscriptions belong to the current saved session; senders cannot select a different chat.
- **Local IPC:** send prompts from a script through `omp-call` and a private Unix socket.

The extension uses OMP APIs and never reads or writes session JSONL directly. Analysis runs while the saved chat is open and waits for the current turn to finish. The separate webhook receiver can keep accepting events while OMP is closed.

Formerly named `omp-session-bridge`. Existing tool names, CLI commands, environment variables and on-disk state paths are retained for compatibility.

## Install

Requires Node.js 22+ and OMP. The real OMP integration suite is verified with OMP 18.11.0. Python 3 is optional for source listeners and required by the full test suite.

```sh
git clone https://github.com/bigidulka/omp-triggers.git
omp plugin link ./omp-triggers
omp
omp-call list
omp-call <session-id-or-name> "summarize today's build failures"
```
For direct socket use, `omp-call --socket /path/to/bridge.sock --prompt 'summarize the latest turn'` is equivalent to the positional prompt form.

The socket is published on `session_start`, removed on `session_shutdown`, and rebound across session switch, branch, and tree transitions. Prompt calls are FIFO and resolve on terminal OMP `agent_end`.

## Ask the agent

After installation, start OMP or run `/reload` in an existing saved chat. For example:

> Use OMP Triggers (`omp-triggers`). Create a webhook on this chat for my Telegram alert listener, and inspect its event receipts.

The agent uses `session_event_write` / `session_event_read` for incoming events, and `session_schedule_write` / `session_schedule_read` for schedules. No separate skill is required; the plugin registers its tools and injects their usage guidance automatically. The tools are also callable through OMP devices such as `write path=xd://session_event_write`.

## Scheduler

Schedules are prompts delivered to the exact saved TUI session, never shell commands or subagent workflows. Durable ownership is anchored to `(canonical sessionFile, cwd)`; `sessionId` is runtime metadata refreshed on resume.

State location:

1. `$OMP_SESSION_BRIDGE_STATE_DIR`
2. `$XDG_STATE_HOME/omp-session-bridge`
3. `~/.local/state/omp-session-bridge`

Runtime socket registry:

1. `$OMP_SESSION_BRIDGE_DIR`
2. `$XDG_RUNTIME_DIR/omp-session-bridge`
3. `/tmp/omp-session-bridge-<uid>`


Scheduled delivery is `idle_only`: a due prompt waits until the current OMP turn is idle and never interrupts it. Queue waiting and delivered execution use separate deadlines; `--max-queue-wait 1h` controls only the idle wait. `--coalesce-key fleet:<workspace>` guarantees one active recurring cycle per saved session/key and returns the existing task on duplicate creation.
Directories are `0700`; state and registry files are `0600`. Definitions survive `/reload`, `/quit`, and `omp --resume`. The scheduler is in-process and does not run while OMP is closed.

```sh
omp-call <target> schedule list
omp-call <target> schedule add --at '2026-12-25T09:00:00Z' --prompt 'Prepare the release checklist'
omp-call <target> schedule add --every 5m --prompt 'Check the build status'
omp-call <target> schedule add --cron '0 9 * * 1-5' --prompt 'Review priorities' --expires 7d --jitter 30s --max-queue-wait 1h --coalesce-key workspace:review
omp-call <target> schedule show TASK_ID
omp-call <target> schedule pause TASK_ID
omp-call <target> schedule resume TASK_ID
omp-call <target> schedule run-now TASK_ID --idempotency-key manual-2026-01-01
omp-call <target> schedule history TASK_ID --json
```

`at` accepts RFC3339. `every` accepts integer durations such as `1m`, `2h`, and `7d`, with a one-minute minimum. `cron` is strict numeric five-field Vixie syntax. Recurring schedules expire after seven days by default; maximum expiry is one year. Missed recurring occurrences coalesce into one pending run. Maximum 50 active schedules per saved session.

Set `OMP_SESSION_BRIDGE_DISABLE_SCHEDULER=1` to disable schedule creation and `run_now` while preserving external IPC.

## Agent tools

OMP exposes two compact current-session tools:

- `session_schedule_read`: `identity`, `list`, `get`, `history`
- `session_schedule_write`: `create`, `update`, `delete`, `pause`, `resume`, `run_now`

They have no session selector. The agent is bound to its current saved session and cannot address another session through these tools.

TUI commands:

```text
/schedule list
/schedule pause TASK_ID
/schedule resume TASK_ID
/schedule delete TASK_ID
/schedule run-now TASK_ID
/loop 5m review the build status
/loop list
/loop stop TASK_ID
```

## Clients

`omp-call` is dependency-free:

```sh
omp-call list
omp-call --name my-session ping
omp-call --session-id SESSION_ID status --json
omp-call /absolute/path/session.jsonl "Do the next task"
printf 'Do the next task\n' | omp-call --session-id SESSION_ID --stdin
```

Python client:

```python
from omp_session_bridge import schedule_create, schedule_list, schedule_run_now
created = schedule_create(session_id="full-live-session-id", every="5m", prompt="Check build")
print(schedule_list(session_id="full-live-session-id"))
print(schedule_run_now(created["taskId"], session_id="full-live-session-id"))
```

Protocol version remains `1`: `ping`, `status`, `prompt`, and `schedule.create|list|get|update|delete|pause|resume|run_now|history`.

## Security

No TCP listener. Registry and state lookup validate owner, mode, type, and symlinks. A same-UID local process can access another same-UID Unix socket, so this is a local convenience boundary, not hostile-user isolation.

## Development

```sh
npm test
python3 -m py_compile clients/omp_session_bridge.py
npm pack --dry-run
```

## Incoming event webhooks (v0.4)

An agent can subscribe **its current saved chat** to Telegram alerts, wallet-deposit observations or any other external script signal. No target session ID is supplied by the agent or by the sender. The route is bound to the canonical saved-session file and cwd, including after resume assigns a new runtime ID.

Create from OMP with the `write` tool:

```json
{
  "path": "xd://session_event_write",
  "content": "{\"action\":\"create\",\"name\":\"telegram-alerts\",\"instruction\":\"Analyze the incoming Telegram signal and report whether it needs attention.\"}"
}
```

The result contains `subscription.id`, `webhookUrl`, and `subscription.tokenFile`. The Bearer credential is saved only in that owner-only file; tool responses do not print the token. Repeating the same name/instruction returns the same subscription. A changed instruction under the same name is rejected rather than silently replacing an existing route.

The first subscription starts a detached local Node receiver on `127.0.0.1:7341`. This receiver remains available after OMP closes; queued events are saved before HTTP `202` is returned. It is not installed as an OS service and does not auto-start after a machine reboot. `OMP_SESSION_BRIDGE_EVENT_PORT` chooses another port; `OMP_SESSION_BRIDGE_EVENT_AUTOSTART=0` disables automatic launch. For a remote sender, expose the loopback receiver through your separately configured HTTPS reverse proxy/tunnel and preserve the Bearer header. The plugin does not publish a public endpoint.

Inspect and manage from the same agent:

```text
write path=xd://session_event_read content={"action":"list"}
write path=xd://session_event_read content={"action":"status"}
write path=xd://session_event_read content={"action":"history","subscriptionId":"RETURNED_ID","limit":20}
write path=xd://session_event_write content={"action":"pause","subscriptionId":"RETURNED_ID"}
write path=xd://session_event_write content={"action":"resume","subscriptionId":"RETURNED_ID"}
write path=xd://session_event_write content={"action":"delete","subscriptionId":"RETURNED_ID"}
```

A source listener posts JSON `{ "eventId": "telegram:channel:message-id", "type": "telegram.alert", "data": { "text": "signal" } }` to the returned URL using `Authorization: Bearer TOKEN`. `eventId` must be stable and unique at the source. Wallet listeners can use chain + transaction hash + log index, and put confirmation/reorg information into `data`. Telegram access, chain subscription and financial authorization belong to the source integration; creating an incoming route does not itself start a Telegram client, perform transfers or authorize trading.

A local listener can avoid handling the token itself:

```sh
node /absolute/path/to/omp-triggers/bin/omp-events.mjs emit RETURNED_ID < event.json
```

Or import `clients/omp_events.py` from a Python listener:

```python
from omp_events import emit
receipt = emit(webhook_url, token_file,
               event_id=f"telegram:{channel_id}:{message_id}",
               event_type="telegram.alert", data={"text": text})
```

`node bin/omp-events.mjs start|serve|status|stop` manages the local receiver. Receiver metadata and subscription credentials live under the bridge state directory's `events/` subdirectory. CLI processes should use the same state-directory environment as OMP. `OMP_SESSION_BRIDGE_EVENT_DIR` is available for standalone receiver/CLI overrides; agent tools return their exact credential location and URL.

Delivery states:

- `queued`: accepted on disk; waiting for the bound OMP session and an idle turn.
- `dispatching`: a durable claim was recorded before calling OMP.
- `delivered`: OMP accepted the prompt, with event data separated from the subscription instruction.
- `settled`: the corresponding terminal OMP turn ended. This is not proof of a successful external action.
- `uncertain`: timeout, interruption or runtime restart prevented trustworthy completion. No automatic replay is attempted; inspect the chat and source before taking further action.
- `failed`: a definite pre-delivery failure; `cancelled`: a queued event's subscription was deleted.

Identical retries return the existing receipt; an existing `eventId` with changed data returns `409`. Deduplication lasts seven days after acceptance, and never expires while an event remains pending. Each subscription admits up to 1,000 pending events and 10,000 deduplication entries in the retention window, with a 16 KiB event-body limit. Capacity exhaustion returns `429` without accepting the new event. Up to 100 terminal event details are retained alongside pending events. Paused routes reject new events and retain their existing queue. Deletion revokes the credential and cancels undelivered queued events; an already delivered agent turn cannot be undone.

Signals wait for the current turn to end. The event inbox normally polls at 250 ms while OMP is running. Closing OMP preserves intake through the separate receiver, but analysis waits until that saved chat is resumed. Work is serialized with existing scheduled/external prompts.

### Scheduler corrections in v0.4

- `update` changes and persists `at`, `every`, `cron`, `maxQueueWait` and `coalesceKey` instead of returning success with the old values. Failed multi-field updates leave the previous task intact. Trigger-kind changes require a new schedule; in-flight one-shots cannot be rescheduled.
- `expires` accepts either a duration or an explicit future RFC3339 deadline, bounded to one year. ISO durations such as `PT30M` and `PT12H` are accepted. `at` still requires an explicit timezone.
- Empty optional trigger strings are ignored. Read defaults to `list`; write defaults to `create`; `cancel` aliases `delete`. Task operations accept an unambiguous UUID prefix of at least eight characters. `history` accepts limits up to 200.
- Existing per-session controller ownership, scheduler state and provider settings are preserved. A running OMP instance must reload the extension or resume the saved chat to load new tools/code.
