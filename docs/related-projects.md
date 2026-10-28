# Public project comparison

Primary documentation reviewed on 2026-10-28. This is a comparison of documented models, not a runtime benchmark, exhaustive feature audit or endorsement. Implementations and docs may change.

| Project / primary source | Documented model | Applied here |
| --- | --- | --- |
| [OpenClaw automations](https://docs.openclaw.ai/automation/cron-jobs) | Persistent scheduler with agent wakeups, lifecycle/history, chat or webhook output; schedules and external integrations share an automation framework. | Keep durable trigger state outside the interactive extension, and separate admission from execution evidence. OMP Triggers currently waits for an open chat; it does not reproduce OpenClaw's unattended wakeup machinery. |
| [Hermes scheduled tasks](https://hermes-agent.nousresearch.com/docs/user-guide/features/cron) | One action-based `cronjob_manage` tool; plain-language create/edit/pause/run, scheduled and webhook-fired jobs, fresh execution sessions and origin-chat output. | One agent-facing facade and saved editable instructions. OMP delivery reuses the bound saved chat instead of creating fresh sessions. Model/provider selection remains with OMP. |
| [Activepieces webhook triggers](https://www.activepieces.com/docs/build-pieces/piece-reference/triggers/webhook-trigger) | A connector lifecycle includes enable, optional handshake, payload conversion and disable; real trigger verification requires an actual published-flow event and run inspection. | Provider subscription lifecycle belongs in the source adapter. Returning a webhook URL alone does not demonstrate that monitoring is active. |
| [n8n Webhook node](https://docs.n8n.io/integrations/builtin/core-nodes/n8n-nodes-base.webhook/) | External HTTP starts a workflow; separate test/production URLs, authentication options and execution inspection. | Stable authenticated event entry and observable receipts. This plugin deliberately exposes one loopback endpoint, without n8n's editor, publishing or separate test routing. |
| [Inngest events and triggers](https://www.inngest.com/docs/durable-execution/guides-and-advanced/events-and-triggers) | App actions, provider webhooks and schedules start durable workflow runs; producers send events rather than calling every function. | Normalize sources into one event/run path. Producer code does not know OMP runtime details. |
| [Temporal Workflow Execution](https://docs.temporal.io/workflow-execution) | Durable execution state and history, workflow-local state, signals and external activities. | Separate definition from execution and record uncertain boundaries. This plugin does not implement Temporal replay, distributed workers or durable arbitrary agent steps. |
| [CloudEvents specification](https://github.com/cloudevents/spec) | A common event-data description for interoperability across services and transports. | Use a stable producer ID, explicit event type and structured data. The current `{eventId,type,data}` envelope is a small private contract, not a claim of CloudEvents compliance. |

## Resulting choices

The smallest useful core for this plugin is **Definition + Revision + Event + Run + RuntimeBinding**. HTTP, clock ticks and adapter notifications all become the same kind of event. Queue policy is explicit, while prompt editing is independent of transport credentials. The agent manages these objects in natural language through one tool.

SQLite WAL is sufficient for a local service with short atomic transactions. A full workflow orchestrator would add deployment and operational complexity without resolving OMP-specific lifecycle attribution. If distributed workers, unattended inference, multi-step durable activities or cross-host ownership become requirements, re-evaluate an existing execution engine rather than extending the local queue into one implicitly.

A provider adapter still needs its own correctness contract: Telegram catch-up and stable message IDs differ from wallet confirmations and reorgs. The facade hides transport mechanics from prompt management; it cannot remove those source-specific responsibilities.
