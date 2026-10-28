import crypto from "node:crypto";
import { TriggerClient } from "./trigger-client.mjs";
import { globalTimers } from "./host-runtime.mjs";
import { BridgeError } from "./protocol.mjs";

/** Thin OMP adapter. All durable decisions and timers belong to the service. */
export class TriggerRuntime {
  constructor({ binding, directory, timers = globalTimers(), canDispatch, dispatch }) {
    this.binding = binding; this.client = new TriggerClient({ binding, directory }); this.timers = timers;
    this.canDispatch = canDispatch; this.dispatch = dispatch; this.closed = false; this.lastError = null;
    this.operations = Promise.resolve(); this.definitions = []; this.scheduler = new ScheduleCompatibility(this);
  }
  async start() {
    await this.client.connect(); await this.client.call("register", { pid: process.pid }); this.registered = true; await this.refresh();
    this.timer = this.timers.setInterval(() => this.tick(), 250);
  }
  async refresh() { this.definitions = (await this.client.call("list")).triggers; }
  async manage(params) {
    if (process.env.OMP_SESSION_BRIDGE_DISABLE_SCHEDULER === "1" && ["create", "update", "run", "resume"].includes(params.action)) {
      const type = params.action === "create" ? params.source?.type : (await this.client.call("get", { id: params.id })).trigger.source.type;
      if (type === "schedule") throw new BridgeError("scheduler_disabled", "schedule execution is disabled for this runtime");
    }
    const result = await this.client.call(params.action || "list", params);
    if (["create", "update", "pause", "resume", "delete"].includes(params.action)) await this.refresh();
    return result;
  }
  track(fn) {
    this.operations = this.operations.catch(() => {}).then(fn).catch((e) => { this.lastError = { code: e.code || "runtime_error", message: e.message }; });
    return this.operations;
  }
  ack(id, state, metadata) { return this.track(() => this.client.call("ack", { id, state, metadata })); }
  async tick() {
    if (this.closed || this.polling) return;
    this.polling = (async () => {
      try {
        if (!this.lastHeartbeat || Date.now() - this.lastHeartbeat > 5000) {
          await this.client.call("heartbeat"); this.lastHeartbeat = Date.now(); await this.refresh();
        }
        if (!this.canDispatch()) return;
        const { run } = await this.client.call("claim", { schedulesEnabled: process.env.OMP_SESSION_BRIDGE_DISABLE_SCHEDULER !== "1" });
        if (!run) return;
        if (this.closed) { await this.client.call("ack", { id: run.id, state: "failed", metadata: { error: { code: "shutdown_before_delivery" } } }); return; }
        this.dispatch(run.prompt, {
          onDelivered: () => this.ack(run.id, "delivered"),
          onResponse: (message) => this.ack(run.id, "responded", { message }),
          onSettled: () => this.ack(run.id, "settled"),
          onFailed: (e) => this.ack(run.id, ["delivery_failed", "queue_full", "queue_timeout", "shutdown_before_delivery", "agent_error", "agent_aborted"].includes(e?.code) ? "failed" : "uncertain", { error: { code: e?.code || "runtime_error", message: e?.message } }),
        }, { source: "trigger", runId: run.id });
        this.lastError = null;
      } catch (e) {
        this.lastError = { code: e.code || "runtime_error", message: e.message };
        // A daemon restart loses leases, never replay inflight work. Registering
        // again only allows new queued work after the OMP lane becomes idle.
        if (e.code === "runtime_not_registered") await this.client.call("register", { pid: process.pid }).catch(() => {});
      }
    })();
    try { await this.polling; } finally { this.polling = null; }
  }
  async close() {
    this.closed = true; this.timers.clear(this.timer);
    await this.polling; await this.operations;
    if (!this.registered) return;
    await this.client.call("release", { reason: "runtime_closed" }).catch((e) => { this.lastError = { code: e.code || "close_failed", message: e.message }; });
  }
}

// Old devices and Unix clients continue to work, but share the same definitions
// and delivery path as trigger_manage. They do not own a second scheduler.
class ScheduleCompatibility {
  constructor(runtime) { this.runtime = runtime; this.binding = runtime.binding; }
  task(t) {
    const s = t.schedule || {};
    return { ...s, id: t.id, prompt: t.prompt, enabled: t.enabled && !t.deletedAt, paused: !t.enabled, deletedAt: t.deletedAt,
      nextRunAt: t.nextRunAt, runCount: t.runCount, maxQueueWaitMs: t.maxQueueWaitMs, coalesceKey: t.name.startsWith("schedule-") ? null : t.name,
      at: s.kind === "at" ? new Date(s.at).toISOString() : undefined, every: s.kind === "every" ? `${s.everyMs}ms` : undefined };
  }
  list() { return this.runtime.definitions.filter((t) => t.source.type === "schedule").map((t) => this.task(t)); }
  async get(id) { return this.task((await this.runtime.client.call("get", { id })).trigger); }
  status() {
    const tasks = this.list();
    return { enabled: true, activeCount: tasks.filter((t) => t.enabled).length, totalCount: tasks.length, nextDueAt: tasks.map((t) => t.nextRunAt).filter(Boolean).sort()[0] || null, capabilities: ["at", "every", "cron", "shared_service"] };
  }
  async create(input) {
    const source = Object.fromEntries(Object.entries(input).filter(([k, v]) => ["at", "every", "cron", "expires", "jitter", "grace"].includes(k) && v !== undefined));
    const name = input.coalesceKey || `schedule-${input.idempotencyKey ? crypto.createHash("sha256").update(input.idempotencyKey).digest("hex").slice(0, 16) : crypto.randomUUID().slice(0, 8)}`;
    const result = await this.runtime.manage({ action: "create", name, source: { type: "schedule", ...source }, prompt: input.prompt, policy: "coalesce", idempotencyKey: input.idempotencyKey, maxQueueWait: input.maxQueueWait });
    return { taskId: result.trigger.id, task: this.task(result.trigger), coalesced: Boolean(result.duplicate) };
  }
  async update(id, input) {
    const { trigger } = await this.runtime.client.call("get", { id });
    const sourceKeys = ["at", "every", "cron", "expires", "jitter", "grace"];
    const changed = Object.fromEntries(Object.entries(input).filter(([k, v]) => sourceKeys.includes(k) && v !== undefined));
    let source;
    if (Object.keys(changed).length) {
      source = { ...trigger.source, ...changed };
      const newKind = ["at", "every", "cron"].filter((k) => changed[k] !== undefined);
      if (newKind.length > 1) throw new Error("provide exactly one schedule kind");
      if (newKind.length) for (const key of ["at", "every", "cron"]) if (key !== newKind[0]) delete source[key];
    }
    const result = await this.runtime.manage({ action: "update", id: trigger.id, expectedRevision: trigger.revision, prompt: input.prompt, source, maxQueueWait: input.maxQueueWait });
    return { taskId: result.trigger.id, task: this.task(result.trigger) };
  }
  async state(id, action) { const result = await this.runtime.manage({ action, id }); return { taskId: result.trigger.id, task: this.task(result.trigger), ...(action === "delete" ? { deleted: true } : {}) }; }
  pause(id) { return this.state(id, "pause"); }
  resume(id) { return this.state(id, "resume"); }
  delete(id) { return this.state(id, "delete"); }
  async runNow(id, idempotencyKey) { const { run } = await this.runtime.manage({ action: "run", id, idempotencyKey }); return { taskId: run.triggerId, runId: run.id, coalesced: run.coalesced }; }
  async history(id, limit) { return (await this.runtime.client.call("history", { id, limit })).runs.map((r) => ({ ...r, taskId: r.triggerId, runId: r.id, queuedAt: r.acceptedAt })); }
  async stop() { /* The adapter is closed by TriggerRuntime, not by a second owner. */ }
}
