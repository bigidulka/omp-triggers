import crypto from "node:crypto";
import { normalizeCreate, parseDuration, parseRfc3339, parseCron, expiryDuration, MIN_INTERVAL_MS, publicTask, promptHash, taskNextAt, scheduleError, validatePrompt } from "./schedule-model.mjs";
import { ScheduleStore, stateDirectory } from "./schedule-store.mjs";
import { globalTimers } from "./host-runtime.mjs";

const timestamp = (ms = Date.now()) => new Date(ms).toISOString();
const MAX_IDEMPOTENCY_KEYS = 200;
function taskFor(state, id) {
  if (typeof id !== "string" || !id) throw scheduleError("invalid_schedule", "taskId is required; use the ID returned by create or list");
  const exact = state.tasks.find((item) => item.id === id);
  if (exact) return exact;
  const matches = /^[a-f0-9-]{8,}$/i.test(id) ? state.tasks.filter((item) => item.id.startsWith(id)) : [];
  if (matches.length > 1) throw scheduleError("ambiguous_schedule", "taskId prefix is ambiguous; use the full ID from list");
  if (matches.length === 1) return matches[0];
  throw scheduleError("schedule_not_found", `schedule ${id} was not found; use the taskId returned by create or list in this same session`);
}
function terminalState(task) {
  if (task.deletedAt) return "deleted";
  if (task.terminalState) return task.terminalState;
  if (task.completedAt) return "completed";
  return null;
}
function requireRunnable(task) {
  const state = terminalState(task);
  if (state) throw scheduleError("schedule_terminal", `${state} schedules cannot be run or resumed`);
}
function idempotent(state, key, fingerprint) {
  const entry = state.idempotency[key];
  if (!entry) return null;
  if (entry.fingerprint && entry.fingerprint !== fingerprint) throw scheduleError("idempotency_conflict", "idempotency key was already used with different parameters");
  return entry.result || entry;
}
function rememberIdempotent(state, key, fingerprint, result) {
  if (!key) return;
  state.idempotency[key] = { fingerprint, result, createdAt: timestamp() };
  const keys = Object.keys(state.idempotency);
  for (const stale of keys.slice(0, Math.max(0, keys.length - MAX_IDEMPOTENCY_KEYS))) delete state.idempotency[stale];
}

/** Durable schedule policy and a live-only dispatch adapter. No timer runs while OMP is closed. */
export class Scheduler {
  constructor({ binding, dispatch, isDisabled = process.env.OMP_SESSION_BRIDGE_DISABLE_SCHEDULER === "1", directory = stateDirectory(), clock = () => Date.now(), timers = globalTimers() }) {
    this.binding = binding; this.dispatch = dispatch; this.disabled = isDisabled; this.clock = clock; this.timers = timers; this.store = new ScheduleStore(binding, directory); this.timer = null; this.running = false; this.cancellations = new Map();
  }
  async start() {
    await this.store.start();
    // A process crash/reload can leave a claimed run with no trustworthy
    // acknowledgement boundary. Do not re-submit it; make the limitation
    // visible and let a recurring task compute its next occurrence.
    await this.store.mutate((state) => {
      const pending = new Set();
      for (const run of state.history) {
        if (["queued", "queued_waiting_for_idle", "delivered"].includes(run.state)) { run.state = "failed"; run.error = "runtime_restarted_before_terminal"; run.settledAt = timestamp(this.clock()); pending.add(run.runId); }
      }
      for (const task of state.tasks) {
        if (!pending.has(task.pendingRunId)) continue;
        task.pendingRunId = null;
        if (task.kind === "at") {
          task.enabled = false; task.nextRunAt = null; task.completedAt = timestamp(this.clock()); task.terminalState = "failed"; task.updatedAt = task.completedAt;
        }
      }
    });
    this.running = !this.disabled;
    if (this.running) { await this.reconcile(); this.timer = this.timers.setInterval(() => this.tick().catch(() => {}), 1000); }
    return this;
  }
  async stop() { this.running = false; this.timers.clear(this.timer); this.timer = null; await this.store.close(); }
  status() {
    const tasks = this.store.state?.tasks || []; const active = tasks.filter((task) => task.enabled && !task.paused);
    return { enabled: !this.disabled, activeCount: active.length, totalCount: tasks.length, nextDueAt: active.map((task) => task.nextRunAt).filter(Boolean).sort()[0] || null, capabilities: ["schedule.create", "schedule.list", "schedule.get", "schedule.update", "schedule.delete", "schedule.pause", "schedule.resume", "schedule.run_now", "schedule.history"] };
  }
  ensureEnabled() { if (this.disabled) throw scheduleError("scheduler_disabled", "scheduler is disabled by OMP_SESSION_BRIDGE_DISABLE_SCHEDULER=1"); }
  async create(input) {
    this.ensureEnabled(); const key = input?.idempotencyKey;
    return this.store.mutate((state) => {
      const idempotencyKey = key && `create:${key}`; const fingerprint = JSON.stringify({ ...input, idempotencyKey: undefined });
      const prior = idempotencyKey && idempotent(state, idempotencyKey, fingerprint); if (prior) return prior;
      const normalized = normalizeCreate(input, this.binding, this.clock());
      if (input?.coalesceKey) {
        const existing = state.tasks.find((task) => task.enabled && !terminalState(task) && task.coalesceKey === input.coalesceKey);
        if (existing) {
          const result = { task: publicTask(existing), taskId: existing.id, durable: true, coalesced: true };
          rememberIdempotent(state, idempotencyKey, fingerprint, result);
          return result;
        }
      }
      if (state.tasks.filter((task) => task.enabled && !terminalState(task)).length >= 50) throw scheduleError("schedule_limit", "maximum of 50 active schedules per session");
      state.tasks.push(normalized);
      const result = { task: publicTask(normalized), taskId: normalized.id, durable: true };
      rememberIdempotent(state, idempotencyKey, fingerprint, result);
      return result;
    });
  }
  list() { return (this.store.snapshot().tasks || []).map(publicTask); }
  get(id) { return publicTask(taskFor(this.store.snapshot(), id)); }
  history(id, limit = 20) {
    if (id) id = taskFor(this.store.snapshot(), id).id;
    const n = Number(limit); if (!Number.isSafeInteger(n) || n < 1 || n > 200) throw scheduleError("invalid_schedule", "history limit must be 1..200");
    return this.store.snapshot().history.filter((run) => !id || run.taskId === id).slice(-n).reverse();
  }
  async delete(id) {
    let cancelledRunId;
    const result = await this.store.mutate((state) => {
      const task = taskFor(state, id); task.enabled = false; task.deletedAt = timestamp(this.clock()); task.terminalState = "deleted"; task.updatedAt = task.deletedAt;
      cancelledRunId = task.pendingRunId;
      if (cancelledRunId) {
        const run = state.history.find((entry) => entry.runId === cancelledRunId);
        if (run && !["settled", "failed", "cancelled"].includes(run.state)) { run.state = "cancelled"; run.error = "schedule_deleted"; run.settledAt = task.deletedAt; }
      }
      task.pendingRunId = null; return { taskId: id, deleted: true };
    });
    this.cancellations.get(cancelledRunId)?.(); this.cancellations.delete(cancelledRunId);
    return result;
  }
  async pause(id) { this.ensureEnabled(); return this.store.mutate((state) => { const task = taskFor(state, id); if (terminalState(task)) throw scheduleError("schedule_terminal", "terminal schedules cannot be paused"); task.paused = true; task.updatedAt = timestamp(this.clock()); return { task: publicTask(task) }; }); }
  async resume(id) { this.ensureEnabled(); return this.store.mutate((state) => { const task = taskFor(state, id); requireRunnable(task); task.paused = false; task.enabled = true; const now = this.clock(); task.nextRunAt = task.kind === "at" ? (task.at > now ? timestamp(task.at) : task.nextRunAt) : timestamp(taskNextAt(task, now)); task.updatedAt = timestamp(now); return { task: publicTask(task) }; }); }
  async update(id, changes) {
    return this.store.mutate((state) => {
      const existing = taskFor(state, id); requireRunnable(existing); if (!changes || typeof changes !== "object") throw scheduleError("invalid_schedule", "update must be an object");
      // Validate on a copy: a rejected multi-field update must not leak partial changes.
      const task = structuredClone(existing);
      const now = this.clock();
      const triggerKeys = ["at", "every", "cron"].filter((key) => changes[key] !== undefined && changes[key] !== "");
      if (triggerKeys.length > 1) throw scheduleError("invalid_schedule", "update accepts exactly one of at, every, or cron");
      if (triggerKeys.length) {
        if (task.pendingRunId && task.kind === "at") throw scheduleError("schedule_busy", "cannot reschedule an in-flight one-shot; wait for history to settle");
        const kind = triggerKeys[0];
        if (kind !== task.kind) throw scheduleError("invalid_schedule", "changing trigger kind requires a new schedule");
        if (kind === "at") task.at = parseRfc3339(changes.at);
        if (kind === "every") {
          task.everyMs = parseDuration(changes.every, "every");
          if (task.everyMs < MIN_INTERVAL_MS) throw scheduleError("interval_too_short", "every must be at least 1m");
          task.createdAtMs = now;
        }
        if (kind === "cron") task.cron = parseCron(changes.cron).expression;
      }
      if (changes.prompt !== undefined) task.prompt = validatePrompt(changes.prompt);
      if (changes.expires !== undefined) { if (task.kind === "at") throw scheduleError("invalid_schedule", "one-shot schedules do not expire"); task.expiresAt = timestamp(now + expiryDuration(changes.expires, now)); }
      if (changes.jitter !== undefined) {
        if (task.kind === "at") throw scheduleError("invalid_schedule", "one-shot schedules cannot have jitter");
        const jitter = parseDuration(changes.jitter, "jitter"); const cap = task.kind === "every" ? Math.min(Math.floor(task.everyMs / 2), 30 * 60_000) : 30 * 60_000;
        if (jitter > cap) throw scheduleError("invalid_schedule", `jitter exceeds ${Math.floor(cap / 1000)}s cap`); task.jitterMs = jitter;
      }
      if (task.kind === "every" && task.jitterMs > Math.min(Math.floor(task.everyMs / 2), 30 * 60_000)) throw scheduleError("invalid_schedule", "existing jitter exceeds the new interval cap; update jitter too");
      if (changes.maxQueueWait !== undefined) task.maxQueueWaitMs = parseDuration(changes.maxQueueWait, "maxQueueWait");
      if (changes.coalesceKey !== undefined) {
        if (typeof changes.coalesceKey !== "string" || !/^[a-zA-Z0-9._:-]{1,128}$/.test(changes.coalesceKey)) throw scheduleError("invalid_schedule", "coalesceKey must be 1..128 safe characters");
        if (state.tasks.some((other) => other.id !== task.id && other.enabled && !terminalState(other) && other.coalesceKey === changes.coalesceKey)) throw scheduleError("coalesce_conflict", "another active schedule already uses this coalesceKey");
        task.coalesceKey = changes.coalesceKey;
      }
      if (triggerKeys.length) task.nextRunAt = timestamp(task.kind === "at" ? task.at : taskNextAt(task, now));
      if (changes.enabled !== undefined) task.enabled = Boolean(changes.enabled);
      if (changes.paused !== undefined) task.paused = Boolean(changes.paused);
      task.updatedAt = timestamp(now); Object.assign(existing, task); return { task: publicTask(existing) };
    });
  }
  async runNow(id, idempotencyKey) {
    this.ensureEnabled(); let result;
    await this.store.mutate((state) => {
      const task = taskFor(state, id); requireRunnable(task); const key = idempotencyKey && `run:${id}:${idempotencyKey}`; const fingerprint = JSON.stringify({ taskId: id });
      const prior = key && idempotent(state, key, fingerprint); if (prior) { result = prior; return; }
      if (task.pendingRunId) { result = { taskId: id, runId: task.pendingRunId, coalesced: true, durable: true }; return; }
      const run = this.makeRun(task, "manual", this.clock()); task.pendingRunId = run.runId; state.history.push(run);
      result = { taskId: id, runId: run.runId, durable: true }; rememberIdempotent(state, key, fingerprint, result);
    });
    if (!result.coalesced) this.dispatchRun(result.runId).catch(() => {});
    return result;
  }
  makeRun(task, reason, scheduledAt) { return { runId: crypto.randomUUID(), taskId: task.id, state: "queued_waiting_for_idle", reason, scheduledAt: timestamp(scheduledAt), queuedAt: timestamp(this.clock()), promptHash: promptHash(task.prompt), sessionId: this.binding.sessionId }; }
  async tick() { if (!this.running) return; await this.reconcile(); }
  async reconcile() {
    if (!this.running) return;
    const toDispatch = [];
    await this.store.mutate((state) => {
      const now = this.clock();
      for (const task of state.tasks) {
        if (!task.enabled || task.paused || task.deletedAt) continue;
        if (task.expiresAt && Date.parse(task.expiresAt) <= now) {
          task.enabled = false; task.terminalState = "expired"; task.updatedAt = timestamp(now); state.history.push({ runId: crypto.randomUUID(), taskId: task.id, state: "expired", reason: "expiry", scheduledAt: task.nextRunAt || timestamp(now), settledAt: timestamp(now) }); continue;
        }
        if (task.kind === "at") {
          if (task.nextRunAt && task.at <= now) {
            if (now - task.at > task.oneShotGraceMs) { task.enabled = false; task.nextRunAt = null; task.terminalState = "missed"; task.completedAt = timestamp(now); state.history.push({ runId: crypto.randomUUID(), taskId: task.id, state: "missed", reason: "past_grace", scheduledAt: timestamp(task.at), settledAt: timestamp(now) }); }
            else if (!task.pendingRunId) { const run = this.makeRun(task, "scheduled", task.at); task.pendingRunId = run.runId; task.nextRunAt = null; state.history.push(run); toDispatch.push(run.runId); }
          }
          continue;
        }
        const next = task.nextRunAt ? Date.parse(task.nextRunAt) : taskNextAt(task, now);
        if (next > now) continue;
        // Move recurrence forward from its scheduled time; missed intervals coalesce into this one durable run.
        let future = taskNextAt(task, next); while (future <= now) future = taskNextAt(task, future);
        task.nextRunAt = timestamp(future);
        if (!task.pendingRunId) { const run = this.makeRun(task, "scheduled", next); task.pendingRunId = run.runId; state.history.push(run); toDispatch.push(run.runId); }
        else state.history.push({ runId: crypto.randomUUID(), taskId: task.id, state: "skipped", reason: "coalesced", scheduledAt: timestamp(next), settledAt: timestamp(now) });
      }
      const perTask = new Map();
      state.history = state.history.slice().reverse().filter((entry) => { const count = perTask.get(entry.taskId) || 0; perTask.set(entry.taskId, count + 1); return count < 20; }).reverse().slice(-200);
    });
    for (const runId of toDispatch) this.dispatchRun(runId).catch(() => {});
  }
  async dispatchRun(runId) {
    let task; let run; let dispatchable = false;
    await this.store.mutate((state) => { run = state.history.find((item) => item.runId === runId); task = state.tasks.find((item) => item.id === run?.taskId); if (!run || !task || task.pendingRunId !== runId || run.state !== "queued_waiting_for_idle") return; run.dispatchRequestedAt = timestamp(this.clock()); dispatchable = true; });
    if (!dispatchable || !task || !run) return;
    try {
      const dispatch = this.dispatch(task.prompt, {
        onDelivered: () => this.transition(runId, "delivered"),
        onSettled: (result) => this.complete(runId, "settled", result),
        onFailed: (error) => this.complete(runId, "failed", "", error),
      }, { deliveryPolicy: task.deliveryPolicy || "idle_only", maxQueueWaitMs: task.maxQueueWaitMs, coalesceKey: task.coalesceKey });
      const cancel = typeof dispatch === "function" ? dispatch : dispatch?.cancel;
      if (cancel) this.cancellations.set(runId, cancel);
    } catch (error) { await this.complete(runId, "failed", "", error); }
  }
  async transition(runId, state) { return this.store.mutate((data) => { const run = data.history.find((item) => item.runId === runId); if (run && run.state === "queued_waiting_for_idle") { run.state = state; run.deliveredAt = timestamp(this.clock()); } }); }
  async complete(runId, state, result = "", error) {
    return this.store.mutate((data) => {
      const run = data.history.find((item) => item.runId === runId); if (!run || ["settled", "failed", "cancelled"].includes(run.state)) return;
      run.state = state; run.settledAt = timestamp(this.clock()); run.resultHash = result ? promptHash(result) : undefined; if (error) run.error = error?.code || error?.message || String(error);
      const task = data.tasks.find((item) => item.id === run.taskId); if (!task) return;
      this.cancellations.delete(runId);
      if (task.pendingRunId === runId) task.pendingRunId = null;
      task.lastRunAt = run.settledAt; task.runCount = (task.runCount || 0) + 1;
      if (task.kind === "at") { task.enabled = false; task.completedAt = run.settledAt; task.terminalState = state === "settled" ? "completed" : "failed"; }
    });
  }
}
