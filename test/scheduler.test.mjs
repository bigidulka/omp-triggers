import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { spawnSync } from "node:child_process";
import { parseCron, cronMatches, nextCronAt, normalizeCreate } from "../lib/schedule-model.mjs";
import { Scheduler } from "../lib/scheduler.mjs";

const binding = { sessionId: "immutable-session", sessionFile: "/tmp/immutable.jsonl", cwd: "/tmp/project" };
async function waitFor(predicate) { for (let i = 0; i < 50; i++) { if (predicate()) return; await new Promise((resolve) => setTimeout(resolve, 2)); } assert.fail("condition did not become true"); }
async function fixture(t, options = {}) {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "pi-session-scheduler-")); let now = options.now ?? Date.parse("2026-01-01T00:00:00Z"); const sent = [];
  const scheduler = new Scheduler({ binding, directory, clock: () => now, isDisabled: options.disabled, dispatch: (prompt, callbacks) => { sent.push({ prompt, callbacks }); callbacks.onDelivered?.(); } });
  await scheduler.start(); t.after(async () => { await scheduler.stop(); await fs.rm(directory, { recursive: true, force: true }); });
  return { scheduler, sent, advance: (ms) => { now += ms; }, directory };
}

test("strict cron supports ranges, lists, steps and DOM/DOW OR semantics", () => {
  const cron = parseCron("*/15 9-17 1,15 * 1-5");
  assert.equal(nextCronAt(cron, Date.parse("2026-01-01T08:59:00Z")), Date.parse("2026-01-01T09:00:00Z"));
  // 2026-02-01 is Sunday (DOM only); 2026-02-02 is Monday (DOW only).
  const or = parseCron("0 0 1 * 1");
  assert.equal(cronMatches(or, new Date(2026, 1, 1, 0, 0)), true);
  assert.equal(cronMatches(or, new Date(2026, 1, 2, 0, 0)), true);
  assert.equal(cronMatches(or, new Date(2026, 1, 3, 0, 0)), false);
  for (const expression of ["0xA * * * *", "1e1 * * * *", "+5 * * * *", "*/1e1 * * * *"]) assert.throws(() => parseCron(expression), { code: "invalid_cron" });
  assert.throws(() => parseCron("0 0 L * *"), { code: "invalid_cron" });
  assert.throws(() => parseCron("0 0 * * mon"), { code: "invalid_cron" });
});

test("cron searches leap-day ranges and preserves both DST fallback instants", () => {
  const root = path.resolve(import.meta.dirname, "..");
  const script = `import { parseCron,nextCronAt } from ${JSON.stringify(path.join(root, "lib/schedule-model.mjs"))};\nconst leap=parseCron('0 0 29 2 *');\nconst a=nextCronAt(leap,Date.parse('2096-03-01T00:00:00Z'));\nconst b=nextCronAt(parseCron('30 1 * * *'),Date.parse('2026-11-01T05:30:00Z'));\nconsole.log(JSON.stringify([a,b]));`;
  const result = spawnSync(process.execPath, ["--input-type=module", "-e", script], { env: { ...process.env, TZ: "America/New_York" }, encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  const [leap, fallback] = JSON.parse(result.stdout);
  assert.equal(leap, Date.parse("2104-02-29T05:00:00Z"), "eight-year scan crosses the non-leap 2100 century");
  assert.equal(fallback, Date.parse("2026-11-01T06:30:00Z"));
});

test("one-shot RFC3339 validation rejects calendar and clock normalization", () => {
  for (const at of ["2026-02-30T00:00Z", "2026-04-31T00:00Z", "2026-01-01T24:00Z", "2026-01-01T00:00+24:00"]) {
    assert.throws(() => normalizeCreate({ at, prompt: "x" }, binding), { code: "invalid_schedule" });
  }
});

test("create/idempotency, manual run, history, and reload persistence", async (t) => {
  const { scheduler, sent, directory } = await fixture(t);
  const created = await scheduler.create({ every: "5m", prompt: "check", idempotencyKey: "same" });
  const duplicate = await scheduler.create({ every: "5m", prompt: "check", idempotencyKey: "same" });
  assert.equal(created.taskId, duplicate.taskId);
  await assert.rejects(scheduler.create({ every: "5m", prompt: "different", idempotencyKey: "same" }), { code: "idempotency_conflict" });
  const second = await scheduler.create({ every: "5m", prompt: "other" });
  const run = await scheduler.runNow(created.taskId, "run-key");
  assert.notEqual((await scheduler.runNow(second.taskId, "run-key")).taskId, created.taskId, "run keys are task scoped");
  assert.equal((await scheduler.runNow(created.taskId, "run-key")).runId, run.runId);
  assert.deepEqual(sent.map((entry) => entry.prompt), ["check", "other"]);
  sent[0].callbacks.onSettled("done");
  await scheduler.store.tail;
  assert.equal(scheduler.history(created.taskId)[0].state, "settled");
  await scheduler.stop();
  const reloaded = new Scheduler({ binding, directory, dispatch: () => {}, isDisabled: true }); await reloaded.start();
  assert.equal(reloaded.list()[0].id, created.taskId); assert.equal(reloaded.history(created.taskId)[0].state, "settled"); await reloaded.stop();
});

test("recurring coalesce key creates one durable supervisor cycle", async (t) => {
  const { scheduler } = await fixture(t);
  const first = await scheduler.create({ every: "5m", prompt: "cycle", coalesceKey: "fleet:workspace-a", maxQueueWait: "1h" });
  const second = await scheduler.create({ every: "5m", prompt: "cycle duplicate", coalesceKey: "fleet:workspace-a", maxQueueWait: "1h" });
  assert.equal(second.taskId, first.taskId);
  assert.equal(second.coalesced, true);
  assert.equal(scheduler.list().filter((task) => task.enabled).length, 1);
  assert.equal(scheduler.get(first.taskId).deliveryPolicy, "idle_only");
  assert.equal(scheduler.get(first.taskId).maxQueueWaitMs, 3_600_000);
});

test("coalescing does not bypass schedule validation", async (t) => {
  const { scheduler } = await fixture(t);
  await scheduler.create({ every: "5m", prompt: "valid", coalesceKey: "shared:cycle" });
  await assert.rejects(scheduler.create({ every: "5m", prompt: "invalid", coalesceKey: "bad key" }), { code: "invalid_schedule" });
  await assert.rejects(scheduler.create({ every: "30s", prompt: "invalid", coalesceKey: "shared:cycle" }), { code: "interval_too_short" });
});

test("one shots expire after terminal run and overdue ones are missed after grace", async (t) => {
  const { scheduler, sent, advance } = await fixture(t);
  const task = await scheduler.create({ at: "2026-01-01T00:00:30Z", prompt: "once" });
  advance(30_000); await scheduler.reconcile(); await waitFor(() => sent.length === 1);
  sent[0].callbacks.onSettled("ok"); await scheduler.store.tail;
  assert.equal(scheduler.get(task.taskId).enabled, false);
  const old = await scheduler.create({ at: "2026-01-01T00:01:00Z", prompt: "old", grace: "1m" });
  advance(121_000); await scheduler.reconcile();
  assert.equal(scheduler.history(old.taskId)[0].state, "missed");
  for (const id of [task.taskId, old.taskId]) {
    await assert.rejects(scheduler.runNow(id), { code: "schedule_terminal" });
    await assert.rejects(scheduler.resume(id), { code: "schedule_terminal" });
  }
});

test("reload terminalizes interrupted one-shot", async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "pi-session-reload-"));
  t.after(() => fs.rm(directory, { recursive: true, force: true }));
  const active = new Scheduler({ binding, directory, dispatch: () => {} }); await active.start();
  const task = await active.create({ at: "2026-01-01T00:02:00Z", prompt: "pending" });
  await active.runNow(task.taskId); await active.store.tail; await active.stop();
  const restored = new Scheduler({ binding, directory, isDisabled: true, dispatch: () => {} }); await restored.start();
  assert.equal(restored.history(task.taskId)[0].state, "failed");
  assert.equal(restored.get(task.taskId).enabled, false);
  assert.equal(restored.get(task.taskId).terminalState, "failed");
  await restored.stop();
});

test("delete cancels the dispatched run and terminal schedules stay terminal", async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "pi-session-delete-"));
  t.after(() => fs.rm(directory, { recursive: true, force: true }));
  let cancelled = 0; let dispatched = 0;
  const scheduler = new Scheduler({ binding, directory, dispatch: () => { dispatched++; return () => { cancelled++; }; } }); await scheduler.start();
  const task = await scheduler.create({ every: "1m", prompt: "delete me" });
  const run = await scheduler.runNow(task.taskId);
  await new Promise((resolve) => setImmediate(resolve));
  await scheduler.delete(task.taskId); await scheduler.store.tail;
  assert.equal(dispatched, 1);
  assert.equal(cancelled, 1, "delete invokes the installed bridge cancellation guard");
  assert.equal(scheduler.history(task.taskId).find((entry) => entry.runId === run.runId).state, "cancelled");
  await assert.rejects(scheduler.runNow(task.taskId), { code: "schedule_terminal" });
  await assert.rejects(scheduler.resume(task.taskId), { code: "schedule_terminal" });
  await scheduler.stop();
});

test("terminal schedules cannot be re-enabled through update", async (t) => {
  const { scheduler, sent } = await fixture(t);
  const task = await scheduler.create({ at: "2026-01-01T00:00:30Z", prompt: "once" });
  await scheduler.runNow(task.taskId);
  await waitFor(() => sent.length === 1);
  sent[0].callbacks.onSettled("ok");
  await scheduler.store.tail;
  await assert.rejects(scheduler.update(task.taskId, { enabled: true, prompt: "again" }), { code: "schedule_terminal" });
  assert.equal(scheduler.get(task.taskId).enabled, false);
});

test("updates enforce the shared UTF-8 prompt limit", async (t) => {
  const { scheduler } = await fixture(t);
  const task = await scheduler.create({ every: "1m", prompt: "small" });
  await assert.rejects(scheduler.update(task.taskId, { prompt: "é".repeat(16_385) }), { code: "prompt_too_large" });
});

test("recurring due work coalesces while a run is pending and respects expiry/limits", async (t) => {
  const { scheduler, sent, advance } = await fixture(t);
  const task = await scheduler.create({ every: "1m", prompt: "recurring", expires: "3m" });
  advance(60_000); await scheduler.reconcile(); await waitFor(() => sent.length === 1);
  advance(60_000); await scheduler.reconcile(); await scheduler.store.tail;
  assert.ok(scheduler.history(task.taskId).some((entry) => entry.state === "skipped" && entry.reason === "coalesced"));
  await assert.rejects(scheduler.create({ every: "30s", prompt: "short" }), { code: "interval_too_short" });
});

test("disabled scheduler rejects mutations", async (t) => {
  const { scheduler } = await fixture(t, { disabled: true });
  await assert.rejects(scheduler.create({ every: "1m", prompt: "no" }), { code: "scheduler_disabled" });
});

test("failed scheduler startup releases its session lock", async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "omp-session-start-failure-"));
  t.after(() => fs.rm(directory, { recursive: true, force: true }));
  const initial = new Scheduler({ binding, directory, isDisabled: true, dispatch: () => {} });
  await initial.start();
  await initial.stop();
  await fs.chmod(initial.store.file, 0o644);
  await assert.rejects(new Scheduler({ binding, directory, dispatch: () => {} }).start(), { code: "unsafe_schedule_state" });
  await fs.chmod(initial.store.file, 0o600);
  const recovered = new Scheduler({ binding, directory, isDisabled: true, dispatch: () => {} });
  await recovered.start();
  await recovered.stop();
});

test("real-world trigger updates persist and rejected updates are atomic", async (t) => {
  const { scheduler, directory } = await fixture(t);
  const recurring = await scheduler.create({ every: "6h", prompt: "original" });
  await scheduler.update(recurring.taskId, { every: "2h", maxQueueWait: "15m", coalesceKey: "updated:cycle" });
  assert.equal(scheduler.get(recurring.taskId).every, "7200000ms");
  assert.equal(scheduler.get(recurring.taskId).nextRunAt, "2026-01-01T02:00:00.000Z");
  assert.equal(scheduler.get(recurring.taskId).maxQueueWaitMs, 900_000);
  const cron = await scheduler.create({ cron: "*/15 * * * *", prompt: "cron" });
  await scheduler.update(cron.taskId, { cron: "*/30 * * * *" });
  assert.equal(scheduler.get(cron.taskId).cron, "*/30 * * * *");
  assert.equal(scheduler.get(cron.taskId).nextRunAt, "2026-01-01T00:30:00.000Z");
  const once = await scheduler.create({ at: "2026-01-01T13:45:00Z", prompt: "once" });
  await scheduler.update(once.taskId, { at: "2026-01-01T13:46:30Z" });
  assert.equal(scheduler.get(once.taskId).nextRunAt, "2026-01-01T13:46:30.000Z");
  await assert.rejects(scheduler.update(recurring.taskId, { prompt: "must not leak", every: "30s" }), { code: "interval_too_short" });
  assert.equal(scheduler.get(recurring.taskId).prompt, "original");
  assert.equal(scheduler.get(recurring.taskId).every, "7200000ms");
  await assert.rejects(scheduler.update(recurring.taskId, { every: "1h", cron: "* * * * *" }), { code: "invalid_schedule" });
  await scheduler.stop();
  const restored = new Scheduler({ binding, directory, isDisabled: true, dispatch: () => {} }); await restored.start();
  assert.equal(restored.get(recurring.taskId).every, "7200000ms");
  assert.equal(restored.get(once.taskId).at, "2026-01-01T13:46:30.000Z");
  await restored.stop();
});

test("observed duration mistakes and UUID prefixes have safe supported forms", async (t) => {
  const { scheduler } = await fixture(t);
  const created = await scheduler.create({ every: "PT30M", at: "", cron: "", expires: "2026-01-02T00:00:00Z", prompt: "valid formats" });
  assert.equal(scheduler.get(created.taskId.slice(0, 8)).every, "1800000ms");
  await scheduler.update(created.taskId, { expires: "PT12H" });
  assert.equal(scheduler.get(created.taskId).expiresAt, "2026-01-01T12:00:00.000Z");
  await assert.rejects(scheduler.update(created.taskId, { expires: "2025-01-01T00:00:00Z" }), { code: "invalid_schedule" });
});
