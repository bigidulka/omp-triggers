import assert from "node:assert/strict";
import test from "node:test";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { TriggerStore } from "../lib/trigger-store.mjs";
import { EventStore, eventDirectory } from "../lib/event-store.mjs";
import { ScheduleStore } from "../lib/schedule-store.mjs";
import { normalizeCreate } from "../lib/schedule-model.mjs";
import { migrateLegacy } from "../lib/trigger-migration.mjs";
import { reconcileRuns } from "../lib/trigger-reconcile.mjs";

async function fixture(t) {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "omp-triggers-"));
  let now = Date.now(); const store = new TriggerStore(directory, { clock: () => now });
  t.after(async () => { store.close(); await fs.rm(directory, { recursive: true, force: true }); });
  const binding = { sessionFile: path.join(directory, "saved.jsonl"), cwd: directory };
  return { store, binding, directory, advance: (ms) => { now += ms; } };
}
const input = (name = "signal") => ({ name, source: { type: "webhook" }, prompt: "Analyze the signal" });
async function token(trigger) { return (await fs.readFile(trigger.tokenFile, "utf8")).trim(); }
const event = (id, data = { value: 1 }) => ({ eventId: id, type: "test.signal", data });

test("editing pins revisions at dispatch and preserves route, token and queued events", async (t) => {
  const { store, binding } = await fixture(t);
  const { trigger: initial } = store.create(binding, input()); const secret = await token(initial);
  store.accept(initial.id, secret, event("one"));
  store.register(binding, "owner", process.pid); const first = store.claim(binding, "owner");
  assert.equal(first.revision, 1);
  store.accept(initial.id, secret, event("two"));
  const { trigger: edited } = store.update(binding, { id: initial.id, prompt: "New trusted instruction", expectedRevision: 1 });
  assert.equal(edited.id, initial.id); assert.equal(await token(edited), secret); assert.equal(edited.revision, 2);
  assert.throws(() => store.update(binding, { id: initial.id, prompt: "stale", expectedRevision: 1 }), /current revision is 2/);
  assert.equal(store.claim(binding, "owner"), null);
  store.acknowledge(binding, "owner", first.id, "delivered"); store.acknowledge(binding, "owner", first.id, "settled");
  const second = store.claim(binding, "owner"); assert.equal(second.revision, 2); assert.match(second.prompt, /New trusted instruction/);
  assert.match(first.prompt, /Analyze the signal/); assert.equal(store.versions(binding, initial.id).length, 2);
});

test("canonical dedup and payload conflicts survive SQLite service restart", async (t) => {
  const f = await fixture(t), { store, binding } = f;
  const { trigger } = store.create(binding, input()); const secret = await token(trigger);
  const receipt = store.accept(trigger.id, secret, event("__proto__", { b: 2, a: 1 }));
  assert.equal(store.accept(trigger.id, secret, event("__proto__", { a: 1, b: 2 })).duplicate, true);
  assert.throws(() => store.accept(trigger.id, secret, event("__proto__", { a: 3 })), /different content/);
  store.close();
  const reopened = new TriggerStore(f.directory); store.db = reopened.db;
  assert.equal(store.accept(trigger.id, secret, event("__proto__", { a: 1, b: 2 })).id, receipt.id);
});

test("service restart and late heartbeat cannot hand a live OMP lane to another runtime", async (t) => {
  const { store, binding, directory, advance } = await fixture(t);
  const { trigger } = store.create(binding, input());
  store.accept(trigger.id, await token(trigger), event("active"));
  store.register(binding, "live-owner", process.pid);
  const active = store.claim(binding, "live-owner");
  advance(60_000); store.tick();
  assert.throws(() => store.register(binding, "other-owner", process.pid), /already has a live/);
  store.close(); const reopened = new TriggerStore(directory); store.db = reopened.db;
  assert.equal(store.history(binding, trigger.id)[0].state, "uncertain");
  assert.throws(() => store.register(binding, "other-owner", process.pid), /already has a live/);
  store.acknowledge(binding, "live-owner", active.id, "settled");
  assert.equal(store.history(binding, trigger.id)[0].state, "settled");
  store.release(binding, "live-owner"); store.register(binding, "other-owner", process.pid);
  assert.equal(store.claim(binding, "other-owner"), null);
});

test("schedule ticks enqueue while OMP is absent, coalesce catch-up and share the webhook lane", async (t) => {
  const { store, binding, advance } = await fixture(t);
  const schedule = store.create(binding, { name: "clock", prompt: "Check status", source: { type: "schedule", every: "1m" } }).trigger;
  const webhook = store.create(binding, input()).trigger;
  advance(61_000); store.tick();
  advance(600_000); store.tick();
  assert.equal(store.history(binding, schedule.id).length, 1);
  store.accept(webhook.id, await token(webhook), event("external"));
  store.register(binding, "runtime", process.pid);
  const clock = store.claim(binding, "runtime"); assert.equal(clock.triggerId, schedule.id);
  assert.equal(store.claim(binding, "runtime"), null);
  store.acknowledge(binding, "runtime", clock.id, "settled");
  assert.equal(store.claim(binding, "runtime").triggerId, webhook.id);
  assert.equal(store.history(binding, schedule.id)[0].revision, 1);
});

test("observed final response survives host disposal without falsely claiming terminal settlement", async (t) => {
  const { store, binding } = await fixture(t); const { trigger } = store.create(binding, input());
  store.accept(trigger.id, await token(trigger), event("reply")); store.register(binding, "runtime", process.pid);
  const run = store.claim(binding, "runtime"); store.acknowledge(binding, "runtime", run.id, "delivered");
  store.acknowledge(binding, "runtime", run.id, "responded", { message: { messageId: "native-leaf", stopReason: "stop", textHash: "digest" } });
  store.acknowledge(binding, "runtime", run.id, "uncertain", { error: { code: "shutdown" } }); store.release(binding, "runtime");
  const [receipt] = store.history(binding, trigger.id);
  assert.equal(receipt.state, "responded"); assert.equal(receipt.settledAt, undefined); assert.equal(receipt.response.messageId, "native-leaf");
  store.register(binding, "new-runtime", process.pid); assert.equal(store.claim(binding, "new-runtime"), null);
});

test("pause, latest, deletion and binding isolation are transactional", async (t) => {
  const { store, binding } = await fixture(t);
  const trigger = store.create(binding, { ...input(), policy: "latest" }).trigger; const secret = await token(trigger);
  const one = store.accept(trigger.id, secret, event("one")); store.accept(trigger.id, secret, event("two"));
  assert.equal(store.history(binding, trigger.id).find((r) => r.id === one.id).state, "coalesced");
  store.setState(binding, trigger.id, "pause");
  assert.throws(() => store.accept(trigger.id, secret, event("three")), /paused/);
  assert.equal(store.accept(trigger.id, secret, event("two")).duplicate, true);
  assert.throws(() => store.get(trigger.id, { ...binding, cwd: "/other" }), /not found/);
  store.setState(binding, trigger.id, "delete");
  assert.equal(store.history(binding, trigger.id).filter((r) => r.state === "cancelled").length, 1);
  assert.throws(() => store.accept(trigger.id, secret, event("three")), /credential/);
});

test("legacy migration preserves ID, token, dedup receipts and schedules with a private backup", async (t) => {
  const { store, binding, directory } = await fixture(t);
  const legacy = new EventStore(eventDirectory(directory));
  const route = await legacy.create(binding, { name: "legacy-hook", instruction: "Keep this prompt" });
  const secret = (await fs.readFile(route.tokenFile, "utf8")).trim();
  const accepted = await legacy.accept(route.id, secret, event("old-event"));
  const schedules = new ScheduleStore(binding, directory); await schedules.start();
  const task = normalizeCreate({ prompt: "old schedule", every: "5m" }, binding);
  await schedules.mutate((state) => state.tasks.push(task));
  assert.equal((await migrateLegacy(store)).deferred, 2);
  await schedules.close(); await migrateLegacy(store);
  assert.equal(store.get(route.id, binding).prompt, "Keep this prompt");
  assert.equal((await token(store.get(route.id, binding))), secret);
  assert.equal(store.accept(route.id, secret, event("old-event")).receiptId, accepted.receiptId);
  assert.equal(store.get(task.id, binding).schedule.everyMs, 300_000);
  assert.equal((await migrateLegacy(store)).imported, 0);
  const backups = await fs.readdir(path.join(directory, "migration-v1-backup")); assert.equal(backups.length, 2);
  assert.equal((await fs.stat(path.join(directory, "migration-v1-backup", backups[0]))).mode & 0o777, 0o600);
});

test("reconcile uses the saved parent chain and never attributes a later interactive reply", async (t) => {
  const { store, binding } = await fixture(t); const { trigger } = store.create(binding, input());
  store.accept(trigger.id, await token(trigger), event("confirmed"));
  store.register(binding, "owner", process.pid); const run = store.claim(binding, "owner"); store.release(binding, "owner");
  const timestamp = Date.now();
  const entries = [
    { id: "u", type: "message", message: { role: "user", content: [{ type: "text", text: JSON.stringify({ receiptId: run.id }) }] } },
    { id: "call", parentId: "u", type: "message", message: { role: "assistant", stopReason: "toolUse", content: [] } },
    { id: "tool", parentId: "call", type: "message", message: { role: "toolResult", content: [] } },
    { id: "reply", parentId: "tool", type: "message", message: { role: "assistant", stopReason: "stop", timestamp, content: [{ type: "text", text: "Verified response" }] } },
  ];
  await fs.writeFile(binding.sessionFile, entries.map(JSON.stringify).join("\n") + "\n");
  const result = await reconcileRuns(store, binding, trigger.id, run.id);
  assert.equal(result.reconciled[0].state, "responded"); assert.equal(result.reconciled[0].response.messageId, "reply"); assert.equal(result.reconciled[0].settledAt, undefined);
  store.accept(trigger.id, await token(trigger), event("interrupted"));
  store.register(binding, "new-owner", process.pid); const interrupted = store.claim(binding, "new-owner"); store.release(binding, "new-owner");
  const later = [
    { id: "event", type: "message", message: { role: "user", content: JSON.stringify({ runId: interrupted.id }) } },
    { id: "interactive", parentId: "event", type: "message", message: { role: "user", content: "Different question" } },
    { id: "other-reply", parentId: "interactive", type: "message", message: { role: "assistant", stopReason: "stop", timestamp, content: "Unrelated answer" } },
  ];
  await fs.appendFile(binding.sessionFile, later.map(JSON.stringify).join("\n") + "\n");
  assert.equal((await reconcileRuns(store, binding, trigger.id, interrupted.id)).unresolved[0].reason, "no_saved_final_response");
});
