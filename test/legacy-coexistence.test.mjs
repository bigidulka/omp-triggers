import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";
import { SessionBridge } from "../lib/server.mjs";
import { EventStore, eventDirectory } from "../lib/event-store.mjs";
import { startTriggerDaemon } from "../lib/trigger-daemon.mjs";

async function until(check) {
  const end = Date.now() + 5000;
  while (Date.now() < end) { const result = await check(); if (result) return result; await new Promise((r) => setTimeout(r, 20)); }
  throw new Error("legacy coexistence did not converge");
}

test("new receiver serves a live legacy runtime then transfers receipts and tokens at handoff", { timeout: 15_000 }, async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "omp-legacy-coexist-"));
  const state = path.join(root, "state");
  const binding = { sessionId: crypto.randomUUID(), sessionFile: path.join(root, "saved.jsonl"), cwd: root };
  await fs.writeFile(binding.sessionFile, "");
  const sent = []; let bridge; let daemon;
  t.after(async () => { await bridge?.close(); await daemon?.close(); await fs.rm(root, { recursive: true, force: true }); });
  bridge = new SessionBridge({ ...binding, legacyBackend: true, stateDirectory: state, directory: path.join(root, "runtime"), isIdle: () => true,
    sendUserMessage(prompt) { sent.push(prompt); queueMicrotask(() => { bridge.onMessageEnd({ role: "assistant", stopReason: "stop", content: "LEGACY-OK" }); bridge.onAgentSettled(); }); } });
  await bridge.start();
  const schedule = await bridge.scheduler.create({ at: "2099-01-01T00:00:00Z", prompt: "legacy timer" });
  const legacy = new EventStore(eventDirectory(state));
  const route = await legacy.create(binding, { name: "live-v04", instruction: "legacy event" });
  const token = (await fs.readFile(route.tokenFile, "utf8")).trim();
  daemon = await startTriggerDaemon({ directory: state, port: 0 });
  assert.match((await fetch(`${daemon.baseUrl}/health`).then((r) => r.json())).version, /^\d+\.\d+\.\d+/);
  assert.equal(daemon.store.list(binding).length, 0, "live legacy ownership is deferred");
  await bridge.scheduler.runNow(schedule.taskId);
  await until(() => bridge.scheduler.history(schedule.taskId).find((r) => r.state === "settled"));
  const event = { eventId: "legacy:live:1", type: "legacy.signal", data: { message: "safe compatibility test" } };
  const post = (payload = event) => fetch(`${daemon.baseUrl}/v1/events/${route.id}`, { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: JSON.stringify(payload) });
  const response = await post(); assert.equal(response.status, 202, "new intake must keep existing legacy webhooks alive");
  const accepted = await response.json();
  await until(async () => (await legacy.receipts(route.id, binding))[0]?.state === "settled");
  assert.equal(sent.length, 2);
  await legacy.setEnabled(route.id, binding, false);
  assert.equal((await post({ ...event, eventId: "paused" })).status, 409);
  await legacy.setEnabled(route.id, binding, true);
  await bridge.close();
  const control = (await fs.readFile(path.join(state, "control.token"), "utf8")).trim();
  const inflight = Array.from({ length: 20 }, (_, n) => post({ ...event, eventId: `handoff:${n}` }));
  const registered = await fetch(`${daemon.baseUrl}/v1/control`, { method: "POST", headers: { authorization: `Bearer ${control}`, "content-type": "application/json" }, body: JSON.stringify({ action: "register", binding, owner: "new-owner", pid: process.pid }) });
  for (const response of await Promise.all(inflight)) assert.equal(response.status, 202);
  assert.equal(registered.status, 200);
  assert.equal(daemon.store.get(route.id, binding).prompt, "legacy event");
  const duplicate = await post(); assert.equal(duplicate.status, 202);
  const receipt = await duplicate.json();
  assert.equal(receipt.duplicate, true); assert.equal(receipt.receiptId, accepted.receiptId); assert.equal(receipt.state, "settled");
  assert.equal((await fs.readFile(route.tokenFile, "utf8")).trim(), token);
  assert.equal(daemon.store.history(binding, route.id, 200).length, 21, "all handoff events were transferred without loss");
  daemon.store.setState(binding, route.id, "delete");
  assert.equal((await post()).status, 401, "revoked SQLite credential cannot fall back to the old JSON credential");
});
