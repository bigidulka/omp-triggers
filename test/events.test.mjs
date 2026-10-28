import assert from "node:assert/strict";
import test from "node:test";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { spawn } from "node:child_process";
import { EventStore, eventDirectory } from "../lib/event-store.mjs";
import { startEventReceiver } from "../lib/event-receiver.mjs";
import { SessionBridge } from "../lib/server.mjs";

async function fixture(t) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "omp-events-test-"));
  const binding = { sessionId: "event-test", sessionFile: path.join(root, "saved.jsonl"), cwd: root };
  await fs.writeFile(binding.sessionFile, ""); const stateDirectory = path.join(root, "state");
  const store = new EventStore(eventDirectory(stateDirectory)); const receiver = await startEventReceiver({ directory: store.directory, port: 0 });
  const subscription = await store.create(binding, { name: "telegram-alert", instruction: "Summarize this Telegram event." });
  const token = (await fs.readFile(subscription.tokenFile, "utf8")).trim();
  let bridge; const sent = []; let idle = true;
  const start = async (otherBinding = binding, timeoutMs = 5000) => {
    bridge = new SessionBridge({ ...otherBinding, directory: path.join(root, "runtime"), stateDirectory, timeoutMs, isIdle: () => idle, sendUserMessage: (prompt) => sent.push(prompt) }); await bridge.start(); return bridge;
  };
  const send = async (body, credential = token) => {
    const response = await fetch(`${receiver.baseUrl}/v1/events/${subscription.id}`, { method: "POST", headers: { "content-type": "application/json", authorization: `Bearer ${credential}` }, body: JSON.stringify(body) });
    return { status: response.status, body: await response.json() };
  };
  t.after(async () => { await bridge?.close(); await receiver.close(); await fs.rm(root, { recursive: true, force: true }); });
  return { root, store, binding, sent, start, send, subscription, token, setIdle: (value) => { idle = value; }, receiver };
}
async function until(check) {
  for (let i = 0; i < 200; i++) { const result = await check(); if (result) return result; await new Promise((r) => setTimeout(r, 10)); }
  assert.fail("event state did not converge");
}
const event = { eventId: "telegram:channel:42", type: "telegram.alert", data: { text: "signal" } };

test("HTTP accepts offline, deduplicates concurrent retries, binds to saved chat and settles", async (t) => {
  const f = await fixture(t);
  const replies = await Promise.all(Array.from({ length: 12 }, () => f.send(event)));
  assert.ok(replies.every((r) => r.status === 202));
  assert.equal(new Set(replies.map((r) => r.body.receiptId)).size, 1);
  assert.equal(replies.filter((r) => !r.body.duplicate).length, 1);
  assert.equal((await f.store.receipts(f.subscription.id, f.binding))[0].state, "queued");
  const other = { ...f.binding, sessionId: "other", sessionFile: path.join(f.root, "other.jsonl") }; await fs.writeFile(other.sessionFile, "");
  await assert.rejects(f.store.receipts(f.subscription.id, other), { code: "subscription_not_found" });
  const otherBridge = await f.start(other); await otherBridge.eventInbox.tick(); assert.equal(f.sent.length, 0); await otherBridge.close();
  f.setIdle(false); const bridge = await f.start(); await bridge.eventInbox.tick(); assert.equal(f.sent.length, 0);
  f.setIdle(true); await bridge.eventInbox.tick(); assert.equal(f.sent.length, 1);
  assert.match(f.sent[0], /Summarize this Telegram event/); assert.match(f.sent[0], /untrusted source data/);
  assert.match(f.sent[0], /telegram:channel:42/);
  await until(async () => (await f.store.receipts(f.subscription.id, f.binding))[0].state === "delivered");
  bridge.onAgentSettled();
  await until(async () => (await f.store.receipts(f.subscription.id, f.binding))[0].state === "settled");
  assert.equal((await f.send(event)).body.state, "settled");
  assert.equal((await f.send({ ...event, data: { text: "changed" } })).status, 409);
});

test("invalid authentication, payload and cross-session targeting never enter inbox", async (t) => {
  const f = await fixture(t);
  assert.equal((await f.send(event, "wrong")).status, 401);
  assert.equal((await f.send({ ...event, sessionFile: "/other/session" })).status, 400);
  assert.equal((await f.send({ ...event, data: { value: "x".repeat(17_000) } })).status, 413);
  assert.equal((await f.store.receipts(f.subscription.id, f.binding)).length, 0);
  await f.store.setEnabled(f.subscription.id, f.binding, false);
  assert.equal((await f.send(event)).status, 409);
  await f.store.setEnabled(f.subscription.id, f.binding, true);
  assert.equal((await f.send(event)).status, 202);
  await f.store.setEnabled(f.subscription.id, f.binding, false, true);
  assert.equal((await f.store.receipts(f.subscription.id, f.binding))[0].state, "cancelled");
  assert.equal((await f.send(event)).status, 401);
});

test("uncertain timeout is retained across restart and is never replayed", async (t) => {
  const f = await fixture(t); const bridge = await f.start(f.binding, 30);
  await f.send(event); await bridge.eventInbox.tick(); assert.equal(f.sent.length, 1);
  await until(async () => (await f.store.receipts(f.subscription.id, f.binding))[0].state === "uncertain");
  await bridge.close(); const restarted = await f.start(); await restarted.eventInbox.tick();
  assert.equal(f.sent.length, 1);
  const duplicate = await f.send(event); assert.equal(duplicate.body.state, "uncertain"); assert.equal(duplicate.body.duplicate, true);
});

test("crash-window claim is reconciled as uncertain and distinct source events survive", async (t) => {
  const f = await fixture(t); const accepted = await f.send(event);
  await f.store.transition(f.subscription.id, accepted.body.receiptId, "dispatching", { owner: "old-runtime" });
  await f.send({ ...event, eventId: "telegram:channel:43" });
  const bridge = await f.start({ ...f.binding, sessionId: "resumed-runtime-id" });
  await bridge.eventInbox.tick(); assert.equal(f.sent.length, 1); assert.match(f.sent[0], /telegram:channel:43/);
  const receipts = await f.store.receipts(f.subscription.id, f.binding);
  assert.equal(receipts.find((r) => r.eventId === event.eventId).state, "uncertain");
  bridge.onAgentSettled();
});

test("subscription creation is idempotent and credentials stay in a private file", async (t) => {
  const f = await fixture(t);
  const existing = await f.store.create(f.binding, { name: "telegram-alert", instruction: "Summarize this Telegram event." });
  assert.equal(existing.id, f.subscription.id); assert.equal(existing.existing, true);
  assert.equal((await fs.stat(existing.tokenFile)).mode & 0o777, 0o600);
  assert.equal(JSON.stringify(existing).includes(f.token), false);
  await assert.rejects(f.store.create(f.binding, { name: "telegram-alert", instruction: "different" }), { code: "subscription_conflict" });
});

test("pending deduplication survives its age window and special JSON property names", async (t) => {
  const f = await fixture(t);
  const special = { ...event, eventId: "__proto__" };
  assert.equal((await f.send(special)).body.duplicate, false);
  await f.store.mutate(f.subscription.id, f.binding, (route) => {
    route.seen.__proto__.acceptedAt = new Date(Date.now() - 8 * 86_400_000).toISOString();
  });
  assert.equal((await f.send(special)).body.duplicate, true);
  assert.equal((await f.store.receipts(f.subscription.id, f.binding)).length, 1);
});

function runChild(command, args, environment = process.env) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { env: environment, stdio: ["ignore", "pipe", "pipe"] }); let output = ""; let error = "";
    child.stdout.on("data", (chunk) => { output += chunk; }); child.stderr.on("data", (chunk) => { error += chunk; });
    child.once("error", reject); child.once("exit", (code) => code === 0 ? resolve(output) : reject(new Error(`child failed (${code}): ${error}`)));
  });
}

test("independent receiver writers recover a stale lock without losing deduplication", async (t) => {
  const f = await fixture(t);
  await fs.writeFile(path.join(f.store.directory, `${f.subscription.id}.lock`), JSON.stringify({ pid: 2147483647, token: "dead-writer" }), { mode: 0o600 });
  const code = `import fs from 'node:fs/promises'; import {EventStore} from ${JSON.stringify(path.resolve(import.meta.dirname, "../lib/event-store.mjs"))}; const store=new EventStore(${JSON.stringify(f.store.directory)}); const token=(await fs.readFile(${JSON.stringify(f.subscription.tokenFile)},'utf8')).trim(); console.log(JSON.stringify(await store.accept(${JSON.stringify(f.subscription.id)},token,${JSON.stringify(event)})));`;
  const results = await Promise.all(Array.from({ length: 4 }, () => runChild(process.execPath, ["--input-type=module", "-e", code]).then(JSON.parse)));
  assert.equal(results.filter((r) => !r.duplicate).length, 1);
  assert.equal(new Set(results.map((r) => r.receiptId)).size, 1);
  assert.equal((await f.store.receipts(f.subscription.id, f.binding)).length, 1);
});

test("Python source-listener helper delivers and reuses the durable receipt", async (t) => {
  const f = await fixture(t);
  const code = `import json;from omp_events import emit;print(json.dumps(emit(${JSON.stringify(`${f.receiver.baseUrl}/v1/events/${f.subscription.id}`)},${JSON.stringify(f.subscription.tokenFile)},event_id='python:42',event_type='wallet.deposit',data={'confirmations':12})))`;
  const environment = { ...process.env, PYTHONPATH: path.resolve(import.meta.dirname, "../clients"), PYTHONDONTWRITEBYTECODE: "1" };
  const first = JSON.parse(await runChild("python3", ["-c", code], environment));
  const duplicate = JSON.parse(await runChild("python3", ["-c", code], environment));
  assert.equal(first.state, "queued"); assert.equal(first.receiptId, duplicate.receiptId); assert.equal(duplicate.duplicate, true);
});
