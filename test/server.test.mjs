import assert from "node:assert/strict";
import fs from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { SessionBridge } from "../lib/server.mjs";
import { request } from "../lib/client.mjs";

async function makeBridge(options = {}) {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "omp-session-bridge-test-"));
  const stateDirectory = path.join(directory, "state");
  let idle = options.idle ?? true;
  const sent = [];
  const bridge = new SessionBridge({ sessionId: options.sessionId || "test-session", sessionFile: "/tmp/test.jsonl", cwd: "/tmp", directory, stateDirectory, timeoutMs: options.timeoutMs || 100, maxQueueLength: 2, isIdle: () => idle, sendUserMessage: (p) => sent.push(p) });
  await bridge.start();
  return { bridge, directory, stateDirectory, sent, setIdle: (value) => { idle = value; } };
}

test("scheduled work waits idle without consuming execution timeout", async (t) => {
  const { bridge, directory, sent, setIdle } = await makeBridge({ idle: false, timeoutMs: 30 });
  t.after(async () => { await bridge.close(); await fs.rm(directory, { recursive: true, force: true }); });
  let settled; let failed;
  bridge.enqueueScheduled("supervisor", { onSettled: (value) => { settled = value; }, onFailed: (error) => { failed = error; } }, { maxQueueWaitMs: 500 });
  await new Promise((resolve) => setTimeout(resolve, 80));
  assert.equal(failed, undefined);
  assert.equal(bridge.status().deliveryState, "queued_waiting_for_idle");
  setIdle(true); bridge.drain();
  assert.deepEqual(sent, ["supervisor"]);
  bridge.onMessageEnd({ role: "assistant", content: [{ type: "text", text: "cycle done" }] });
  bridge.onAgentSettled();
  assert.equal(settled, "cycle done");
});

test("scheduled queue wait has a distinct bounded deadline", async (t) => {
  const { bridge, directory } = await makeBridge({ idle: false, timeoutMs: 500 });
  t.after(async () => { await bridge.close(); await fs.rm(directory, { recursive: true, force: true }); });
  const failure = new Promise((resolve) => bridge.enqueueScheduled("wait", { onFailed: resolve }, { maxQueueWaitMs: 25 }));
  assert.equal((await failure).code, "queue_timeout");
  assert.equal(bridge.status().deliveryState, "idle");
});

test("idle pump dispatches work queued during an unrelated active turn", async (t) => {
  const { bridge, directory, sent, setIdle } = await makeBridge({ idle: false, timeoutMs: 500 });
  t.after(async () => { await bridge.close(); await fs.rm(directory, { recursive: true, force: true }); });
  bridge.enqueueScheduled("wake", {}, { maxQueueWaitMs: 5_000 });
  setIdle(true);
  for (let attempt = 0; attempt < 30 && sent.length === 0; attempt++) await new Promise((resolve) => setTimeout(resolve, 50));
  assert.deepEqual(sent, ["wake"]);
  bridge.onAgentSettled();
});

test("existing saved-session paths are canonicalized for durable binding", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "omp-session-bridge-canonical-"));
  const runtime = path.join(root, "runtime"); const state = path.join(root, "state"); const realCwd = path.join(root, "real-cwd"); const realSession = path.join(realCwd, "saved.jsonl");
  await fs.mkdir(realCwd); await fs.writeFile(realSession, ""); await fs.symlink(realCwd, path.join(root, "cwd-link"), "dir"); await fs.symlink(realSession, path.join(root, "session-link.jsonl"));
  const bridge = new SessionBridge({ sessionId: "canonical", sessionFile: path.join(root, "session-link.jsonl"), cwd: path.join(root, "cwd-link"), directory: runtime, stateDirectory: state, isIdle: () => true, sendUserMessage: () => {} });
  await bridge.start(); t.after(async () => { await bridge.close(); await fs.rm(root, { recursive: true, force: true }); });
  assert.equal(bridge.sessionFile, realSession); assert.equal(bridge.cwd, realCwd);
});

test("lifecycle serves ping/status and removes artifacts", async (t) => {
  const { bridge, directory } = await makeBridge();
  t.after(() => fs.rm(directory, { recursive: true, force: true }));
  const ping = await request(bridge.paths.socket, { id: "1", method: "ping" });
  assert.equal(ping.response.result.pong, true);
  const status = await request(bridge.paths.socket, { id: "2", method: "status" });
  assert.equal(status.response.result.idle, true);
  await bridge.close();
  await assert.rejects(fs.lstat(bridge.paths.socket), { code: "ENOENT" });
  await assert.rejects(fs.lstat(bridge.paths.registry), { code: "ENOENT" });
});

test("external prompts are FIFO and only resolve at settled", async (t) => {
  const { bridge, directory, sent } = await makeBridge({ idle: false, timeoutMs: 500 });
  t.after(async () => { await bridge.close(); await fs.rm(directory, { recursive: true, force: true }); });
  const first = request(bridge.paths.socket, { id: "1", method: "prompt", prompt: "one" });
  const second = request(bridge.paths.socket, { id: "2", method: "prompt", prompt: "two" });
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.deepEqual(sent, []);
  bridge.isIdle = () => true; bridge.drain();
  assert.deepEqual(sent, ["one"]);
  bridge.onMessageEnd({ role: "assistant", content: [{ type: "text", text: "answer one" }] });
  bridge.onAgentSettled();
  assert.equal((await first).response.result.text, "answer one");
  assert.deepEqual(sent, ["one", "two"]);
  bridge.onMessageEnd({ role: "assistant", content: [{ type: "text", text: "answer two" }] });
  bridge.onAgentSettled();
  assert.equal((await second).response.result.text, "answer two");
});

test("interactive activity fails active request and quarantines output until settlement", async (t) => {
  const { bridge, directory, sent } = await makeBridge({ timeoutMs: 500 });
  t.after(async () => { await bridge.close(); await fs.rm(directory, { recursive: true, force: true }); });
  const first = request(bridge.paths.socket, { id: "first", method: "prompt", prompt: "bridge" }, 300).then(() => null, (error) => error);
  await new Promise((resolve) => setTimeout(resolve, 5));
  bridge.onExternalActivity("user");
  const error = await first;
  assert.equal(error.code, "session_busy");
  bridge.onMessageEnd({ role: "assistant", content: [{ type: "text", text: "private interactive output" }] });
  const second = request(bridge.paths.socket, { id: "second", method: "prompt", prompt: "next" }, 300);
  await new Promise((resolve) => setTimeout(resolve, 5));
  assert.deepEqual(sent, ["bridge"], "quarantine prevents a next dispatch before settlement");
  bridge.onAgentSettled();
  await new Promise((resolve) => setTimeout(resolve, 5));
  assert.deepEqual(sent, ["bridge", "next"]);
  bridge.onAgentSettled();
  assert.equal((await second).response.result.text, "");
});

test("delivered timeout quarantines the dispatch lane until eventual settle", async (t) => {
  const { bridge, directory, sent } = await makeBridge({ timeoutMs: 30 });
  t.after(async () => { await bridge.close(); await fs.rm(directory, { recursive: true, force: true }); });
  const first = request(bridge.paths.socket, { id: "first", method: "prompt", prompt: "one" }, 300).then(() => null, (error) => error);
  await new Promise((resolve) => setTimeout(resolve, 10));
  const firstError = await first;
  assert.match(firstError.message, /request exceeded/);
  assert.deepEqual(sent, ["one"]);
  await new Promise((resolve) => setTimeout(resolve, 40));
  assert.deepEqual(sent, ["one"], "timed-out active prompt must keep lane quarantined");
  bridge.onAgentSettled();
  const second = request(bridge.paths.socket, { id: "second", method: "prompt", prompt: "two" }, 300);
  await new Promise((resolve) => setTimeout(resolve, 5));
  assert.deepEqual(sent, ["one", "two"]);
  bridge.onAgentSettled();
  assert.equal((await second).response.result.text, "");
});

test("queue timeout and malformed request get clean errors", async (t) => {
  const { bridge, directory } = await makeBridge({ idle: false, timeoutMs: 500 });
  bridge.maxQueueWaitMs = 30;
  t.after(async () => { await bridge.close(); await fs.rm(directory, { recursive: true, force: true }); });
  await assert.rejects(request(bridge.paths.socket, { id: "x", method: "prompt", prompt: "wait" }, 300), /waited more than/);
  const raw = await new Promise((resolve, reject) => {
    const socket = net.createConnection(bridge.paths.socket); let data = "";
    socket.on("connect", () => socket.end("{bad}\n")); socket.on("data", (chunk) => { data += chunk; }); socket.on("end", () => resolve(JSON.parse(data))); socket.on("error", reject);
  });
  assert.equal(raw.error.code, "invalid_json");
});
