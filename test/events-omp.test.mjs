import assert from "node:assert/strict";
import test from "node:test";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import http from "node:http";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import { EventStore, eventDirectory } from "../lib/event-store.mjs";
import { receiverStatus } from "../lib/event-service.mjs";

const root = path.resolve(import.meta.dirname, "..");
async function until(check, timeout = 20_000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) { const result = await check(); if (result) return result; await new Promise((r) => setTimeout(r, 25)); }
  throw new Error("OMP event integration did not converge");
}
async function stop(child) {
  if (!child || child.exitCode !== null || child.signalCode) return;
  await new Promise((resolve) => { const timer = setTimeout(() => { child.kill("SIGKILL"); resolve(); }, 3000); child.once("exit", () => { clearTimeout(timer); resolve(); }); child.kill("SIGTERM"); });
}

test("real OMP agent creates its own webhook, consumes events, resumes offline inbox and deduplicates", { timeout: 60_000 }, async (t) => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "omp-webhook-live-"));
  const agent = path.join(temporary, "agent"); await fs.mkdir(agent);
  const state = path.join(temporary, "state"); const directory = eventDirectory(state); const store = new EventStore(directory);
  const sessionFile = path.join(temporary, "saved.jsonl"); const sessionId = crypto.randomUUID();
  await fs.writeFile(sessionFile, JSON.stringify({ type: "session", version: 3, id: sessionId, timestamp: new Date().toISOString(), cwd: temporary }) + "\n");
  let modelCalls = 0; let eventCalls = 0; let child; let stderr = ""; let stdout = "";
  const mock = http.createServer(async (req, res) => {
    let body = ""; for await (const chunk of req) body += chunk;
    const input = JSON.parse(body); modelCalls++;
    const last = input.messages.at(-1);
    const lastText = typeof last?.content === "string" ? last.content : JSON.stringify(last?.content);
    const isInit = last?.role === "user" && lastText.includes("INITIALIZE_TEST_WEBHOOK");
    if (last?.role === "user" && !isInit) eventCalls++;
    const delta = isInit ? { role: "assistant", tool_calls: [{ index: 0, id: "call_subscribe", type: "function", function: { name: "write", arguments: JSON.stringify({ path: "xd://session_event_write", i: "Create webhook on this chat", content: JSON.stringify({ action: "create", name: "live-event-test", instruction: "Analyze the external signal and reply OMP-WEBHOOK-E2E-OK." }) }) } }] } : { role: "assistant", content: last?.role === "tool" ? "SUBSCRIPTION-READY" : "OMP-WEBHOOK-E2E-OK" };
    res.writeHead(200, { "content-type": "text/event-stream" });
    const base = { id: "chatcmpl-local-test", object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: "probe" };
    res.write(`data: ${JSON.stringify({ ...base, choices: [{ index: 0, delta, finish_reason: null }] })}\n\n`);
    res.write(`data: ${JSON.stringify({ ...base, choices: [{ index: 0, delta: {}, finish_reason: isInit ? "tool_calls" : "stop" }], usage: { prompt_tokens: 100, completion_tokens: 10, total_tokens: 110 } })}\n\ndata: [DONE]\n\n`); res.end();
  });
  await new Promise((resolve) => mock.listen(0, "127.0.0.1", resolve));
  await fs.writeFile(path.join(agent, "models.yml"), JSON.stringify({ providers: { "webhook-test": { baseUrl: `http://127.0.0.1:${mock.address().port}/v1`, apiKey: "local-test-only", api: "openai-completions", models: [{ id: "probe", name: "Local webhook probe", reasoning: false, input: ["text"], contextWindow: 131072, maxTokens: 4096 }] } } }));
  const env = { ...process.env, PI_CODING_AGENT_DIR: agent, OMP_SESSION_BRIDGE_STATE_DIR: state, OMP_SESSION_BRIDGE_DIR: path.join(temporary, "runtime"), OMP_SESSION_BRIDGE_EVENT_PORT: "0" };
  const start = () => {
    child = spawn(process.env.OMP_BIN || "omp", ["--mode", "rpc", "--no-extensions", "-e", path.join(root, "index.ts"), "--no-skills", "--no-rules", "--no-title", "--no-lsp", "--no-pty", "--approval-mode", "yolo", "--model", "webhook-test/probe", "--thinking", "off", "--cwd", temporary, "--session", sessionFile, "--session-dir", temporary], { env, stdio: ["pipe", "pipe", "pipe"] });
    child.stdout.on("data", (chunk) => { stdout += chunk; }); child.stderr.on("data", (chunk) => { stderr += chunk; }); return child;
  };
  t.after(async () => {
    await stop(child); const live = await receiverStatus(directory); if (live.available) process.kill(live.pid, "SIGTERM");
    await new Promise((r) => mock.close(r));
    if (process.env.OMP_EVENT_E2E_EVIDENCE_DIR) {
      await fs.mkdir(process.env.OMP_EVENT_E2E_EVIDENCE_DIR, { recursive: true });
      await fs.writeFile(path.join(process.env.OMP_EVENT_E2E_EVIDENCE_DIR, "omp-event-e2e.json"), JSON.stringify({ model: "local deterministic HTTP stub; no real provider used", modelCalls, eventCalls, stderr, hasWebhookToolCall: (await fs.readFile(sessionFile, "utf8")).includes("xd://session_event_write"), receipts: await store.list({ sessionFile, cwd: temporary }).then((routes) => routes.flatMap((route) => route.events.map(({ eventId, state, acceptedAt, deliveredAt, settledAt }) => ({ eventId, state, acceptedAt, deliveredAt, settledAt })))) }, null, 2));
    }
    await fs.rm(temporary, { recursive: true, force: true });
  });
  const binding = { sessionFile, cwd: temporary };
  start(); child.stdin.write(JSON.stringify({ id: "init", type: "prompt", message: "INITIALIZE_TEST_WEBHOOK" }) + "\n");
  let subscription;
  try { subscription = await until(async () => (await store.list(binding))[0]); }
  catch (error) { throw new Error(`${error.message}; OMP stderr: ${stderr}; last output: ${stdout.slice(-1600)}`); }
  assert.equal(subscription.binding.sessionFile, sessionFile);
  await until(async () => (await fs.readFile(sessionFile, "utf8")).includes("SUBSCRIPTION-READY"));
  const live = await until(async () => { const value = await receiverStatus(directory); return value.available && value; });
  const token = (await fs.readFile(path.join(directory, `${subscription.id}.token`), "utf8")).trim();
  const post = async (eventId) => {
    const response = await fetch(`${live.baseUrl}/v1/events/${subscription.id}`, { method: "POST", headers: { authorization: `Bearer ${token}`, "content-type": "application/json" }, body: JSON.stringify({ eventId, type: "telegram.alert", data: { text: "integration signal" } }) });
    assert.equal(response.status, 202); return response.json();
  };
  await post("live:1");
  await until(async () => (await store.receipts(subscription.id, binding)).find((r) => r.eventId === "live:1")?.state === "settled");
  assert.ok((await fs.readFile(sessionFile, "utf8")).includes("OMP-WEBHOOK-E2E-OK"));
  await stop(child);
  const offline = await post("offline:2"); assert.equal(offline.state, "queued");
  start();
  await until(async () => (await store.receipts(subscription.id, binding)).find((r) => r.eventId === "offline:2")?.state === "settled");
  const callsBeforeDuplicate = modelCalls;
  assert.equal((await post("live:1")).duplicate, true);
  await new Promise((r) => setTimeout(r, 400));
  assert.equal(modelCalls, callsBeforeDuplicate);
  assert.equal(eventCalls, 2);
});
