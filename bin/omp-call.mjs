#!/usr/bin/env node
import fs from "node:fs/promises";
import { callTarget, listRegistries, scheduleTarget } from "../lib/client.mjs";
import { runtimeDirectory } from "../lib/registry.mjs";

function usage() {
  return `Usage: omp-call [target] [prompt] [options]

One-line prompt: omp-call <session-id|name|session-file> "prompt"
Commands: omp-call list | omp-call <target> ping | omp-call <target> status
          omp-call <target> schedule list|show|pause|resume|delete|run-now|history [TASK_ID]
          omp-call <target> schedule add --at RFC3339 --prompt TEXT
          omp-call <target> schedule add --every 5m --prompt TEXT
          omp-call <target> schedule add --cron "*/5 * * * *" --prompt TEXT [--expires 7d] [--jitter 30s]
Options:  --session-id ID --session-file FILE --name NAME --socket PATH --timeout MS
          --prompt TEXT --at TIME --every DURATION --cron EXPR --expires DURATION --jitter DURATION
          --max-queue-wait DURATION --coalesce-key KEY --idempotency-key KEY --limit N
          --json --stdin --directory DIR
`;
}
export function parseArgs(argv) { return parse(argv); }
export function resolveInvocation(parsed) {
  const { options, positional } = parsed; const first = positional[0];
  let target = options.target; let method = "prompt"; let prompt; let scheduleArgs;
  if (first === "schedule" && (target || options.socket)) scheduleArgs = [target, "schedule", ...positional.slice(1)];
  else if (target || options.socket) {
    if (first === "ping" || first === "status") method = first;
    else if (first === "schedule") scheduleArgs = [target, "schedule", ...positional.slice(1)];
    else if (first) prompt = positional.join(" ");
  } else if (first === "ping" || first === "status") method = first;
  else if (first) { target = first; if (positional[1] === "ping" || positional[1] === "status") method = positional[1]; else if (positional[1] === "schedule") scheduleArgs = positional; else prompt = positional.slice(1).join(" "); }
  return { options, positional, target, method, prompt, scheduleArgs };
}
function fail(message, code = 2) { process.stderr.write(`omp-call: ${message}\n`); process.exitCode = code; }
function parse(argv) {
  const options = { timeoutMs: 300_000, json: false }; const positional = [];
  const values = new Set(["--session-id", "--session-file", "--name", "--socket", "--timeout", "--directory", "--prompt", "--at", "--every", "--cron", "--expires", "--jitter", "--max-queue-wait", "--coalesce-key", "--idempotency-key", "--limit"]);
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--help" || arg === "-h") return { help: true };
    if (arg === "--json") options.json = true; else if (arg === "--stdin") options.stdin = true;
    else if (values.has(arg)) {
      const value = argv[++i]; if (!value) throw new Error(`${arg} requires a value`);
      const key = arg.slice(2).replace(/-([a-z])/g, (_, c) => c.toUpperCase()); options[key] = value;
      // Preserve explicit selector type through resolveTarget; do not collapse
      // it into an ambiguous free-form target.
      if (["sessionId", "sessionFile", "name"].includes(key)) options.target = value;
      if (key === "timeout") { options.timeoutMs = Number(value); if (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs <= 0) throw new Error("--timeout must be a positive integer"); }
    } else if (arg.startsWith("-")) throw new Error(`unknown option: ${arg}`); else positional.push(arg);
  }
  return { options, positional };
}
function display(value, json) {
  if (json) return process.stdout.write(`${JSON.stringify(value)}\n`);
  if (value?.tasks) { if (!value.tasks.length) return process.stdout.write("No schedules.\n"); for (const task of value.tasks) process.stdout.write(`${task.id}\t${task.enabled && !task.paused ? "active" : "paused"}\t${task.nextRunAt || "-"}\t${task.kind}\n`); return; }
  if (value?.history) { if (!value.history.length) return process.stdout.write("No history.\n"); for (const run of value.history) process.stdout.write(`${run.runId}\t${run.state}\t${run.scheduledAt}\t${run.taskId}\n`); return; }
  process.stdout.write(`${JSON.stringify(value)}\n`);
}
async function schedule(target, positional, options) {
  const action = positional[2] || "list"; const id = positional[3]; let operation; let params = {};
  if (action === "add" || action === "create") { operation = "create"; params = { prompt: options.prompt, at: options.at, every: options.every, cron: options.cron, expires: options.expires, jitter: options.jitter, maxQueueWait: options.maxQueueWait, coalesceKey: options.coalesceKey, idempotencyKey: options.idempotencyKey }; if (options.stdin) params.prompt = await fs.readFile(0, "utf8"); }
  else if (action === "list") operation = "list";
  else if (action === "show" || action === "get") { operation = "get"; params = { taskId: id }; }
  else if (action === "pause" || action === "resume" || action === "delete") { operation = action; params = { taskId: id }; }
  else if (action === "run-now") { operation = "run_now"; params = { taskId: id, idempotencyKey: options.idempotencyKey }; }
  else if (action === "history") { operation = "history"; params = { taskId: id, limit: options.limit && Number(options.limit) }; }
  else throw new Error(`unknown schedule command: ${action}`);
  if (["get", "pause", "resume", "delete", "run_now"].includes(operation) && !params.taskId) throw new Error(`${action} requires a task ID`);
  const { result } = await scheduleTarget(target, operation, params, options); display(result, options.json);
}

async function main() {
  try {
    const parsed = parse(process.argv.slice(2));
    if (parsed.help) { process.stdout.write(usage()); return; }
    const invocation = resolveInvocation(parsed);
    const { options, positional } = invocation; const first = positional[0];
    if (first === "list") {
      if (positional.length !== 1) throw new Error("list takes no target or prompt");
      const records = await listRegistries(options.directory || runtimeDirectory());
      if (options.json) process.stdout.write(`${JSON.stringify(records)}\n`); else if (!records.length) process.stdout.write("No live OMP session bridges. Start OMP with omp-triggers loaded.\n"); else for (const record of records) process.stdout.write(`${record.sessionId}\t${record.name || "-"}\t${record.cwd}\n`);
    } else {
      let { target, method, prompt } = invocation;
      if (invocation.scheduleArgs) { await schedule(target, invocation.scheduleArgs, options); return; }
      if (!target && !options.socket) throw new Error("target required: use a session id/name/file, --socket, or omp-call list");
      if (method === "prompt") { if (options.stdin) prompt = await fs.readFile(0, "utf8"); prompt ||= options.prompt; if (!prompt?.trim()) throw new Error("prompt required (or pass --stdin)"); }
      const { result } = await callTarget(target, method, { ...options, prompt });
      if (options.json) process.stdout.write(`${JSON.stringify(result)}\n`); else if (method === "prompt") process.stdout.write(`${result.text}\n`); else process.stdout.write(`${method === "ping" ? "pong" : "status"}: ${JSON.stringify(result)}\n`);
    }
  } catch (error) { fail(error.message, error.code === "timeout" ? 124 : 1); }
}

if (process.argv[1] && (await fs.realpath(process.argv[1]).catch(() => process.argv[1])) === (await fs.realpath(new URL(import.meta.url)).catch(() => new URL(import.meta.url).pathname))) await main();
