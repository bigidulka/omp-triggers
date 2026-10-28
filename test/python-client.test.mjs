import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { SessionBridge } from "../lib/server.mjs";

function python(args, env) {
  return new Promise((resolve, reject) => {
    const child = spawn("python3", args, { env, stdio: ["ignore", "pipe", "pipe"] }); let stdout = ""; let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += chunk; }); child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.once("error", reject); child.once("exit", (code) => code === 0 ? resolve(stdout) : reject(new Error(`python exited ${code}: ${stderr}`)));
  });
}

test("Python schedule API has live socket parity", async (t) => {
  const root = path.resolve(import.meta.dirname, ".."); const runtime = await fs.mkdtemp(path.join(os.tmpdir(), "pi-session-python-runtime-")); const state = await fs.mkdtemp(path.join(os.tmpdir(), "pi-session-python-state-"));
  const bridge = new SessionBridge({ sessionId: "python-session", sessionFile: "/tmp/python.jsonl", cwd: "/tmp/python", directory: runtime, stateDirectory: state, isIdle: () => true, sendUserMessage: () => {} });
  await bridge.start();
  t.after(async () => { await bridge.close(); await fs.rm(runtime, { recursive: true, force: true }); await fs.rm(state, { recursive: true, force: true }); });
  const source = "import json; from omp_session_bridge import schedule_create, schedule_list; x=schedule_create(session_id='python-session', every='1m', prompt='python parity', max_queue_wait='1h', coalesce_key='python:parity', directory=None); print(json.dumps({'id':x['taskId'],'count':len(schedule_list(session_id='python-session')),'queue':x['task']['maxQueueWaitMs'],'key':x['task']['coalesceKey']}))";
  const output = await python(["-c", source], { ...process.env, PYTHONPATH: path.join(root, "clients"), OMP_SESSION_BRIDGE_DIR: runtime });
  const result = JSON.parse(output); assert.equal(result.count, 1); assert.ok(result.id); assert.equal(result.queue, 3_600_000); assert.equal(result.key, "python:parity");
  const cli = await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [path.join(root, "bin/omp-call.mjs"), "--session-id", "python-session", "schedule", "list", "--json"], { env: { ...process.env, OMP_SESSION_BRIDGE_DIR: runtime }, stdio: ["ignore", "pipe", "pipe"] }); let stdout = ""; let stderr = "";
    child.stdout.on("data", (chunk) => { stdout += chunk; }); child.stderr.on("data", (chunk) => { stderr += chunk; }); child.once("error", reject); child.once("exit", (code) => code === 0 ? resolve(stdout) : reject(new Error(stderr)));
  });
  assert.equal(JSON.parse(cli).tasks.length, 1, "--session-id ID schedule list keeps schedule as the command");
  const timeoutProbe = "import omp_session_bridge as p; seen={}; p._target=lambda *a, **k: {'socketPath':'x'}; p._request=lambda *a, **k: seen.update(k) or {'text':'ok'}; p.call(session_id='x', prompt='p', timeout=12); print(seen['timeout'])";
  assert.equal((await python(["-c", timeoutProbe], { ...process.env, PYTHONPATH: path.join(root, "clients") })).trim(), "12");
});
