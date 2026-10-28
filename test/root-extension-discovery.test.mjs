import assert from "node:assert/strict";
import { once } from "node:events";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import test from "node:test";
import { request } from "../lib/client.mjs";
import { listRegistries } from "../lib/registry.mjs";

const projectRoot = path.resolve(import.meta.dirname, "..");
const ompBin = process.env.OMP_BIN || "omp";

function waitForResponse(child, id) {
  return new Promise((resolve, reject) => {
    let output = "";
    const timer = setTimeout(() => reject(new Error("OMP RPC response timed out")), 10_000);
    child.stdout.on("data", (chunk) => {
      output += chunk;
      for (;;) {
        const newline = output.indexOf("\n");
        if (newline < 0) break;
        const line = output.slice(0, newline); output = output.slice(newline + 1);
        try {
          const message = JSON.parse(line);
          if (message.id === id) { clearTimeout(timer); resolve(message); return; }
        } catch { /* OMP diagnostics are not RPC frames. */ }
      }
    });
    child.once("error", (error) => { clearTimeout(timer); reject(error); });
  });
}

async function waitForRegistry(directory) {
  for (let attempt = 0; attempt < 300; attempt++) {
    const [registry] = await listRegistries(directory);
    if (registry?.socketPath) return registry;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error("session_start did not publish a bridge registry");
}

function startRpc(args, env) {
  return spawn(ompBin, ["--mode", "rpc", "--no-extensions", "-e", path.join(projectRoot, "index.ts"), ...args], { env, stdio: ["pipe", "pipe", "pipe"] });
}

test("OMP loads the extension without creating an unsaved bridge", async (t) => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "omp-session-bridge-root-extension-"));
  const bridgeDir = path.join(temporary, "bridge");
  const child = startRpc(["--no-session"], { ...process.env, OMP_SESSION_BRIDGE_DIR: bridgeDir });
  t.after(async () => { child.kill("SIGTERM"); await Promise.race([once(child, "exit"), new Promise((resolve) => setTimeout(resolve, 2_000))]); try { const descriptor = JSON.parse(await fs.readFile(path.join(temporary, "state", "events", "receiver.json"), "utf8")); process.kill(descriptor.pid, "SIGTERM"); await new Promise((r) => setTimeout(r, 150)); } catch {} await fs.rm(temporary, { recursive: true, force: true }); });
  const state = waitForResponse(child, "state"); child.stdin.write('{"id":"state","type":"get_state"}\n');
  assert.equal((await state).success, true);
  assert.deepEqual(await listRegistries(bridgeDir), []);
});

test("saved OMP session restart retains schedule state and replaces its socket", async (t) => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "omp-session-bridge-saved-lifecycle-"));
  const bridgeDir = path.join(temporary, "bridge"); const stateDir = path.join(temporary, "state"); const sessionDir = path.join(temporary, "sessions"); const sessionFile = path.join(sessionDir, "saved.jsonl");
  await fs.mkdir(sessionDir, { recursive: true }); await fs.writeFile(sessionFile, JSON.stringify({ type: "session", version: 3, id: "9c2e3d54-9d4a-4dd0-80fd-841615941337", timestamp: new Date().toISOString(), cwd: temporary }) + "\n");
  const environment = { ...process.env, OMP_SESSION_BRIDGE_EVENT_PORT: "0", OMP_SESSION_BRIDGE_DIR: bridgeDir, OMP_SESSION_BRIDGE_STATE_DIR: stateDir };
  const start = () => startRpc(["--session", sessionFile, "--session-dir", sessionDir], environment);
  let child = start();
  t.after(async () => { child.kill("SIGTERM"); await Promise.race([once(child, "exit"), new Promise((resolve) => setTimeout(resolve, 2_000))]); try { const descriptor = JSON.parse(await fs.readFile(path.join(temporary, "state", "events", "receiver.json"), "utf8")); process.kill(descriptor.pid, "SIGTERM"); await new Promise((r) => setTimeout(r, 150)); } catch {} await fs.rm(temporary, { recursive: true, force: true }); });
  const firstRegistry = await waitForRegistry(bridgeDir);
  const created = await request(firstRegistry.socketPath, { id: "create", method: "schedule.create", params: { every: "1m", prompt: "persist me" } });
  const taskId = created.response.result.taskId; const firstPid = firstRegistry.pid;
  child.kill("SIGTERM"); await once(child, "exit");
  child = start();
  let secondRegistry;
  for (let attempt = 0; attempt < 300; attempt++) {
    [secondRegistry] = await listRegistries(bridgeDir);
    if (secondRegistry?.socketPath && secondRegistry.pid !== firstPid) break;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  assert.ok(secondRegistry?.socketPath);
  assert.equal(secondRegistry.sessionFile, sessionFile);
  assert.notEqual(secondRegistry.pid, firstPid);
  const schedules = await request(secondRegistry.socketPath, { id: "list", method: "schedule.list" });
  assert.equal(schedules.response.result.tasks[0].id, taskId);
});
