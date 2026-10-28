import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { Scheduler } from "../lib/scheduler.mjs";
import { compactIdentity, compactRun, compactTask } from "../lib/tool-contract.mjs";

async function scheduler(t, directory, binding) {
  const value = new Scheduler({ binding, directory, dispatch: () => {} });
  await value.start();
  t.after(() => value.stop());
  return value;
}

test("identical cwd sessions remain isolated by canonical session file", async (t) => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "omp-session-isolation-"));
  t.after(() => fs.rm(directory, { recursive: true, force: true }));
  const cwd = path.join(directory, "project"); await fs.mkdir(cwd);
  const a = { sessionId: "session-a", sessionFile: path.join(directory, "a.jsonl"), cwd };
  const b = { sessionId: "session-b", sessionFile: path.join(directory, "b.jsonl"), cwd };
  const first = await scheduler(t, directory, a); const second = await scheduler(t, directory, b);
  const created = await first.create({ every: "1m", prompt: "only a" });
  assert.equal(first.list().length, 1);
  assert.equal(second.list().length, 0);
  assert.throws(() => second.get(created.taskId), { code: "schedule_not_found" });
});

test("compact projections omit binding, hashes, and prompt from list/history", () => {
  const task = compactTask({ id: "t", kind: "every", every: "60000ms", enabled: true, paused: false, nextRunAt: "2026-01-01T00:01:00Z", prompt: "secret", binding: { sessionFile: "/private" }, runCount: 0 });
  const run = compactRun({ runId: "r", taskId: "t", state: "settled", reason: "manual", settledAt: "2026-01-01T00:00:00Z", promptHash: "hash", resultHash: "hash2", sessionId: "secret" });
  assert.deepEqual(task, { id: "t", kind: "every", spec: "60000ms", state: "active", next: "2026-01-01T00:01:00Z" });
  assert.deepEqual(run, { id: "r", taskId: "t", state: "settled", reason: "manual", at: "2026-01-01T00:00:00Z" });
  assert.deepEqual(compactIdentity({ sessionId: "session-a", cwd: "/project", sessionFile: "/private" }), { sessionId: "session-a", cwd: "/project" });
});
