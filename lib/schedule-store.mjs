import crypto from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { ensureSecureDirectory, processAlive } from "./registry.mjs";

const uid = typeof process.getuid === "function" ? process.getuid() : undefined;
function assertOwnedPrivate(stat, kind) {
  if (stat.isSymbolicLink() || !stat.isFile() || (stat.mode & 0o077) || (uid !== undefined && stat.uid !== uid)) throw scheduleError("unsafe_schedule_state", `schedule ${kind} is unsafe`);
}
import { MAX_HISTORY_PER_SESSION, MAX_HISTORY_PER_TASK, SCHEDULE_SCHEMA_VERSION, scheduleError } from "./schedule-model.mjs";

export function stateDirectory(env = process.env) {
  if (env.OMP_SESSION_BRIDGE_STATE_DIR) return path.resolve(env.OMP_SESSION_BRIDGE_STATE_DIR);
  if (env.XDG_STATE_HOME) return path.join(path.resolve(env.XDG_STATE_HOME), "omp-session-bridge");
  return path.join(os.homedir(), ".local", "state", "omp-session-bridge");
}
export function bindingKey(binding) {
  // A saved-session pathname and cwd are stable across process restart. OMP may
  // issue a fresh runtime sessionId when that same file is resumed, so it is
  // verified in state on first creation but not used as the durable filename.
  return crypto.createHash("sha256").update(JSON.stringify([binding.sessionFile, binding.cwd])).digest("hex");
}

function empty(binding) { return { schemaVersion: SCHEDULE_SCHEMA_VERSION, binding, tasks: [], history: [], idempotency: {}, ownerEpoch: 0, updatedAt: new Date().toISOString() }; }
function validState(value, binding) {
  if (!value || value.schemaVersion !== SCHEDULE_SCHEMA_VERSION || !Array.isArray(value.tasks) || !Array.isArray(value.history) || !value.binding || value.binding.sessionFile !== binding.sessionFile || value.binding.cwd !== binding.cwd) throw scheduleError("invalid_schedule_state", "schedule state is invalid or belongs to another session");
  // Resume may assign a new session id to the same saved file. Keep durable
  // task ownership anchored to its canonical file/cwd pair and refresh the
  // informational runtime ID for new run records.
  value.binding.sessionId = binding.sessionId;
  return value;
}

/** Per-bound-session secure JSON store. Calls are serialized within a live extension process. */
export class ScheduleStore {
  constructor(binding, directory = stateDirectory()) {
    this.binding = binding; this.directory = directory; this.file = path.join(directory, `${bindingKey(binding)}.json`); this.lockFile = `${this.file}.lock`; this.ownerToken = crypto.randomUUID(); this.state = null; this.tail = Promise.resolve();
  }
  async acquireLock() {
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const handle = await fs.open(this.lockFile, "wx", 0o600);
        await handle.writeFile(JSON.stringify({ pid: process.pid, token: this.ownerToken, acquiredAt: new Date().toISOString() })); await handle.close(); await fs.chmod(this.lockFile, 0o600); return;
      } catch (error) {
        if (error?.code !== "EEXIST") throw error;
        const stat = await fs.lstat(this.lockFile);
        assertOwnedPrivate(stat, "lock");
        let owner; try { owner = JSON.parse(await fs.readFile(this.lockFile, "utf8")); } catch { throw scheduleError("schedule_locked", "schedule state has an unreadable lock"); }
        if (processAlive(owner?.pid)) throw scheduleError("schedule_locked", "another live extension owns this session scheduler");
        await fs.unlink(this.lockFile);
      }
    }
    throw scheduleError("schedule_locked", "could not acquire schedule state lock");
  }
  async start() {
    await ensureSecureDirectory(this.directory);
    await this.acquireLock();
    try {
      try {
        const stat = await fs.lstat(this.file);
        assertOwnedPrivate(stat, "state");
        this.state = validState(JSON.parse(await fs.readFile(this.file, "utf8")), this.binding);
      } catch (error) {
        if (error?.code !== "ENOENT") {
          if (error instanceof Error && error.code) throw error;
          const backup = `${this.file}.corrupt-${Date.now()}`;
          await fs.rename(this.file, backup).catch(() => {});
        }
        this.state = empty(this.binding);
        await this.persist();
      }
      this.state.ownerEpoch = (this.state.ownerEpoch || 0) + 1;
      await this.persist();
      return this;
    } catch (error) {
      await this.close().catch(() => {});
      throw error;
    }
  }
  async persist() {
    this.state.updatedAt = new Date().toISOString();
    const temporary = path.join(this.directory, `.${path.basename(this.file)}.${process.pid}.${crypto.randomBytes(6).toString("hex")}.tmp`);
    try {
      await fs.writeFile(temporary, `${JSON.stringify(this.state)}\n`, { mode: 0o600, flag: "wx" });
      await fs.rename(temporary, this.file); await fs.chmod(this.file, 0o600);
    } finally { await fs.unlink(temporary).catch(() => {}); }
  }
  mutate(fn) {
    const operation = this.tail.then(async () => { const result = await fn(this.state); await this.persist(); return result; });
    this.tail = operation.catch(() => {}); return operation;
  }
  snapshot() { return this.state; }
  async addHistory(run) {
    return this.mutate((state) => {
      state.history.push(run);
      const perTask = new Map();
      // Retain the newest entries per task and across the session.
      state.history = state.history.slice().reverse().filter((entry) => { const count = perTask.get(entry.taskId) || 0; perTask.set(entry.taskId, count + 1); return count < MAX_HISTORY_PER_TASK; }).reverse().slice(-MAX_HISTORY_PER_SESSION);
    });
  }
  async close() {
    await this.tail;
    try {
      const stat = await fs.lstat(this.lockFile); assertOwnedPrivate(stat, "lock");
      const lock = JSON.parse(await fs.readFile(this.lockFile, "utf8"));
      if (lock?.token === this.ownerToken) await fs.unlink(this.lockFile);
    } catch (error) { if (error?.code !== "ENOENT") throw error; }
  }
}
