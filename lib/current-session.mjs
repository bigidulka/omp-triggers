import fs from "node:fs/promises";
import path from "node:path";
import { globalTimers } from "./host-runtime.mjs";

export const NO_PERSISTENT_SESSION_MESSAGE = "Scheduling requires a saved active OMP session.";
export const SESSION_STOPPING_MESSAGE = "Scheduling is unavailable because the current OMP session is shutting down.";

const REPLACED = Symbol("current-session binding replaced");
const STOPPED = Symbol("current-session stopped");

async function canonicalExistingPath(value) {
  if (!value) return undefined;
  return fs.realpath(path.resolve(value));
}

async function canonicalCwd(value) {
  if (!value) return undefined;
  try { return await canonicalExistingPath(value); } catch (error) {
    if (error?.code === "ENOENT") return path.resolve(value);
    throw error;
  }
}

/**
 * Read OMP's saved-session identity. A file that OMP has selected but not yet
 * created is deliberately not a binding: session_start precedes file creation
 * for some OMP startup paths, and an unsaved bridge cannot own schedules.
 */
export async function savedSessionBinding(ctx) {
  const manager = ctx?.sessionManager;
  const sessionId = manager?.getSessionId?.();
  const file = manager?.getSessionFile?.();
  const cwd = ctx?.cwd || manager?.getCwd?.();
  if (!sessionId || !file || !cwd) return undefined;

  let sessionFile;
  try {
    sessionFile = await canonicalExistingPath(file);
  } catch (error) {
    if (error?.code === "ENOENT") return undefined;
    throw error;
  }
  const resolvedCwd = await canonicalCwd(cwd);
  if (!sessionFile || !resolvedCwd) return undefined;

  // sessionId is runtime metadata, not durable identity. OMP can issue a new
  // ID when it resumes the same saved JSONL in the same working directory.
  return {
    sessionId,
    sessionFile,
    cwd: resolvedCwd,
    key: JSON.stringify([sessionFile, resolvedCwd]),
  };
}

export function createCurrentSessionController(options) {
  const service = new CurrentSessionService(options);
  return {
    getOrStart: (ctx) => service.getOrStart(ctx),
    start: (ctx) => service.start(ctx),
    warm: (ctx) => service.warm(ctx),
    refresh: (ctx) => service.warm(ctx),
    currentBridge: () => service.currentBridge(),
    currentBinding: () => service.currentBinding(),
    unbind: (reason) => service.unbind(reason),
    stopReason: () => service.stopReason,
  };
}

/**
 * Owns one live bridge. Transitions are serialized, but a newer binding marks
 * an older queued/starting transition obsolete immediately. Obsolete work
 * cleans up itself; only this controller's callers see the replacement marker.
 */
export class CurrentSessionService {
  constructor({ createBridge, timers = globalTimers() }) {
    this.createBridge = createBridge;
    this.timers = timers;
    this.bridge = undefined;
    this.binding = undefined;
    this.transition = undefined;
    this.tail = Promise.resolve();
    this.stopped = false;
    this.stopReason = undefined;
  }

  currentBridge() { return this.bridge; }
  currentBinding() { return this.binding ? { ...this.binding } : undefined; }

  /** Opportunistic lifecycle warm-up. Never starts a bridge without a file. */
  async start(ctx) {
    // Only session_start authorizes a controller invalidated by shutdown.
    this.stopped = false;
    this.stopReason = undefined;
    return this.warm(ctx);
  }

  async warm(ctx) {
    if (this.stopped) return undefined;
    const binding = await savedSessionBinding(ctx);
    if (!binding) {
      // OMP can emit session_start after selecting a lexical JSONL pathname but
      // before it creates that file. Wait for persistence without creating the
      // old unsaved bridge; a tool with a saved file remains authoritative.
      if (ctx?.sessionManager?.getSessionFile?.()) this.waitForPersistence(ctx);
      return undefined;
    }
    this.cancelWaiter();
    const bridge = await this.ensure(ctx, binding);
    return bridge === REPLACED || bridge === STOPPED ? undefined : bridge;
  }

  cancelWaiter() {
    if (this.waiter) this.waiter.timers.clear(this.waiter.timer);
    this.waiter = undefined;
  }

  waitForPersistence(ctx, remaining = 200) {
    this.cancelWaiter();
    const timers = ctx?.timers || this.timers;
    const record = { timer: undefined, timers };
    const retry = async () => {
      if (this.stopped || this.waiter !== record) return;
      try {
        const binding = await savedSessionBinding(ctx);
        if (binding) {
          this.waiter = undefined;
          await this.ensure(ctx, binding);
          return;
        }
      } catch {
        // Lifecycle warming is best effort. A tool invocation returns actual
        // filesystem/lock/startup diagnostics from its own authoritative call.
      }
      if (--remaining > 0 && !this.stopped && this.waiter === record) record.timer = timers.setTimeout(retry, 25);
      else if (this.waiter === record) this.waiter = undefined;
    };
    this.waiter = record;
    record.timer = timers.setTimeout(retry, 0);
  }

  async refresh(ctx) { return this.warm(ctx); }

  /**
   * Authoritative execution path. A binding replacement is an internal
   * lifecycle detail, so the tool re-reads OMP's live context and follows it
   * rather than exposing a synthetic retry error to the model.
   */
  async getOrStart(ctx) {
    let replacements = 0;
    for (;;) {
      if (this.stopped) throw new Error(`${SESSION_STOPPING_MESSAGE}${this.stopReason ? ` Last lifecycle event: ${this.stopReason}.` : ""}`);
      const binding = await savedSessionBinding(ctx);
      if (!binding) throw new Error(NO_PERSISTENT_SESSION_MESSAGE);
      this.cancelWaiter();

      const bridge = await this.ensure(ctx, binding);
      if (bridge === STOPPED || this.stopped) throw new Error(`${SESSION_STOPPING_MESSAGE}${this.stopReason ? ` Last lifecycle event: ${this.stopReason}.` : ""}`);
      if (bridge === REPLACED || bridge !== this.bridge || this.binding?.key !== binding.key) {
        // Three immediate hand-offs cover session_start/file-materialization,
        // info refresh, and a concurrent tool. Yield afterwards so a burst of
        // lifecycle events can finish; never turn this internal race into a
        // model-visible "retry" instruction.
        if (++replacements >= 3) await new Promise((resolve) => (ctx?.timers || this.timers).setTimeout(resolve, 0));
        continue;
      }
      // OMP mutates the live manager while lifecycle work is in flight. Re-read
      // it before returning so the first tool follows that authoritative newer
      // file rather than momentarily publishing the old scheduler.
      const latest = await savedSessionBinding(ctx);
      if (!latest) throw new Error(NO_PERSISTENT_SESSION_MESSAGE);
      if (latest.key !== binding.key) {
        if (++replacements >= 3) await new Promise((resolve) => (ctx?.timers || this.timers).setTimeout(resolve, 0));
        continue;
      }
      if (!bridge?.scheduler) throw new Error(NO_PERSISTENT_SESSION_MESSAGE);
      this.updateBinding(bridge, latest);
      return bridge.scheduler;
    }
  }

  async schedulerFor(ctx) { return this.getOrStart(ctx); }

  updateBinding(bridge, binding) {
    this.binding = binding;
    // Keep runtime metadata current without replacing a scheduler whose
    // durable file/cwd identity did not change.
    if (bridge?.scheduler?.binding) bridge.scheduler.binding.sessionId = binding.sessionId;
    if (bridge && "sessionId" in bridge) bridge.sessionId = binding.sessionId;
  }

  ensure(ctx, binding) {
    if (this.stopped) return Promise.resolve(STOPPED);
    if (this.bridge && this.binding?.key === binding.key) {
      this.updateBinding(this.bridge, binding);
      return Promise.resolve(this.bridge);
    }
    if (this.transition?.key === binding.key) {
      // The newest call supplies current sessionId/name/idle metadata while it
      // shares the same stable file/cwd startup operation.
      this.transition.binding = binding;
      this.transition.ctx = ctx;
      return this.transition.promise;
    }

    const record = { key: binding.key, binding, ctx, promise: undefined };
    this.transition = record;
    const operation = this.tail.catch(() => {}).then(() => this.runTransition(record));
    record.promise = operation.finally(() => {
      if (this.transition === record) this.transition = undefined;
    });
    // Keep queue progress alive after a terminal startup error.
    this.tail = record.promise.catch(() => {});
    return record.promise;
  }

  async runTransition(record) {
    if (this.stopped) return STOPPED;
    if (this.transition !== record) return REPLACED;

    const old = this.bridge;
    if (old && this.binding?.key !== record.key) {
      this.bridge = undefined;
      this.binding = undefined;
      await old.close("session replaced");
      if (this.stopped) return STOPPED;
      if (this.transition !== record) return REPLACED;
    }

    const { binding, ctx } = record;
    const manager = ctx.sessionManager;
    const candidate = this.createBridge({
      sessionId: binding.sessionId,
      sessionFile: binding.sessionFile,
      cwd: binding.cwd,
      getName: () => manager.getSessionName(),
      isIdle: () => ctx.isIdle(),
      timers: ctx?.timers || this.timers,
    });
    try {
      await candidate.start();
    } catch (error) {
      await candidate.close("bridge startup failed").catch(() => {});
      throw error;
    }

    if (this.stopped || this.transition !== record) {
      await candidate.close(this.stopped ? "session shutdown" : "session replaced").catch(() => {});
      return this.stopped ? STOPPED : REPLACED;
    }
    this.bridge = candidate;
    this.updateBinding(candidate, record.binding);
    return candidate;
  }

  async unbind(reason = "session shutdown") {
    this.stopped = true;
    this.stopReason = reason;
    this.cancelWaiter();
    // Invalidating the record makes an in-flight candidate close itself after
    // start; awaiting tail prevents it publishing after shutdown returns.
    this.transition = undefined;
    const old = this.bridge;
    this.bridge = undefined;
    this.binding = undefined;
    await old?.close(reason);
    await this.tail;
  }
}
