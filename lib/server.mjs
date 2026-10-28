import net from "node:net";
import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import { DEFAULT_REQUEST_TIMEOUT_MS, BridgeError, JsonLineParser, MAX_QUEUE_LENGTH, encodeLine, errorResponse } from "./protocol.mjs";
import { artifactPaths, ensureSecureDirectory, removeStaleArtifacts, safeUnlink, writeRegistryAtomic } from "./registry.mjs";
import { Scheduler } from "./scheduler.mjs";
import { globalTimers } from "./host-runtime.mjs";
import { EventInbox } from "./event-inbox.mjs";
import { eventDirectory } from "./event-store.mjs";
import { TriggerRuntime } from "./trigger-runtime.mjs";

function now() { return new Date().toISOString(); }
async function canonicalPath(value) {
  if (!value) return value;
  const lexical = path.resolve(value);
  try { return await fs.realpath(lexical); } catch (error) { if (error?.code === "ENOENT") return lexical; throw error; }
}
function asText(message) {
  if (!message || message.role !== "assistant") return "";
  if (typeof message.content === "string") return message.content;
  if (!Array.isArray(message.content)) return "";
  return message.content.filter((item) => item?.type === "text" && typeof item.text === "string").map((item) => item.text).join("");
}

export class SessionBridge {
  constructor(options) {
    this.sessionId = options.sessionId;
    // start() canonicalizes existing paths while retaining lexical fallback for
    // lifecycle callers that provide a not-yet-created saved-session file.
    this.sessionFile = options.sessionFile ? path.resolve(options.sessionFile) : options.sessionFile;
    this.cwd = options.cwd ? path.resolve(options.cwd) : options.cwd;
    this.getName = options.getName || (() => undefined);
    this.isIdle = options.isIdle;
    this.sendUserMessage = options.sendUserMessage;
    this.directory = options.directory;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    this.maxQueueWaitMs = options.maxQueueWaitMs ?? 24 * 60 * 60 * 1000;
    this.timers = options.timers || globalTimers();
    this.maxQueueLength = options.maxQueueLength ?? MAX_QUEUE_LENGTH;
    this.server = null;
    this.paths = null;
    this.startedAt = null;
    this.queue = [];
    this.active = null;
    this.closed = false;
    this.sockets = new Set();
    this.idlePump = null;
    this.scheduler = null;
    this.stateDirectory = options.stateDirectory;
    this.legacyBackend = options.legacyBackend === true;
  }

  record() {
    return {
      protocolVersion: 1,
      pid: process.pid,
      sessionId: this.sessionId,
      sessionFile: this.sessionFile || null,
      cwd: this.cwd || process.cwd(),
      name: this.getName() || null,
      socketPath: this.paths.socket,
      startedAt: this.startedAt,
      updatedAt: now(),
    };
  }

  status() {
    const scheduledWaiting = this.queue.filter((job) => job.source === "scheduled").length;
    return {
      protocolVersion: 1,
      sessionId: this.sessionId,
      sessionFile: this.sessionFile || null,
      cwd: this.cwd || process.cwd(),
      name: this.getName() || null,
      pid: process.pid,
      idle: Boolean(this.isIdle()),
      active: Boolean(this.active),
      queued: this.queue.length,
      deliveryState: this.active ? "delivered" : scheduledWaiting ? "queued_waiting_for_idle" : this.queue.length ? "queued" : "idle",
      scheduledWaitingForIdle: scheduledWaiting,
      startedAt: this.startedAt,
      scheduler: this.scheduler ? this.scheduler.status() : { enabled: false, reason: this.sessionFile ? "not_started" : "no_persistent_session", activeCount: 0, totalCount: 0, nextDueAt: null, capabilities: [] },
      events: { enabled: Boolean(this.triggers || this.eventInbox), lastError: this.triggers?.lastError || this.eventInbox?.lastError || null },
    };
  }

  async start() {
    if (this.server) return;
    this.sessionFile = await canonicalPath(this.sessionFile);
    this.cwd = await canonicalPath(this.cwd);
    await ensureSecureDirectory(this.directory);
    await removeStaleArtifacts(this.directory);
    this.paths = artifactPaths(this.sessionId, this.directory);
    // Do not clobber a live or unregistered socket. Stale registered artifacts
    // were removed above only after ownership and PID checks.
    try {
      await fs.lstat(this.paths.socket);
      throw new Error(`bridge socket already exists: ${this.paths.socket}`);
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }
    try {
      await fs.lstat(this.paths.registry);
      throw new Error(`bridge registry already exists: ${this.paths.registry}`);
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }
    this.startedAt = now();
    this.closed = false;
    this.server = net.createServer((socket) => this.accept(socket));
    await new Promise((resolve, reject) => {
      this.server.once("error", reject);
      this.server.listen(this.paths.socket, () => { this.server.off("error", reject); resolve(); });
    });
    // Unsaved/no-session OMP invocations lack an immutable file binding, so they
    // intentionally expose no persistent schedules.
    if (this.sessionId && this.sessionFile && this.cwd && !this.legacyBackend) {
      this.triggers = new TriggerRuntime({
        binding: { sessionId: this.sessionId, sessionFile: this.sessionFile, cwd: this.cwd }, directory: this.stateDirectory, timers: this.timers,
        canDispatch: () => !this.closed && !this.active && !this.queue.length && this.isIdle(),
        dispatch: (prompt, callbacks, options) => this.enqueueScheduled(prompt, callbacks, options),
      });
      try { await this.triggers.start(); } catch (e) { await this.close("trigger service startup failed"); throw e; }
      this.scheduler = this.triggers.scheduler;
    }
    if (this.sessionId && this.sessionFile && this.cwd && this.legacyBackend) {
      this.scheduler = new Scheduler({
        binding: { sessionId: this.sessionId, sessionFile: this.sessionFile, cwd: this.cwd },
        directory: this.stateDirectory,
        dispatch: (prompt, callbacks, options) => this.enqueueScheduled(prompt, callbacks, options),
        timers: this.timers,
      });
      await this.scheduler.start();
      this.eventInbox = new EventInbox({
        binding: { sessionId: this.sessionId, sessionFile: this.sessionFile, cwd: this.cwd },
        directory: eventDirectory(this.stateDirectory), timers: this.timers,
        canDispatch: () => !this.closed && !this.active && !this.queue.length && this.isIdle(),
        dispatch: (prompt, callbacks) => this.enqueueScheduled(prompt, callbacks, { source: "event" }),
      });
      await this.eventInbox.start();
    }
    this.idlePump = this.timers.setInterval(() => this.drain(), 1000);
    await this.refreshRegistry();
  }

  async refreshRegistry() {
    if (!this.server || this.closed) return;
    await writeRegistryAtomic(this.paths.registry, this.record(), this.directory);
  }

  write(socket, value) {
    if (!socket.destroyed) socket.write(encodeLine(value));
  }

  accept(socket) {
    this.sockets.add(socket);
    socket.setNoDelay(true);
    let received = false;
    const parser = new JsonLineParser(
      (request) => {
        if (received) {
          this.write(socket, errorResponse(request.id, new BridgeError("invalid_request", "one request per connection")));
          socket.end();
          return;
        }
        received = true;
        this.handleRequest(socket, request);
      },
      (error) => { this.write(socket, errorResponse(null, error)); socket.end(); },
    );
    socket.on("data", (chunk) => parser.push(chunk));
    socket.on("end", () => parser.end());
    socket.on("close", () => this.sockets.delete(socket));
    socket.on("error", () => {});
  }

  handleRequest(socket, request) {
    if (this.closed) return this.failSocket(socket, request.id, "shutdown", "bridge is shutting down");
    if (request.method === "ping") {
      this.write(socket, { id: request.id, type: "result", result: { pong: true, ...this.status() } });
      return socket.end();
    }
    if (request.method === "status") {
      this.write(socket, { id: request.id, type: "result", result: this.status() });
      return socket.end();
    }
    if (request.method.startsWith("schedule.")) return this.handleSchedule(socket, request);
    if (this.queue.length + (this.active ? 1 : 0) >= this.maxQueueLength) {
      return this.failSocket(socket, request.id, "queue_full", `bridge queue limit (${this.maxQueueLength}) reached`);
    }
    const job = { id: request.id, prompt: request.prompt, socket, acceptedAt: now(), timer: null, text: [], source: "external", queueWaitMs: this.maxQueueWaitMs };
    job.timer = this.timers.setTimeout(() => this.queueTimeout(job), job.queueWaitMs);
    this.queue.push(job);
    this.write(socket, { id: job.id, type: "accepted", acceptedAt: job.acceptedAt, queuePosition: this.queue.length });
    this.drain();
  }

  async handleSchedule(socket, request) {
    if (!this.scheduler) return this.failSocket(socket, request.id, "no_persistent_session", "persistent schedules require a saved OMP session");
    try {
      const params = request.params || {}; let result;
      switch (request.method) {
        case "schedule.create": result = await this.scheduler.create(params); break;
        case "schedule.list": result = { tasks: this.scheduler.list() }; break;
        case "schedule.get": result = { task: await this.scheduler.get(params.taskId) }; break;
        case "schedule.update": result = await this.scheduler.update(params.taskId, params); break;
        case "schedule.delete": result = await this.scheduler.delete(params.taskId); break;
        case "schedule.pause": result = await this.scheduler.pause(params.taskId); break;
        case "schedule.resume": result = await this.scheduler.resume(params.taskId); break;
        case "schedule.run_now": result = await this.scheduler.runNow(params.taskId, params.idempotencyKey); break;
        case "schedule.history": result = { history: await this.scheduler.history(params.taskId, params.limit) }; break;
        default: throw new BridgeError("unknown_method", "unknown schedule method");
      }
      this.write(socket, { id: request.id, type: "result", result }); socket.end();
    } catch (error) { this.failSocket(socket, request.id, error.code || "internal_error", error.message || "schedule operation failed"); }
  }

  enqueueScheduled(prompt, callbacks, options = {}) {
    if (this.closed || this.queue.length + (this.active ? 1 : 0) >= this.maxQueueLength) {
      callbacks.onFailed?.(new BridgeError(this.closed ? "shutdown" : "queue_full", this.closed ? "bridge is shutting down" : "bridge queue is full")); return false;
    }
    const queueWaitMs = options.maxQueueWaitMs ?? this.maxQueueWaitMs;
    const job = { id: `schedule:${Date.now()}:${Math.random()}`, prompt, acceptedAt: now(), timer: null, text: [], source: options.source || "scheduled", callbacks, deliveryPolicy: options.deliveryPolicy || "idle_only", coalesceKey: options.coalesceKey, queueWaitMs };
    job.timer = this.timers.setTimeout(() => this.queueTimeout(job), queueWaitMs); this.queue.push(job); this.drain();
    // Deletion can remove an undelivered schedule. An already-delivered OMP
    // turn cannot be revoked, so it remains quarantined until settlement.
    return () => {
      const index = this.queue.indexOf(job);
      if (index !== -1) { this.queue.splice(index, 1); this.finish(job, "error", new BridgeError("schedule_deleted", "schedule was deleted")); return; }
      if (this.active === job && !job.finished) this.quarantine(job, new BridgeError("schedule_deleted", "schedule was deleted"));
    };
  }

  /** Only dispatch while OMP is idle. Interactive activity is never overtaken. */
  drain() {
    if (this.closed || this.active || !this.queue.length || !this.isIdle()) return;
    const job = this.queue.shift();
    this.timers.clear(job.timer);
    job.timer = this.timers.setTimeout(() => this.executionTimeout(job), this.timeoutMs);
    this.active = job;
    try {
      this.sendUserMessage(job.prompt);
      if (job.source !== "external") job.callbacks?.onDelivered?.();
      else this.write(job.socket, { id: job.id, type: "accepted", state: "delivered", deliveredAt: now() });
    } catch (error) {
      this.finish(job, "error", new BridgeError("delivery_failed", error?.message || "OMP rejected prompt"));
    }
  }

  onMessageEnd(message, metadata = {}) {
    if (!this.active || this.active.quarantined) return;
    const text = asText(message);
    // Keep the most recent completed assistant text: tool-call intermediates do
    // not become part of the caller's final answer.
    if (text) this.active.text = [text];
    if (message?.role === "assistant" && ["error", "aborted"].includes(message.stopReason)) this.active.agentError = new BridgeError(message.stopReason === "aborted" ? "agent_aborted" : "agent_error", `OMP assistant ended with ${message.stopReason}`);
    if (message?.role === "assistant" && message?.stopReason === "stop" && text) {
      this.active.response = { messageId: null, timestamp: message.timestamp, stopReason: message.stopReason,
        textHash: crypto.createHash("sha256").update(text).digest("hex") };
      this.active.callbacks?.onResponse?.(this.active.response);
    }
  }

  onResponseSaved({ entries = [] } = {}) {
    const job = this.active;
    if (!job?.response || job.quarantined || job.timedOut) return;
    // message_end precedes SessionManager.appendMessage. The leaf at that
    // moment is often the user prompt, so only identify the persisted reply
    // after validating both its timestamp and content digest.
    const entry = [...entries].reverse().find((e) => e.message?.role === "assistant" && e.message.timestamp === job.response.timestamp &&
      crypto.createHash("sha256").update(asText(e.message)).digest("hex") === job.response.textHash);
    if (entry) { job.response.messageId = entry.id; job.callbacks?.onResponse?.(job.response); }
  }

  onAgentSettled(metadata = {}) {
    if (this.closed) return;
    this.onResponseSaved(metadata);
    if (this.active?.timedOut || this.active?.quarantined) this.release(this.active);
    else if (this.active?.agentError) this.finish(this.active, "error", this.active.agentError);
    else if (this.active) this.finish(this.active, "result", this.active.text.join(""));
    this.drain();
  }

  /** Do not insert a bridge request ahead of interactive/RPC activity. */
  onExternalActivity(source) {
    if (source === "extension") return;
    if (this.active) {
      // A non-extension turn means ensuing assistant output cannot safely be
      // attributed to the active bridge request. Fail once and hold this lane
      // until OMP emits its matching eventual settlement.
      this.quarantine(this.active, new BridgeError("session_busy", "interactive or RPC activity occurred during bridge delivery"));
      return;
    }
    if (!this.queue.length) return;
    const queued = this.queue.splice(0);
    for (const job of queued) {
      // Durable work stays in FIFO behind interactive activity rather than being dropped.
      if (job.source !== "external") { this.queue.push(job); continue; }
      this.finish(job, "error", new BridgeError("session_busy", "interactive or RPC activity occurred before bridge delivery; retry after it settles"));
    }
  }

  quarantine(job, error) {
    if (!job || job.finished || this.active !== job) return;
    job.quarantined = true; job.finished = true; this.timers.clear(job.timer);
    if (job.source !== "external") job.callbacks?.onFailed?.(error);
    else { this.write(job.socket, errorResponse(job.id, error)); if (!job.socket.destroyed) job.socket.end(); }
  }

  queueTimeout(job) {
    const queuedAt = this.queue.indexOf(job);
    if (queuedAt === -1 || job.finished) return;
    this.queue.splice(queuedAt, 1);
    this.finish(job, "error", new BridgeError("queue_timeout", `request waited more than ${job.queueWaitMs}ms for an idle session`));
    this.drain();
  }

  executionTimeout(job) {
    if (this.active !== job || job.finished) return;
    // A timed-out caller does not prove OMP stopped. Quarantine the lane until
    // the eventual settled/shutdown signal so late events cannot hit another job.
    job.timedOut = true; this.timers.clear(job.timer);
    if (job.source !== "external") job.callbacks?.onFailed?.(new BridgeError("timeout", `delivered request exceeded ${this.timeoutMs}ms`));
    else { job.finished = true; this.write(job.socket, errorResponse(job.id, new BridgeError("timeout", `delivered request exceeded ${this.timeoutMs}ms`))); if (!job.socket.destroyed) job.socket.end(); }
  }

  release(job) { if (this.active === job) this.active = null; this.timers.clear(job.timer); }

  finish(job, kind, payload) {
    if (!job || job.finished) return;
    job.finished = true; this.release(job);
    if (job.source !== "external") {
      if (kind === "result") job.callbacks?.onSettled?.(payload);
      else job.callbacks?.onFailed?.(payload);
      return;
    }
    if (kind === "result") this.write(job.socket, { id: job.id, type: "result", result: { text: payload, settledAt: now() } });
    else this.write(job.socket, errorResponse(job.id, payload));
    if (!job.socket.destroyed) job.socket.end();
  }

  failSocket(socket, id, code, message) {
    this.write(socket, errorResponse(id, new BridgeError(code, message)));
    socket.end();
  }

  async close(reason = "bridge shutdown") {
    if (this.closed) return;
    this.closed = true;
    if (this.eventInbox) this.eventInbox.closed = true;
    if (this.triggers) this.triggers.closed = true;
    this.timers.clear(this.idlePump); this.idlePump = null;
    for (const job of [...this.queue, this.active].filter(Boolean)) {
      if (job.timedOut) { if (job.source !== "external") job.callbacks?.onFailed?.(new BridgeError("shutdown", reason)); this.release(job); }
      else this.finish(job, "error", new BridgeError("shutdown", reason));
    }
    this.queue = [];
    await this.eventInbox?.close(); this.eventInbox = null;
    await this.triggers?.close(); this.triggers = null;
    await this.scheduler?.stop(); this.scheduler = null;
    // Let final shutdown errors flush before forcing malicious/idle clients out.
    // server.close waits for open connections, so bound that wait to preserve OMP
    // session replacement/shutdown responsiveness.
    for (const socket of this.sockets) if (!socket.destroyed) socket.end();
    if (this.server) {
      const closed = new Promise((resolve) => this.server.close(() => resolve()));
      await Promise.race([closed, new Promise((resolve) => this.timers.setTimeout(resolve, 250))]);
      for (const socket of this.sockets) if (!socket.destroyed) socket.destroy();
      await Promise.race([closed, new Promise((resolve) => this.timers.setTimeout(resolve, 250))]);
    }
    this.sockets.clear();
    this.server = null;
    if (this.paths) {
      await safeUnlink(this.paths.registry, this.directory).catch(() => {});
      await safeUnlink(this.paths.socket, this.directory).catch(() => {});
    }
  }
}
