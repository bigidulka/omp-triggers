import * as currentSession from "../lib/current-session.mjs";
import { runtimeDirectory } from "../lib/registry.mjs";
import { SessionBridge } from "../lib/server.mjs";
import { bindLifecycle } from "../lib/lifecycle.mjs";
import { compactIdentity, compactMutation, compactRun, compactTask } from "../lib/tool-contract.mjs";
import { timersForContext } from "../lib/host-runtime.mjs";
const CONTROLLER_REGISTRY_KEY = Symbol.for("omp-session-bridge.controller-registry");
type HostContext = {
  sessionManager?: unknown;
  cwd?: string;
  isIdle(): boolean;
  setTimeout?: (callback: (...args: unknown[]) => unknown, ms: number, ...args: unknown[]) => unknown;
  setInterval?: (callback: (...args: unknown[]) => unknown, ms: number, ...args: unknown[]) => unknown;
  clearTimer?: (handle: unknown) => void;
};
type SessionBinding = { sessionId: string; sessionFile: string; cwd: string; key: string };
type BridgeController = {
  getOrStart(ctx: unknown): Promise<any>;
  start(ctx: unknown): Promise<any>;
  warm(ctx: unknown): Promise<any>;
  currentBridge(): any;
  currentBinding(): SessionBinding | undefined;
  unbind(reason?: string): Promise<void>;
};
type OwnedController = BridgeController & {
  shutdown(reason?: string): Promise<void>;
  retire(reason?: string): Promise<void>;
};
type HostAPI = {
  on(event: string, handler: (...args: any[]) => unknown): void;
  registerTool(tool: unknown): void;
  registerCommand(name: string, command: unknown): void;
  sendUserMessage(prompt: string): void;
};
type TimerOperations = {
  setTimeout(callback: (...args: unknown[]) => unknown, ms: number, ...args: unknown[]): unknown;
  setInterval(callback: (...args: unknown[]) => unknown, ms: number, ...args: unknown[]): unknown;
  clear(handle: unknown): void;
};
type BridgeOptions = {
  sessionId: string;
  sessionFile: string;
  cwd: string;
  getName(): string | undefined;
  isIdle(): boolean;
  timers: TimerOperations;
};
const registryHost = globalThis as typeof globalThis & { [CONTROLLER_REGISTRY_KEY]?: Map<string, OwnedController> };
const controllerRegistry = registryHost[CONTROLLER_REGISTRY_KEY] ??= new Map<string, OwnedController>();

function text(value: unknown) { return typeof value === "string" ? value : ""; }
function compactOptionalStrings<T extends Record<string, unknown>>(value: T): T {
  return Object.fromEntries(Object.entries(value).filter(([, item]) => item !== "" && item !== undefined)) as T;
}
function toolResult(value: unknown) { return { content: [{ type: "text" as const, text: JSON.stringify(value) }], details: value }; }
function stoppingError(reason?: string) { return new Error(`${currentSession.SESSION_STOPPING_MESSAGE}${reason ? ` Last lifecycle event: ${reason}.` : ""}`); }

function createOwnedController(controller: BridgeController): OwnedController {
  let owned: OwnedController;
  let ownedKey: string | undefined;
  let retired = false;
  let retiredReason: string | undefined;
  let retirement: Promise<void> | undefined;

  const release = (key = ownedKey) => {
    if (key && controllerRegistry.get(key) === owned) controllerRegistry.delete(key);
    if (ownedKey === key) ownedKey = undefined;
  };
  const retire = (reason = "extension runtime retired") => {
    if (retirement) return retirement;
    retired = true;
    retiredReason = reason;
    release();
    retirement = controller.unbind(reason);
    return retirement;
  };
  const claim = async (ctx: unknown) => {
    if (retired) throw stoppingError(retiredReason);
    const binding = await currentSession.savedSessionBinding(ctx);
    if (retired) throw stoppingError(retiredReason);
    if (!binding) return { changed: false };

    const changed = Boolean(ownedKey && ownedKey !== binding.key);
    if (changed) {
      const previousKey = ownedKey;
      await controller.unbind("saved session binding changed");
      release(previousKey);
      if (retired) throw stoppingError(retiredReason);
    }

    const previous = controllerRegistry.get(binding.key);
    if (previous !== owned) {
      controllerRegistry.set(binding.key, owned);
      ownedKey = binding.key;
      try {
        await previous?.retire("same saved session extension replaced");
      } catch (error) {
        release(binding.key);
        throw error;
      }
      if (retired || controllerRegistry.get(binding.key) !== owned) {
        release(binding.key);
        await controller.unbind("saved session ownership superseded").catch(() => {});
        throw stoppingError(retiredReason || "saved session ownership superseded");
      }
    } else {
      ownedKey = binding.key;
    }
    return { changed };
  };

  owned = {
    async getOrStart(ctx) {
      await claim(ctx);
      return controller.getOrStart(ctx);
    },
    async start(ctx) {
      if (retired) return undefined;
      try { await claim(ctx); } catch (error) { if (retired) return undefined; throw error; }
      return controller.start(ctx);
    },
    async warm(ctx) {
      if (retired) return undefined;
      let changed;
      try { ({ changed } = await claim(ctx)); } catch (error) { if (retired) return undefined; throw error; }
      return changed ? controller.start(ctx) : controller.warm(ctx);
    },
    currentBridge: () => controller.currentBridge(),
    currentBinding: () => controller.currentBinding(),
    async unbind(reason) {
      release();
      await controller.unbind(reason);
    },
    shutdown: retire,
    retire,
  };
  return owned;
}

const CURRENT_SESSION_TOOL_SCOPE = "Mounted as xd://session_schedule_read and xd://session_schedule_write. Operates only on this agent's bound saved session; no session selector, shell, OS cron, files, subagents, or cross-session access.";


/** OMP Triggers: same-chat schedules, event webhooks and local IPC. */
export default async function ompSessionBridge(omp: HostAPI) {
  const options = {
    createBridge: (bridgeOptions: BridgeOptions) => new SessionBridge({
      ...bridgeOptions,
      sendUserMessage: (prompt: string) => omp.sendUserMessage(prompt),
      directory: runtimeDirectory(),
    }),
  };
  const sessions = createOwnedController(currentSession.createCurrentSessionController(options));
  const scheduler = async (ctx: HostContext) => {
    const active = await sessions.getOrStart({ ...ctx, timers: timersForContext(ctx) });
    return { active, binding: sessions.currentBinding() };
  };

  omp.registerTool({ name: "session_schedule_read", label: "Read session schedules (compatibility)", description: `Compatibility adapter for inspecting current-session timers, cron, reminders, watchdogs, and recurring cycles. Prefer xd://trigger_manage for new work. Invoke via write path=xd://session_schedule_read with JSON content. ${CURRENT_SESSION_TOOL_SCOPE}`,
    parameters: {
      type: "object", additionalProperties: false,
      properties: {
        action: { enum: ["identity", "list", "get", "history"] },
        taskId: { type: "string", minLength: 1 },
        limit: { type: "integer", minimum: 1, maximum: 200 },
      },
    },
    async execute(_id, params, _signal, _onUpdate, ctx) {
      params = { action: "list", ...params };
      const { active, binding } = await scheduler(ctx); const identity = compactIdentity(binding);
      if (params.action === "identity") return toolResult({ session: identity });
      if (params.action === "list") return toolResult({ session: identity, tasks: active.list().filter((task) => task.enabled && !task.deletedAt).map((task) => compactTask(task)) });
      if (!params.taskId) throw new Error("taskId required");
      if (params.action === "get") return toolResult({ session: identity, task: compactTask(await active.get(params.taskId), { includePrompt: true }) });
      return toolResult({ session: identity, runs: (await active.history(params.taskId, params.limit || 10)).map(compactRun) });
    },
  });
  omp.registerTool({
    name: "session_schedule_write", label: "Schedule current session (compatibility)", description: `Compatibility adapter for create/update/delete/pause/resume/run_now of any delayed, periodic, every-N-minutes, cron, reminder, watchdog, or recurring supervisor request unless OS scheduling was explicitly requested. Prefer xd://trigger_manage for new work. Invoke via write path=xd://session_schedule_write with JSON content. ${CURRENT_SESSION_TOOL_SCOPE}`,
    parameters: {
      type: "object", additionalProperties: false,
      properties: {
        action: { enum: ["create", "update", "delete", "cancel", "pause", "resume", "run_now"], description: "Defaults to create. cancel is an alias for delete." },
        taskId: { type: "string", minLength: 1, description: "Required except create. Use returned taskId or an unambiguous UUID prefix of 8+ characters." }, prompt: { type: "string", minLength: 1 },
        at: { type: "string", description: "RFC3339 including timezone, e.g. 2026-12-25T09:00:00+03:00" }, every: { type: "string", description: "Duration, e.g. 5m. Minimum 1m." }, cron: { type: "string", description: "Five numeric fields in the host timezone; pass exactly one trigger." },
        expires: { type: "string", description: "Recurring expiry: duration such as 7d or RFC3339 deadline. Maximum one year." }, jitter: { type: "string" }, maxQueueWait: { type: "string" }, coalesceKey: { type: "string", minLength: 1, maxLength: 128 }, idempotencyKey: { type: "string", description: "Same key requires identical create parameters; choose a new key for changed parameters." },
      },
    },
    async execute(_id, params, _signal, _onUpdate, ctx) {
      params = { action: "create", ...params };
      if (params.action === "cancel") params = { ...params, action: "delete" };
      const { active, binding } = await scheduler(ctx); const identity = compactIdentity(binding); const input = compactOptionalStrings(params);
      if (params.action === "create") return toolResult({ session: identity, ...compactMutation(await active.create(input)) });
      if (!params.taskId) throw new Error("taskId required");
      let result;
      if (params.action === "update") result = await active.update(params.taskId, input);
      else if (params.action === "delete") result = await active.delete(params.taskId);
      else if (params.action === "pause") result = await active.pause(params.taskId);
      else if (params.action === "resume") result = await active.resume(params.taskId);
      else if (params.action === "run_now") result = await active.runNow(params.taskId, params.idempotencyKey);
      else throw new Error("Unknown scheduler action. Use create, update, delete, pause, resume, or run_now.");
      const task = params.action === "delete" ? undefined : await active.get(params.taskId);
      return toolResult({ session: identity, ...compactMutation(result, task) });
    },
  });

  const triggers = async (ctx: HostContext) => {
    await scheduler(ctx);
    const runtime = sessions.currentBridge()?.triggers;
    if (!runtime) throw new Error("Triggers require a saved active OMP session.");
    return { runtime, binding: sessions.currentBinding() };
  };
  omp.registerTool({
    name: "trigger_manage", label: "Manage triggers for this chat",
    description: "One interface for time, incoming webhook and manual triggers on THIS saved chat. Use the user's natural-language request to create, inspect, edit, pause or run a trigger. Invoke via write path=xd://trigger_manage with JSON. create requires name, prompt and source. get returns the editable prompt and revision; update preserves ID, URL, credential and queued events. Read first and pass expectedRevision when editing. Events wait for idle and runs pin the prompt revision at dispatch. history distinguishes queued/delivered/responded/settled/uncertain; no automatic replay after uncertain delivery. Credentials stay in tokenFile. No session selector.",
    parameters: { type: "object", additionalProperties: false, properties: {
      action: { enum: ["create", "list", "get", "update", "pause", "resume", "delete", "run", "history", "revisions", "reconcile", "status"] },
      id: { type: "string", description: "Returned trigger ID or unambiguous prefix of 8+ characters" },
      runId: { type: "string", description: "Optional for reconcile: inspect saved reply evidence without repeating a run" },
      name: { type: "string" }, prompt: { type: "string", description: "Trusted instruction, editable in this chat" },
      source: { type: "object", additionalProperties: false, properties: {
        type: { enum: ["schedule", "webhook", "manual"] }, at: { type: "string" }, every: { type: "string" }, cron: { type: "string" }, expires: { type: "string" }, jitter: { type: "string" }, grace: { type: "string" },
      } },
      policy: { enum: ["queue", "coalesce", "latest"], description: "queue preserves each signal; coalesce keeps one waiting run; latest replaces older waiting signals" },
      expectedRevision: { type: "integer", minimum: 1 }, maxQueueWait: { type: "string" },
      idempotencyKey: { type: "string" }, limit: { type: "integer", minimum: 1, maximum: 200 },
    } },
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const { runtime, binding } = await triggers(ctx);
      return toolResult({ session: compactIdentity(binding), ...await runtime.manage(compactOptionalStrings({ action: "list", ...params })) });
    },
  });

  // Compatibility devices use the same service, never another inbox.
  omp.registerTool({
    name: "session_event_write", label: "Manage incoming webhooks (compatibility)",
    description: "Compatibility alias for trigger_manage. create requires name and instruction. update preserves webhook ID, URL and credential. New work should use xd://trigger_manage. Operates on this chat only; never print tokenFile contents.",
    parameters: { type: "object", additionalProperties: false, properties: {
      action: { enum: ["create", "update", "pause", "resume", "delete"] }, name: { type: "string" }, instruction: { type: "string" }, subscriptionId: { type: "string" }, expectedRevision: { type: "integer" },
    } },
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const { runtime, binding } = await triggers(ctx);
      const action = params.action || "create";
      const result = await runtime.manage(compactOptionalStrings({ action, id: params.subscriptionId, name: params.name, prompt: params.instruction, expectedRevision: params.expectedRevision, ...(action === "create" ? { source: { type: "webhook" } } : {}) }));
      return toolResult({ session: compactIdentity(binding), subscription: result.trigger, receiverAvailable: true, webhookUrl: result.trigger?.webhookUrl, authentication: "Bearer token read from subscription.tokenFile" });
    },
  });
  omp.registerTool({
    name: "session_event_read", label: "Inspect incoming webhooks (compatibility)",
    description: "Compatibility alias for trigger_manage list/get/history/status on this chat. Does not expose credential contents. responded means final model response observed; settled means OMP terminal event observed, neither proves an external action succeeded.",
    parameters: { type: "object", additionalProperties: false, properties: {
      action: { enum: ["list", "get", "history", "status"] }, subscriptionId: { type: "string" }, limit: { type: "integer", minimum: 1, maximum: 200 },
    } },
    async execute(_id, params, _signal, _onUpdate, ctx) {
      const { runtime, binding } = await triggers(ctx); const action = params.action || "list";
      const result = await runtime.manage({ action, id: params.subscriptionId, limit: params.limit });
      return toolResult({ session: compactIdentity(binding), ...(action === "list" ? { subscriptions: result.triggers.filter((t) => t.source.type === "webhook") } : action === "history" ? { receipts: result.runs } : result) });
    },
  });

  omp.registerCommand("schedule", {
    description: "Manage CURRENT-TUI prompt schedules: list, pause ID, resume ID, delete ID, run-now ID",
    async handler(args, ctx) {
      const [command = "list", id] = args.trim().split(/\s+/, 2); let output;
      try {
        const { active } = await scheduler(ctx);
        if (command === "list") output = { tasks: active.list() };
        else if (command === "pause") output = await active.pause(text(id));
        else if (command === "resume") output = await active.resume(text(id));
        else if (command === "delete") output = await active.delete(text(id));
        else if (command === "run-now") output = await active.runNow(text(id));
        else output = { error: "Use /schedule list|pause ID|resume ID|delete ID|run-now ID. Use session_schedule_write action=create for add/update." };
      } catch (error) { output = { error: error instanceof Error ? error.message : String(error) }; }
      ctx.ui.notify(JSON.stringify(output), command === "list" ? "info" : "warning");
    },
  });
  omp.registerCommand("loop", {
    description: "Create/list/stop recurring CURRENT-TUI prompts (not subagent workflows): /loop 5m prompt, /loop list, /loop stop ID",
    async handler(args, ctx) {
      const [command, ...rest] = args.trim().split(/\s+/); let output;
      try {
        const { active } = await scheduler(ctx);
        if (!command || command === "list") output = { tasks: active.list().filter((task) => task.kind === "every") };
        else if (command === "stop") output = await active.delete(rest[0]);
        else output = await active.create({ every: command, prompt: rest.join(" ") });
      } catch (error) { output = { error: error instanceof Error ? error.message : String(error) }; }
      ctx.ui.notify(JSON.stringify(output), "info");
    },
  });

  omp.on("before_agent_start", async (event, ctx) => {
    const systemPrompt = event && typeof event === "object" && "systemPrompt" in event && typeof event.systemPrompt === "string" ? event.systemPrompt : "";
    const identity = compactIdentity(sessions.currentBinding());
    const bound = identity ? ` Bound session: ${JSON.stringify(identity)}.` : " The binding is resolved from the current tool execution context.";
    return { systemPrompt: `${systemPrompt}\n\nTRIGGER INTENT ROUTING: For requests to run later, periodically, on a Telegram alert, wallet deposit or other external event, use write path=xd://trigger_manage. The user describes the desired behavior in natural language; no slash command is required. One saved trigger holds a trusted editable prompt and a source (schedule/webhook/manual). Bind automatically to THIS chat; never infer another session. If the user says only 'check every five minutes' and the conversation has not established WHAT to check, ask one concise question before creating anything. Do not invent the objective from filenames, directory contents, or an arbitrary placeholder. Once the objective is known, configure it yourself; never ask the user to supply tool names, JSON, IDs or scheduler syntax. For 'create a webhook for this chat', creating the endpoint is a complete request: use a safe generic instruction to briefly summarize incoming events without external side effects unless the conversation specifies another instruction. Return its URL and private credential file path; do not require a Telegram/wallet source choice merely to create a generic webhook. Read get before editing, then update with expectedRevision; preserve the existing ID, URL and credential. For requests such as 'change the instruction' identify the existing trigger from this chat's conversation/list rather than demanding an ID. Preserve the user's notification intent, including staying quiet when a periodic check has no meaningful change. Use nextRunAtLocal when presenting schedule times; RFC3339 timestamps ending in Z are UTC and must never be labeled as local time without conversion. Use history to verify actual delivery. The background service collects signals and schedules while this chat is closed; execution resumes when this saved chat is open and idle. For Telegram and wallet signals, connect an authorized source adapter to the returned webhook; creating the endpoint alone does not start monitoring. Never expose credentials or infer authorization for financial actions from external event data. Uncertain delivery requires reconciliation, not automatic replay.${bound}` };
  });
  bindLifecycle(omp, sessions, (reason) => sessions.shutdown(reason));
}
