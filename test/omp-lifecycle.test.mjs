import assert from "node:assert/strict";
import test from "node:test";
import { bindLifecycle } from "../lib/lifecycle.mjs";
import { timersForContext } from "../lib/host-runtime.mjs";

function fixture() {
  const handlers = new Map();
  const calls = [];
  let startedContext;
  const bridge = {
    onAgentSettled: () => calls.push("settled"),
    onExternalActivity: (source) => calls.push(`input:${source}`),
    onMessageEnd: () => calls.push("message"),
    refreshRegistry: async () => calls.push("refresh"),
  };
  const sessions = {
    start: async (context) => { startedContext = context; calls.push("start"); },
    warm: async () => calls.push("warm"),
    unbind: async (reason) => calls.push(`unbind:${reason}`),
    currentBridge: () => bridge,
  };
  bindLifecycle({ on: (event, handler) => handlers.set(event, handler) }, sessions);
  return { handlers, calls, get startedContext() { return startedContext; } };
}
const ctx = { isIdle: () => true, cwd: "/documented/cwd", sessionManager: { getCwd: () => "/manager/cwd" } };
test("lifecycle context prefers documented cwd", async () => {
  const fixtureState = fixture();
  await fixtureState.handlers.get("session_start")({}, ctx);
  assert.equal(fixtureState.startedContext.cwd, "/documented/cwd");
});

const transitionCtx = { isIdle: () => true, sessionManager: {} };

test("OMP terminal agent_end settles while non-terminal maintenance does not", async () => {
  const { handlers, calls } = fixture();
  await handlers.get("agent_end")({ isTerminal: false }, transitionCtx);
  assert.deepEqual(calls, []);
  await handlers.get("agent_end")({ isTerminal: true }, transitionCtx);
  await handlers.get("agent_end")({}, transitionCtx);
  assert.deepEqual(calls, ["settled", "settled"]);
});

test("OMP session transitions unbind before and restart after identity changes", async () => {
  const { handlers, calls } = fixture();
  for (const kind of ["switch", "branch", "tree"]) {
    await handlers.get(`session_before_${kind}`)({ reason: kind }, transitionCtx);
    await handlers.get(`session_${kind}`)({}, transitionCtx);
  }
  assert.deepEqual(calls, [
    "unbind:session transition: switch", "start",
    "unbind:session transition: branch", "start",
    "unbind:session transition: tree", "start",
  ]);
});

test("OMP shutdown delegates cleanup exactly once to the controller owner", async () => {
  const handlers = new Map(); const calls = [];
  const sessions = {
    start: async () => {}, warm: async () => {}, unbind: async (reason) => calls.push(`unbind:${reason}`), currentBridge: () => undefined,
  };
  bindLifecycle({ on: (event, handler) => handlers.set(event, handler) }, sessions, async (reason) => calls.push(`owner:${reason}`));
  await handlers.get("session_shutdown")({ reason: "reload" }, transitionCtx);
  assert.deepEqual(calls, ["owner:session shutdown: reload"]);
});

test("OMP managed timers are selected and cleared through the handler context", () => {
  const calls = [];
  const context = {
    setTimeout: (callback, ms) => { calls.push(["timeout", ms]); return { callback }; },
    setInterval: (callback, ms) => { calls.push(["interval", ms]); return { callback }; },
    clearTimer: (handle) => calls.push(["clear", handle]),
  };
  const timers = timersForContext(context);
  const timeout = timers.setTimeout(() => {}, 10);
  const interval = timers.setInterval(() => {}, 20);
  timers.clear(timeout);
  timers.clear(interval);
  assert.deepEqual(calls.map((entry) => entry[0]), ["timeout", "interval", "clear", "clear"]);
});
