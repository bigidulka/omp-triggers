import { timersForContext } from "./host-runtime.mjs";

function runtimeContext(ctx) {
  return {
    sessionManager: ctx?.sessionManager,
    cwd: ctx?.cwd || ctx?.sessionManager?.getCwd?.(),
    isIdle: () => ctx.isIdle(),
    timers: timersForContext(ctx),
  };
}

export function bindLifecycle(omp, sessions, onShutdown = async (reason) => sessions.unbind(reason)) {
  const persistedMessages = (ctx) => {
    const manager = ctx?.sessionManager;
    const entries = manager?.getBranch?.() || manager?.getEntries?.() || [];
    return { entries: entries.slice(-32) };
  };
  const warm = async (_event, ctx) => { await sessions.warm(runtimeContext(ctx)); await sessions.currentBridge()?.refreshRegistry(); };
  const unbind = async (event) => { await sessions.unbind(`session transition: ${event?.reason || "identity change"}`); };
  omp.on("session_start", async (_event, ctx) => { await sessions.start(runtimeContext(ctx)); });
  omp.on("session_info_changed", warm);
  for (const event of ["session_before_switch", "session_before_branch", "session_before_tree"]) omp.on(event, unbind);
  for (const event of ["session_switch", "session_branch", "session_tree"]) omp.on(event, async (_value, ctx) => { await sessions.start(runtimeContext(ctx)); });
  omp.on("input", (event) => { sessions.currentBridge()?.onExternalActivity(event?.source); });
  omp.on("message_end", (event) => { sessions.currentBridge()?.onMessageEnd(event?.message); });
  omp.on("agent_end", (event, ctx) => { if (event?.isTerminal !== false) sessions.currentBridge()?.onAgentSettled(persistedMessages(ctx)); });
  omp.on("session_shutdown", async (event, ctx) => {
    sessions.currentBridge()?.onResponseSaved?.(persistedMessages(ctx));
    const reason = `session shutdown: ${event?.reason || "shutdown"}`; await onShutdown(reason);
  });
}
