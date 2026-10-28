function spec(task) {
  if (task.kind === "at") return task.at;
  if (task.kind === "every") return task.every;
  return task.cron;
}

export function compactIdentity(binding) {
  if (!binding) return undefined;
  return { sessionId: binding.sessionId, cwd: binding.cwd };
}

export function compactTask(task, { includePrompt = false } = {}) {
  const state = task.terminalState || (!task.enabled ? "disabled" : task.paused ? "paused" : "active");
  return {
    id: task.id,
    kind: task.kind,
    spec: spec(task),
    state,
    next: task.nextRunAt || null,
    ...(task.expiresAt ? { expires: task.expiresAt } : {}),
    ...(task.lastRunAt ? { last: task.lastRunAt } : {}),
    ...(task.runCount ? { runs: task.runCount } : {}),
    ...(includePrompt ? { prompt: task.prompt } : {}),
  };
}

export function compactRun(run) {
  return {
    id: run.runId,
    taskId: run.taskId,
    state: run.state,
    reason: run.reason,
    at: run.settledAt || run.queuedAt || run.scheduledAt,
    ...(run.error ? { error: run.error } : {}),
  };
}

export function compactMutation(result, task) {
  return {
    taskId: result.taskId || result.task?.id || task?.id,
    ...(result.runId ? { runId: result.runId } : {}),
    ...(result.deleted ? { state: "deleted" } : {}),
    ...(result.coalesced ? { coalesced: true } : {}),
    ...(task ? { task: compactTask(task) } : result.task ? { task: compactTask(result.task) } : {}),
  };
}
