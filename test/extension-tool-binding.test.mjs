import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const projectRoot = path.resolve(import.meta.dirname, "..");

async function extensionFixture(t, prefix) {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  t.after(async () => {
    try { const descriptor = JSON.parse(await fs.readFile(path.join(temporary, "state", "events", "receiver.json"), "utf8")); process.kill(descriptor.pid, "SIGTERM"); await new Promise((r) => setTimeout(r, 150)); } catch {}
    await fs.rm(temporary, { recursive: true, force: true });
  });
  const sessionFile = path.join(temporary, "saved.jsonl"); const runtime = path.join(temporary, "runtime"); const state = path.join(temporary, "state");
  await fs.writeFile(sessionFile, "");
  await fs.mkdir(path.join(temporary, "extensions"), { recursive: true });
  await fs.copyFile(path.join(projectRoot, "extensions", "index.ts"), path.join(temporary, "extensions", "index.ts"));
  await fs.symlink(path.join(projectRoot, "lib"), path.join(temporary, "lib"), "dir");
  return { temporary, sessionFile, runtime, state, script: path.join(temporary, "invoke.mjs") };
}

function invoke(fixture) {
  return spawnSync(process.execPath, ["--experimental-strip-types", fixture.script], {
    env: { ...process.env, OMP_SESSION_BRIDGE_EVENT_PORT: "0", OMP_SESSION_BRIDGE_DIR: fixture.runtime, OMP_SESSION_BRIDGE_STATE_DIR: fixture.state }, encoding: "utf8",
  });
}

test("registered OMP current-session create self-starts from execution context", async (t) => {
  const fixture = await extensionFixture(t, "omp-session-extension-tool-");
  await fs.writeFile(fixture.script, `
    const tools = new Map(); const commands = new Map(); const handlers = new Map();
    const pi = { registerTool: (tool) => tools.set(tool.name, tool), registerCommand: (name, command) => commands.set(name, command), on: (name, handler) => handlers.set(name, handler), sendUserMessage() {} };
    const extension = (await import(${JSON.stringify(path.join(fixture.temporary, "extensions", "index.ts"))})).default;
    extension(pi);
    const ctx = { isIdle: () => true, sessionManager: { getSessionId: () => "saved-tool-session", getSessionFile: () => ${JSON.stringify(fixture.sessionFile)}, getCwd: () => ${JSON.stringify(fixture.temporary)}, getSessionName: () => "saved-tool-session" } };
    const created = await tools.get("session_schedule_write").execute("call", { action: "create", every: "1m", prompt: "bound directly" }, undefined, undefined, ctx);
    const identity = await tools.get("session_schedule_read").execute("identity", { action: "identity" }, undefined, undefined, ctx);
    const guidance = await handlers.get("before_agent_start")({ systemPrompt: "base" }, ctx);
    await handlers.get("session_shutdown")({ reason: "test" }, ctx);
    console.log(JSON.stringify({ tools: [...tools.keys()], commands: Object.fromEntries([...commands].map(([name, command]) => [name, command.description])), write: tools.get("session_schedule_write"), parameters: tools.get("session_schedule_write").parameters, guidance, created: created.details, identity: identity.details }));
  `);
  const run = invoke(fixture);
  assert.equal(run.status, 0, run.stderr);
  const output = JSON.parse(run.stdout);
  assert.ok(output.created.taskId);
  assert.deepEqual(output.tools, ["session_schedule_read", "session_schedule_write", "trigger_manage", "session_event_write", "session_event_read"]);
  assert.match(output.write.description, /bound saved session/);
  assert.match(output.write.description, /delayed, periodic, every-N-minutes, cron, reminder, watchdog, or recurring supervisor/);
  assert.match(output.write.description, /write path=xd:\/\/session_schedule_write/);
  assert.match(output.write.description, /no session selector, shell, OS cron, files, subagents, or cross-session access/);
  assert.equal("sessionId" in output.parameters.properties, false);
  assert.equal("sessionFile" in output.parameters.properties, false);
  assert.deepEqual(output.identity.session, { sessionId: "saved-tool-session", cwd: fixture.temporary });
  assert.match(output.guidance.systemPrompt, /TRIGGER INTENT ROUTING/);
  assert.match(output.guidance.systemPrompt, /write path=xd:\/\/trigger_manage/);
  assert.match(output.guidance.systemPrompt, /natural language/);
  assert.match(output.guidance.systemPrompt, /saved-tool-session/);
  assert.match(output.commands.schedule, /CURRENT-TUI prompt/);
  assert.match(output.commands.loop, /CURRENT-TUI prompts \(not subagent workflows\)/);
});

test("OMP session_start without a file is repaired by saved execution context", async (t) => {
  const fixture = await extensionFixture(t, "omp-session-extension-lifecycle-");
  await fs.writeFile(fixture.script, `
    const tools = new Map(); const handlers = new Map();
    const pi = { registerTool: (tool) => tools.set(tool.name, tool), registerCommand() {}, on: (name, handler) => handlers.set(name, handler), sendUserMessage() {} };
    const extension = (await import(${JSON.stringify(path.join(fixture.temporary, "extensions", "index.ts"))})).default;
    extension(pi);
    const manager = { getSessionId: () => "saved-after-lifecycle", getSessionFile: () => undefined, getCwd: () => ${JSON.stringify(fixture.temporary)}, getSessionName: () => "saved-after-lifecycle" };
    await handlers.get("session_start")({}, { isIdle: () => true, sessionManager: manager });
    manager.getSessionFile = () => ${JSON.stringify(fixture.sessionFile)};
    const ctx = { isIdle: () => true, sessionManager: manager };
    const result = await tools.get("session_schedule_write").execute("call", { action: "create", every: "1m", prompt: "repair lifecycle binding" }, undefined, undefined, ctx);
    await handlers.get("session_shutdown")({ reason: "test" }, ctx);
    console.log(JSON.stringify(result.details));
  `);
  const run = invoke(fixture);
  assert.equal(run.status, 0, run.stderr);
  assert.ok(JSON.parse(run.stdout).taskId);
});

test("one runner can transition between saved sessions without leaking ownership", async (t) => {
  const fixture = await extensionFixture(t, "omp-session-extension-transition-");
  const secondSessionFile = path.join(fixture.temporary, "transition-second.jsonl");
  await fs.writeFile(secondSessionFile, "");
  await fs.writeFile(fixture.script, `
    const tools = new Map(); const handlers = new Map();
    const host = { registerTool: (tool) => tools.set(tool.name, tool), registerCommand() {}, on: (name, handler) => handlers.set(name, handler), sendUserMessage() {} };
    const state = { id: "transition-first", file: ${JSON.stringify(fixture.sessionFile)} };
    const manager = { getSessionId: () => state.id, getSessionFile: () => state.file, getCwd: () => ${JSON.stringify(fixture.temporary)}, getSessionName: () => state.id };
    const ctx = { isIdle: () => true, sessionManager: manager };
    const extension = (await import(${JSON.stringify(path.join(fixture.temporary, "extensions", "index.ts"))})).default;
    await extension(host);
    await tools.get("session_schedule_write").execute("first-create", { action: "create", every: "1m", prompt: "first schedule" }, undefined, undefined, ctx);
    await handlers.get("session_before_switch")({ reason: "resume" }, ctx);
    state.id = "transition-second"; state.file = ${JSON.stringify(secondSessionFile)};
    await handlers.get("session_switch")({}, ctx);
    const created = await tools.get("session_schedule_write").execute("second-create", { action: "create", every: "1m", prompt: "second schedule" }, undefined, undefined, ctx);
    const listed = await tools.get("session_schedule_read").execute("second-list", { action: "list" }, undefined, undefined, ctx);
    await handlers.get("session_shutdown")({ reason: "quit" }, ctx);
    console.log(JSON.stringify({ taskId: created.details.taskId, listed: listed.details.tasks.map((task) => task.id) }));
  `);
  const run = invoke(fixture);
  assert.equal(run.status, 0, run.stderr);
  const output = JSON.parse(run.stdout);
  assert.deepEqual(output.listed, [output.taskId]);
});

test("different in-process extension runners do not stop each other's scheduler", async (t) => {
  const fixture = await extensionFixture(t, "omp-session-extension-concurrent-");
  const secondSessionFile = path.join(fixture.temporary, "second.jsonl");
  await fs.writeFile(secondSessionFile, "");
  await fs.writeFile(fixture.script, `
    function host() {
      const tools = new Map(); const handlers = new Map();
      return { tools, handlers, api: { registerTool: (tool) => tools.set(tool.name, tool), registerCommand() {}, on: (name, handler) => handlers.set(name, handler), sendUserMessage() {} } };
    }
    function context(sessionId, sessionFile) {
      return { isIdle: () => true, sessionManager: { getSessionId: () => sessionId, getSessionFile: () => sessionFile, getCwd: () => ${JSON.stringify(fixture.temporary)}, getSessionName: () => sessionId } };
    }
    const extension = (await import(${JSON.stringify(path.join(fixture.temporary, "extensions", "index.ts"))})).default;
    const first = host(); await extension(first.api);
    const firstCtx = context("first-runner", ${JSON.stringify(fixture.sessionFile)});
    const created = await first.tools.get("session_schedule_write").execute("first-create", { action: "create", every: "1m", prompt: "survives second runner" }, undefined, undefined, firstCtx);
    const second = host(); await extension(second.api);
    const secondCtx = context("second-runner", ${JSON.stringify(secondSessionFile)});
    await second.tools.get("session_schedule_write").execute("second-create", { action: "create", every: "1m", prompt: "independent runner" }, undefined, undefined, secondCtx);
    const listed = await first.tools.get("session_schedule_read").execute("first-list", { action: "list" }, undefined, undefined, firstCtx);
    await second.handlers.get("session_shutdown")({ reason: "quit" }, secondCtx);
    const afterSecondShutdown = await first.tools.get("session_schedule_read").execute("first-list-after", { action: "list" }, undefined, undefined, firstCtx);
    await first.handlers.get("session_shutdown")({ reason: "quit" }, firstCtx);
    console.log(JSON.stringify({ taskId: created.details.taskId, listed: listed.details.tasks.map((task) => task.id), afterSecondShutdown: afterSecondShutdown.details.tasks.map((task) => task.id) }));
  `);
  const run = invoke(fixture);
  assert.equal(run.status, 0, run.stderr);
  const output = JSON.parse(run.stdout);
  assert.deepEqual(output.listed, [output.taskId]);
  assert.deepEqual(output.afterSecondShutdown, [output.taskId]);
});

test("same-session reload retires only the previous owner", async (t) => {
  const fixture = await extensionFixture(t, "omp-session-extension-reload-");
  await fs.writeFile(fixture.script, `
    function host() {
      const tools = new Map(); const handlers = new Map();
      return { tools, handlers, api: { registerTool: (tool) => tools.set(tool.name, tool), registerCommand() {}, on: (name, handler) => handlers.set(name, handler), sendUserMessage() {} } };
    }
    const ctx = { isIdle: () => true, sessionManager: { getSessionId: () => "reload-session", getSessionFile: () => ${JSON.stringify(fixture.sessionFile)}, getCwd: () => ${JSON.stringify(fixture.temporary)}, getSessionName: () => "reload-session" } };
    const extension = (await import(${JSON.stringify(path.join(fixture.temporary, "extensions", "index.ts"))})).default;
    const first = host(); await extension(first.api);
    const created = await first.tools.get("session_schedule_write").execute("first-create", { action: "create", every: "1m", prompt: "reload survives" }, undefined, undefined, ctx);
    const second = host(); await extension(second.api);
    const listed = await second.tools.get("session_schedule_read").execute("second-list", { action: "list" }, undefined, undefined, ctx);
    const firstError = await first.tools.get("session_schedule_read").execute("stale-first", { action: "list" }, undefined, undefined, ctx).then(() => null, (error) => error.message);
    await first.handlers.get("session_shutdown")({ reason: "reload" }, ctx);
    const afterStaleShutdown = await second.tools.get("session_schedule_read").execute("second-list-after", { action: "list" }, undefined, undefined, ctx);
    await second.handlers.get("session_shutdown")({ reason: "quit" }, ctx);
    console.log(JSON.stringify({ taskId: created.details.taskId, listed: listed.details.tasks.map((task) => task.id), firstError, afterStaleShutdown: afterStaleShutdown.details.tasks.map((task) => task.id) }));
  `);
  const run = invoke(fixture);
  assert.equal(run.status, 0, run.stderr);
  const output = JSON.parse(run.stdout);
  assert.deepEqual(output.listed, [output.taskId]);
  assert.equal(output.firstError, "Scheduling is unavailable because the current OMP session is shutting down. Last lifecycle event: same saved session extension replaced.");
  assert.deepEqual(output.afterStaleShutdown, [output.taskId]);
});

test("OMP loads the root extension with callable current-session tools", async (t) => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "omp-session-extension-load-"));
  const sessionFile = path.join(temporary, "saved.jsonl"); const runtime = path.join(temporary, "runtime"); const state = path.join(temporary, "state");
  await fs.writeFile(sessionFile, "");
  t.after(async () => {
    try { const descriptor = JSON.parse(await fs.readFile(path.join(temporary, "state", "events", "receiver.json"), "utf8")); process.kill(descriptor.pid, "SIGTERM"); await new Promise((r) => setTimeout(r, 150)); } catch {}
    await fs.rm(temporary, { recursive: true, force: true });
  });
  const script = path.join(temporary, "invoke.mjs");
  await fs.writeFile(script, `
    const tools = new Map(); const handlers = new Map();
    const omp = { registerTool: (tool) => tools.set(tool.name, tool), registerCommand() {}, on: (name, handler) => handlers.set(name, handler), sendUserMessage() {} };
    const extension = (await import(${JSON.stringify(path.join(projectRoot, "index.ts"))})).default;
    await extension(omp);
    const ctx = { isIdle: () => true, sessionManager: { getSessionId: () => "omp-load", getSessionFile: () => ${JSON.stringify(sessionFile)}, getCwd: () => ${JSON.stringify(temporary)}, getSessionName: () => "omp-load" } };
    const created = await tools.get("session_schedule_write").execute("first", { action: "create", every: "1m", prompt: "loads in OMP" }, undefined, undefined, ctx);
    const listed = await tools.get("session_schedule_read").execute("second", { action: "list" }, undefined, undefined, ctx);
    await handlers.get("session_shutdown")({ reason: "test" }, ctx);
    console.log(JSON.stringify({ tools: [...tools.keys()], taskId: created.details.taskId, listed: listed.details.tasks.map((task) => task.id) }));
  `);
  const run = spawnSync(process.execPath, ["--experimental-strip-types", script], { env: { ...process.env, OMP_SESSION_BRIDGE_EVENT_PORT: "0", OMP_SESSION_BRIDGE_DIR: runtime, OMP_SESSION_BRIDGE_STATE_DIR: state }, encoding: "utf8" });
  assert.equal(run.status, 0, run.stderr);
  const output = JSON.parse(run.stdout);
  assert.deepEqual(output.tools, ["session_schedule_read", "session_schedule_write", "trigger_manage", "session_event_write", "session_event_read"]);
  assert.deepEqual(output.listed, [output.taskId]);
});
