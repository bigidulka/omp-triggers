import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { CurrentSessionService, NO_PERSISTENT_SESSION_MESSAGE, savedSessionBinding } from "../lib/current-session.mjs";

function context(root, state) {
  return {
    isIdle: () => true,
    sessionManager: {
      getSessionId: () => state.id,
      getSessionFile: () => state.file,
      getCwd: () => state.cwd || root,
      getSessionName: () => state.id,
    },
  };
}

async function savedContext(root, id) {
  const file = path.join(root, `${id}.jsonl`);
  await fs.writeFile(file, "");
  return context(root, { id, file });
}

function bridgeFactory({ starts, closes, gates = {}, failStarts = 0 }) {
  return (options) => {
    const scheduler = {
      binding: { sessionId: options.sessionId, sessionFile: options.sessionFile, cwd: options.cwd },
      async create(params) { return { taskId: `${this.binding.sessionId}-task`, params }; },
      list: () => [],
    };
    return {
      scheduler,
      sessionId: options.sessionId,
      async start() {
        starts.push(options.sessionFile);
        await gates[options.sessionFile];
        if (failStarts > 0) { failStarts--; throw new Error(`bridge startup failed for ${options.sessionId}`); }
      },
      async close(reason) { closes.push([options.sessionFile, reason]); },
    };
  };
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));
async function waitFor(check) {
  for (let attempt = 0; attempt < 50; attempt++) {
    if (check()) return;
    await tick();
  }
  throw new Error("condition did not become true");
}

test("saved binding uses canonical file/cwd identity and treats sessionId as metadata", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-session-current-identity-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const actual = path.join(root, "actual"); const linked = path.join(root, "linked");
  await fs.mkdir(actual); await fs.symlink(actual, linked, "dir");
  const file = path.join(actual, "saved.jsonl"); await fs.writeFile(file, "");
  const one = await savedSessionBinding(context(root, { id: "one", file: path.join(linked, "saved.jsonl"), cwd: linked }));
  const two = await savedSessionBinding(context(root, { id: "two", file, cwd: actual }));
  assert.equal(one.key, two.key);
  assert.equal(one.sessionFile, file);
  assert.equal(one.cwd, actual);
});

test("warm waits for a materialized saved file and concurrent tools self-start exactly once", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-session-current-materialize-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const actual = path.join(root, "actual"); const linked = path.join(root, "linked");
  await fs.mkdir(actual); await fs.symlink(actual, linked, "dir");
  const lexicalFile = path.join(linked, "saved.jsonl");
  const state = { id: "first-runtime-id", file: lexicalFile, cwd: linked };
  const starts = []; const closes = [];
  const service = new CurrentSessionService({ createBridge: bridgeFactory({ starts, closes }) });

  assert.equal(await service.warm(context(root, state)), undefined, "session_start must not create an unsaved bridge");
  assert.deepEqual(starts, []);
  await fs.writeFile(path.join(actual, "saved.jsonl"), "");
  const toolCtx = context(root, state);
  const [one, two] = await Promise.all([service.getOrStart(toolCtx), service.getOrStart(toolCtx)]);
  assert.equal(one, two);
  assert.equal(starts.length, 1);
  assert.equal(one.binding.sessionFile, path.join(actual, "saved.jsonl"));
  await service.unbind();
  assert.deepEqual(closes, [[path.join(actual, "saved.jsonl"), "session shutdown"]]);
});

test("same saved file with a new OMP sessionId updates metadata without replacement", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-session-current-metadata-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const state = { id: "first", file: path.join(root, "saved.jsonl") };
  await fs.writeFile(state.file, "");
  const starts = []; const closes = []; const service = new CurrentSessionService({ createBridge: bridgeFactory({ starts, closes }) });
  const first = await service.getOrStart(context(root, state));
  state.id = "resumed-with-new-id";
  const second = await service.getOrStart(context(root, state));
  assert.equal(first, second);
  assert.equal(second.binding.sessionId, "resumed-with-new-id");
  assert.equal(service.currentBridge().sessionId, "resumed-with-new-id");
  assert.equal(starts.length, 1);
  assert.deepEqual(closes, []);
  await service.unbind();
});

test("a stale start cleans itself and the authoritative tool follows the newer binding", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-session-current-replace-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const firstFile = path.join(root, "first.jsonl"); const secondFile = path.join(root, "second.jsonl");
  await fs.writeFile(firstFile, ""); await fs.writeFile(secondFile, "");
  // OMP updates one current session manager in place; an in-flight tool must
  // re-read that authoritative object rather than report a synthetic retry.
  const state = { id: "first", file: firstFile };
  const liveCtx = context(root, state);
  let releaseFirst;
  const gate = new Promise((resolve) => { releaseFirst = resolve; });
  const starts = []; const closes = [];
  const service = new CurrentSessionService({ createBridge: bridgeFactory({ starts, closes, gates: { [firstFile]: gate } }) });
  const staleCaller = service.getOrStart(liveCtx);
  await tick();
  state.id = "second"; state.file = secondFile;
  const newest = service.getOrStart(liveCtx);
  releaseFirst();
  const scheduler = await newest;
  assert.equal(scheduler.binding.sessionId, "second");
  assert.equal((await staleCaller).binding.sessionId, "second", "no lifecycle retry error leaks from stale work");
  assert.deepEqual(starts, [firstFile, secondFile]);
  assert.deepEqual(closes, [[firstFile, "session replaced"]]);
  await service.unbind();
});

test("unbind during startup cleans the candidate and later saved tool calls recover", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-session-current-unbind-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const ctx = await savedContext(root, "shutdown"); const file = ctx.sessionManager.getSessionFile();
  let release;
  const starts = []; const closes = []; const gate = new Promise((resolve) => { release = resolve; });
  const service = new CurrentSessionService({ createBridge: bridgeFactory({ starts, closes, gates: { [file]: gate } }) });
  const pending = service.getOrStart(ctx);
  await waitFor(() => starts.length === 1);
  const stopping = service.unbind("test shutdown");
  release();
  await stopping;
  await assert.rejects(pending, /shutting down\. Last lifecycle event: test shutdown\./);
  assert.deepEqual(closes, [[file, "session shutdown"]]);
  await service.start(ctx); // a subsequent session_start re-enables this controller
  const recovered = await service.getOrStart(ctx);
  assert.equal(recovered.binding.sessionId, "shutdown");
  assert.equal(starts.length, 2);
  await service.unbind();
});

test("terminal startup failures and unsaved tool calls retain their real diagnostics", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "pi-session-current-errors-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  const unsaved = context(root, { id: "unsaved", file: path.join(root, "not-created.jsonl") });
  const starts = []; const closes = []; const service = new CurrentSessionService({ createBridge: bridgeFactory({ starts, closes, failStarts: 1 }) });
  await assert.rejects(service.getOrStart(unsaved), new RegExp(NO_PERSISTENT_SESSION_MESSAGE));
  const saved = await savedContext(root, "failure");
  await assert.rejects(service.getOrStart(saved), /bridge startup failed for failure/);
  assert.deepEqual(closes, [[saved.sessionManager.getSessionFile(), "bridge startup failed"]]);
  assert.equal((await service.getOrStart(saved)).binding.sessionId, "failure");
  await service.unbind();
});
