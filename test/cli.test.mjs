import assert from "node:assert/strict";
import test from "node:test";
import { parseArgs, resolveInvocation } from "../bin/omp-call.mjs";

test("selector option accepts a positional prompt", () => {
  const invocation = resolveInvocation(parseArgs(["--session-id", "session-1", "reply exactly"]));
  assert.equal(invocation.target, "session-1");
  assert.equal(invocation.method, "prompt");
  assert.equal(invocation.prompt, "reply exactly");
});

test("socket option accepts a positional prompt", () => {
  const invocation = resolveInvocation(parseArgs(["--socket", "/tmp/bridge.sock", "reply exactly"]));
  assert.equal(invocation.options.socket, "/tmp/bridge.sock");
  assert.equal(invocation.method, "prompt");
  assert.equal(invocation.prompt, "reply exactly");
});
test("socket option accepts an option-only prompt", () => {
  const invocation = resolveInvocation(parseArgs(["--socket", "/tmp/bridge.sock", "--prompt", "reply exactly"]));
  assert.equal(invocation.target, undefined);
  assert.equal(invocation.options.socket, "/tmp/bridge.sock");
  assert.equal(invocation.prompt, undefined);
  assert.equal(invocation.options.prompt, "reply exactly");
  assert.equal(invocation.method, "prompt");
});

test("selector option preserves schedule and status commands", () => {
  const schedule = resolveInvocation(parseArgs(["--session-id", "session-1", "schedule", "list"]));
  assert.deepEqual(schedule.scheduleArgs, ["session-1", "schedule", "list"]);
  const status = resolveInvocation(parseArgs(["--session-id", "session-1", "status"]));
  assert.equal(status.method, "status");
});
