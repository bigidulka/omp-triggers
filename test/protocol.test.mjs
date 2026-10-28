import assert from "node:assert/strict";
import test from "node:test";
import { JsonLineParser, MAX_LINE_BYTES, validateRequest } from "../lib/protocol.mjs";

test("JSONL parser accepts split LF request and rejects CRLF", () => {
  const values = []; const errors = [];
  const parser = new JsonLineParser((value) => values.push(value), (error) => errors.push(error));
  parser.push('{"id":"a","method":"ping"');
  parser.push("}\n");
  assert.deepEqual(values, [{ id: "a", method: "ping" }]);
  parser.push('{"id":"b","method":"ping"}\r\n');
  assert.equal(errors[0].code, "invalid_framing");
});

test("protocol validates prompt and framing limits", () => {
  assert.throws(() => validateRequest({ id: "x", method: "prompt", prompt: "" }), { code: "invalid_request" });
  assert.throws(() => validateRequest({ id: "x", method: "prompt", prompt: "x".repeat(32_769) }), { code: "prompt_too_large" });
  const errors = [];
  new JsonLineParser(() => {}, (error) => errors.push(error)).push(Buffer.alloc(MAX_LINE_BYTES + 1, 0x61));
  assert.equal(errors[0].code, "line_too_large");
});
