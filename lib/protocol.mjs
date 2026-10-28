export const PROTOCOL_VERSION = 1;
export const MAX_LINE_BYTES = 65_536;
export const MAX_PROMPT_BYTES = 32_768;
export const MAX_QUEUE_LENGTH = 32;
export const DEFAULT_REQUEST_TIMEOUT_MS = 300_000;

export class BridgeError extends Error {
  constructor(code, message) {
    super(message);
    this.name = "BridgeError";
    this.code = code;
  }
}

export function byteLength(value) {
  return Buffer.byteLength(value, "utf8");
}

export function validateRequest(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new BridgeError("invalid_request", "request must be a JSON object");
  }
  if (typeof value.id !== "string" || !value.id || value.id.length > 128) {
    throw new BridgeError("invalid_request", "request id must be a non-empty string up to 128 characters");
  }
  const methods = ["ping", "status", "prompt", "schedule.create", "schedule.list", "schedule.get", "schedule.update", "schedule.delete", "schedule.pause", "schedule.resume", "schedule.run_now", "schedule.history"];
  if (!methods.includes(value.method)) {
    throw new BridgeError("unknown_method", `method must be one of ${methods.join(", ")}`);
  }
  if (value.method === "prompt") {
    if (typeof value.prompt !== "string" || !value.prompt.trim()) {
      throw new BridgeError("invalid_request", "prompt must be a non-empty string");
    }
    if (byteLength(value.prompt) > MAX_PROMPT_BYTES) {
      throw new BridgeError("prompt_too_large", `prompt exceeds ${MAX_PROMPT_BYTES} bytes`);
    }
  }
  if (value.method.startsWith("schedule.")) {
    if (value.params !== undefined && (!value.params || typeof value.params !== "object" || Array.isArray(value.params))) {
      throw new BridgeError("invalid_request", "schedule params must be a JSON object");
    }
  }
  return value;
}

export function errorResponse(id, error) {
  return {
    id: typeof id === "string" ? id : null,
    type: "error",
    error: { code: error.code || "internal_error", message: error.message || "internal error" },
  };
}

export function encodeLine(value) {
  const line = `${JSON.stringify(value)}\n`;
  if (byteLength(line) > MAX_LINE_BYTES) throw new BridgeError("response_too_large", "response exceeds line limit");
  return line;
}

/** Strict LF-only JSONL stream parser. */
export class JsonLineParser {
  constructor(onValue, onError, maxBytes = MAX_LINE_BYTES) {
    this.onValue = onValue;
    this.onError = onError;
    this.maxBytes = maxBytes;
    this.buffer = Buffer.alloc(0);
  }

  push(chunk) {
    this.buffer = Buffer.concat([this.buffer, Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)]);
    if (this.buffer.length > this.maxBytes && !this.buffer.includes(0x0a)) {
      this.fail(new BridgeError("line_too_large", `line exceeds ${this.maxBytes} bytes`));
      return;
    }
    let newline;
    while ((newline = this.buffer.indexOf(0x0a)) !== -1) {
      const raw = this.buffer.subarray(0, newline);
      this.buffer = this.buffer.subarray(newline + 1);
      if (raw.length > this.maxBytes) return this.fail(new BridgeError("line_too_large", `line exceeds ${this.maxBytes} bytes`));
      if (raw.includes(0x0d)) return this.fail(new BridgeError("invalid_framing", "only LF JSONL framing is accepted"));
      if (!raw.length) return this.fail(new BridgeError("invalid_request", "empty JSONL line"));
      try {
        this.onValue(validateRequest(JSON.parse(raw.toString("utf8"))));
      } catch (error) {
        this.fail(error instanceof BridgeError ? error : new BridgeError("invalid_json", "invalid JSON request"));
        return;
      }
    }
  }

  end() {
    if (this.buffer.length) this.fail(new BridgeError("invalid_framing", "request must end with LF"));
  }

  fail(error) {
    this.buffer = Buffer.alloc(0);
    this.onError(error);
  }
}
