import net from "node:net";
import crypto from "node:crypto";
import { encodeLine } from "./protocol.mjs";
import { listRegistries, resolveTarget } from "./registry.mjs";

export async function request(socketPath, request, timeoutMs = 300_000) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(socketPath);
    const responses = [];
    let settled = false;
    const finish = (error, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      error ? reject(error) : resolve(value);
    };
    const timer = setTimeout(() => {
      const error = new Error(`bridge request timed out after ${timeoutMs}ms`);
      error.code = "timeout";
      finish(error);
    }, timeoutMs);
    timer.unref?.();
    let buffer = "";
    socket.once("connect", () => socket.write(encodeLine(request)));
    socket.on("data", (chunk) => {
      buffer += chunk.toString("utf8");
      if (Buffer.byteLength(buffer) > 131_072) return finish(new Error("bridge response exceeds limit"));
      let index;
      while ((index = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, index); buffer = buffer.slice(index + 1);
        try {
          const response = JSON.parse(line);
          responses.push(response);
          if (response.type === "result") return finish(null, { response, responses });
          if (response.type === "error") {
            const error = new Error(response.error?.message || "bridge error");
            error.code = response.error?.code;
            error.responses = responses;
            return finish(error);
          }
        } catch { return finish(new Error("invalid JSON response from bridge")); }
      }
    });
    socket.once("error", (error) => finish(error));
    socket.once("end", () => {
      if (!settled) finish(new Error("bridge closed before final response"));
    });
  });
}

export async function callTarget(target, method, options = {}) {
  const record = await resolveTarget(target, options);
  const id = options.id || crypto.randomUUID();
  const { response, responses } = await request(record.socketPath, { id, method, ...(method === "prompt" ? { prompt: options.prompt } : {}), ...(method.startsWith("schedule.") ? { params: options.params || {} } : {}) }, options.timeoutMs);
  return { record, result: response.result, responses };
}

export async function scheduleTarget(target, operation, params = {}, options = {}) {
  return callTarget(target, `schedule.${operation}`, { ...options, params });
}

export { listRegistries };
