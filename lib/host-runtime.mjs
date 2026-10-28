export class TimerOperations {}

export function globalTimers() {
  return {
    setTimeout(callback, ms, ...args) {
      const handle = globalThis.setTimeout(callback, ms, ...args);
      handle?.unref?.();
      return handle;
    },
    setInterval(callback, ms, ...args) {
      const handle = globalThis.setInterval(callback, ms, ...args);
      handle?.unref?.();
      return handle;
    },
    clear(handle) {
      globalThis.clearTimeout(handle);
      globalThis.clearInterval(handle);
    },
  };
}

export function timersForContext(ctx) {
  if (typeof ctx?.setTimeout !== "function" || typeof ctx?.setInterval !== "function" || typeof ctx?.clearTimer !== "function") return globalTimers();
  return {
    setTimeout: (callback, ms, ...args) => ctx.setTimeout(callback, ms, ...args),
    setInterval: (callback, ms, ...args) => ctx.setInterval(callback, ms, ...args),
    clear: (handle) => { if (handle !== undefined && handle !== null) ctx.clearTimer(handle); },
  };
}
