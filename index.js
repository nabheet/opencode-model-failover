// opencode-model-failover — OpenCode v2 plugin.
//
// Native model failover built entirely on the OpenCode v2 plugin API:
//
//   ctx.event.subscribe()       -> session.execution.failed carries
//                                  { data: { sessionID, error: { type, message } } }
//   ctx.model.list()            -> which provider/model pairs actually resolve
//   ctx.session.switchModel()   -> move the session onto a healthy model
//   ctx.session.context()       -> read the transcript (only for autoRetry)
//   ctx.session.prompt()        -> replay the last user message (only for autoRetry)
//
// Discovery: a plugin directory exposing index.js is loaded automatically from
// .opencode/plugins/. Installing the package is enough; a config entry is only
// needed to pass options:
//
//   { "plugins": [
//       { "package": "opencode-model-failover",
//         "options": { "fallbackModels": ["provider/model"], "autoRetry": false } } ] }
//
// Options (all optional):
//   fallbackModels  string[]  provider/model chain, tried in order (default: empty)
//   maxAttempts     number    fallbacks per session (default 2)
//   cooldownMs      number    skip a failed model this long, in ms (default 60000)
//   autoRetry       boolean   replay the last user message (default false)
//   notify          boolean   log each fallback to stderr (default true)
//   logFile         string    append log lines to this file
//
// No chain ships by default: set fallbackModels to your own provider/model
// pairs. autoRetry is off by default, because replaying a prompt is a
// state-changing action and is deliberately opt-in.

import { appendFileSync } from "node:fs";

// Ships empty on purpose — the chain is yours to define. Nothing about the
// author's provider setup is baked in.
const DEFAULT_CHAIN = [];

// Failure kinds worth failing over on. A bad request or a validation error will
// fail identically on every model, so retrying those just burns attempts.
const RETRYABLE =
  /no-route|provider\.|rate|quota|overload|unavailable|server|timeout|econn|socket|network|502|503|504|429/i;

function splitRef(ref) {
  const at = String(ref).indexOf("/");
  if (at <= 0) return null;
  return { providerID: ref.slice(0, at), modelID: ref.slice(at + 1) };
}

export default {
  id: "model-failover",
  async setup(ctx) {
    const opt = ctx.options || {};
    const chain =
      Array.isArray(opt.fallbackModels) && opt.fallbackModels.length > 0
        ? opt.fallbackModels.filter((m) => splitRef(m))
        : DEFAULT_CHAIN;
    const maxAttempts = Number.isFinite(opt.maxAttempts) ? opt.maxAttempts : 2;
    const cooldownMs = Number.isFinite(opt.cooldownMs) ? opt.cooldownMs : 60_000;
    const autoRetry = opt.autoRetry === true;
    const notify = opt.notify !== false;
    // Optional file sink — the only reliable way to observe a server-side
    // plugin. The env var fallback keeps it usable without touching config.
    const logFile = typeof opt.logFile === "string" ? opt.logFile : process.env.MODEL_FALLBACK_LOG || "";

    const attempts = new Map(); // sessionID -> failovers used
    const busy = new Set(); // sessionID -> failover in flight
    const cooldown = new Map(); // "provider/model" -> epoch ms it is usable again

    const log = (msg) => {
      if (!logFile && !notify) return;
      const line = `[model-failover] ${new Date().toISOString()} ${msg}`;
      if (logFile) {
        try {
          appendFileSync(logFile, `${line}\n`);
        } catch {
          /* best effort */
        }
      }
      if (notify) console.error(line);
    };

    log(`setup: chain=[${chain.join(", ")}] maxAttempts=${maxAttempts} autoRetry=${autoRetry}`);
    if (chain.length === 0) {
      log("no fallbackModels configured; set fallbackModels to enable failover");
    }

    async function availableRefs() {
      try {
        const listed = await ctx.model.list();
        const rows = listed && Array.isArray(listed.data) ? listed.data : [];
        return new Set(rows.map((m) => `${m.providerID}/${m.modelID}`));
      } catch {
        return new Set();
      }
    }

    function lastUserText(messages) {
      const rows = Array.isArray(messages) ? messages : [];
      for (let i = rows.length - 1; i >= 0; i--) {
        const row = rows[i] || {};
        const role = row.role || (row.info && row.info.role);
        if (role !== "user") continue;
        const parts = row.parts || (row.info && row.info.parts) || [];
        const text = parts
          .filter((p) => p && p.type === "text" && typeof p.text === "string")
          .map((p) => p.text)
          .join("\n")
          .trim();
        if (text) return text;
      }
      return "";
    }

    async function failover(sessionID, error) {
      const used = attempts.get(sessionID) || 0;
      if (used >= maxAttempts) {
        log(`max attempts reached for session ${sessionID} (${used}/${maxAttempts})`);
        return;
      }

      const set = await availableRefs();
      const now = Date.now();
      log(`availability=${set.size} models; looking for next in chain`);
      const next = chain.find((ref) => set.has(ref) && (cooldown.get(ref) || 0) <= now);
      if (!next) {
        log("no available model left in chain");
        return;
      }

      const ref = splitRef(next);
      if (!ref) return;

      attempts.set(sessionID, used + 1);
      try {
        await ctx.session.switchModel({
          sessionID,
          model: { providerID: ref.providerID, id: ref.modelID },
        });
        log(
          `session ${sessionID} moved to ${next} (attempt ${used + 1}/${maxAttempts}) after ` +
            `${(error && error.type) || "error"}`,
        );
      } catch (e) {
        cooldown.set(next, Date.now() + cooldownMs);
        log(`switchModel to ${next} failed: ${e && e.message}`);
        return;
      }

      if (!autoRetry) return;
      try {
        const transcript = await ctx.session.context({ sessionID });
        const text = lastUserText(transcript && (transcript.data || transcript));
        if (text) await ctx.session.prompt({ sessionID, text });
      } catch (e) {
        log(`autoRetry failed: ${e && e.message}`);
      }
    }

    const controller = new AbortController();
    void (async () => {
      try {
        for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
          if (!event || event.type !== "session.execution.failed") continue;
          const data = event.data || {};
          const sessionID = data.sessionID;
          const error = data.error || {};
          log(`failed event: session=${sessionID} error=${error.type} msg=${String(error.message).slice(0, 120)}`);
          if (!sessionID || busy.has(sessionID)) continue;

          const haystack = `${error.type || ""} ${error.message || ""}`;
          if (!RETRYABLE.test(haystack)) {
            log(`not retryable, ignoring: ${haystack.slice(0, 120)}`);
            continue;
          }

          busy.add(sessionID);
          try {
            await failover(sessionID, error);
          } finally {
            busy.delete(sessionID);
          }
        }
      } catch {
        /* stream aborted on unload */
      }
    })();

    return () => controller.abort();
  },
};
