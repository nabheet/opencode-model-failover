// OpenCode v2 plugin. Options and behaviour: see README.md.

import { appendFileSync } from "node:fs";

// Ships empty: the chain is yours to define.
const DEFAULT_CHAIN = [];

// Only retryable failures are worth failing over on; a validation error fails on every model.
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
    const logFile =
      typeof opt.logFile === "string" ? opt.logFile : process.env.MODEL_FALLBACK_LOG || "";

    const attempts = new Map(); // sessionID -> failovers used
    const busy = new Set(); // sessionID -> failover in flight
    const cooldown = new Map(); // "provider/model" -> epoch ms it is usable again

    const log = (msg) => {
      if (!logFile && !notify) return;
      const line = `[model-failover] ${new Date().toISOString()} ${msg}`;
      if (logFile) {
        try {
          appendFileSync(logFile, `${line}\n`);
        } catch {}
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
        const role = row.role || row.info?.role;
        if (role !== "user") continue;
        const parts = row.parts || row.info?.parts || [];
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
            `${error?.type || "error"}`,
        );
      } catch (e) {
        cooldown.set(next, Date.now() + cooldownMs);
        log(`switchModel to ${next} failed: ${e?.message}`);
        return;
      }

      if (!autoRetry) return;
      try {
        const transcript = await ctx.session.context({ sessionID });
        const text = lastUserText(transcript && (transcript.data || transcript));
        if (text) await ctx.session.prompt({ sessionID, text });
      } catch (e) {
        log(`autoRetry failed: ${e?.message}`);
      }
    }

    const controller = new AbortController();
    void (async () => {
      try {
        for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
          if (event?.type !== "session.execution.failed") continue;
          const data = event.data || {};
          const sessionID = data.sessionID;
          const error = data.error || {};
          log(
            `failed event: session=${sessionID} error=${error.type} msg=${String(error.message).slice(0, 120)}`,
          );
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
      } catch {}
    })();

    return () => controller.abort();
  },
};
