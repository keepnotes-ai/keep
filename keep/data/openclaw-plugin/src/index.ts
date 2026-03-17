/**
 * keep — OpenClaw plugin
 *
 * Context engine for OpenClaw. Requires plugins.slots.contextEngine: 'keep'.
 * Note remaining hook: session_end (no CE onSessionEnd lifecycle).
 */

import { execFileSync } from "child_process";
import path from "node:path";
import fs from "node:fs";
import { KeepMcpTransport } from "./mcp-transport.js";

// ---------------------------------------------------------------------------
// Startup check
// ---------------------------------------------------------------------------

function keepAvailable(): boolean {
  try {
    execFileSync("keep", ["config"], { timeout: 3000, stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// Message content helpers
// ---------------------------------------------------------------------------

/** Extract text from an AgentMessage content field (string or content blocks). */
function extractText(content: unknown): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .filter((b: any) => b.type === "text" && typeof b.text === "string")
      .map((b: any) => b.text)
      .join("\n");
  }
  if (content && typeof content === "object") {
    return JSON.stringify(content);
  }
  return "";
}

/** Truncate text to a char limit, appending ellipsis if truncated. */
function truncate(text: string, limit: number): string {
  if (text.length <= limit) return text;
  return text.slice(0, limit) + "…";
}

/** Rough token estimate: ~4 chars per token. */
function estimateTokens(text: string): number {
  return Math.ceil(text.length / 4);
}

// Roles worth ingesting into keep's now trace.
const INGEST_ROLES = new Set(["user", "assistant"]);

// ---------------------------------------------------------------------------
// Plugin registration
// ---------------------------------------------------------------------------

export default function register(api: any) {
  if (!keepAvailable()) {
    api.logger?.warn("[keep] keep CLI not found, plugin inactive");
    return;
  }

  // Read keep's max_inline_length once at startup (the store's definition
  // of "inline-sized content"). Used for assistant message truncation.
  let maxInlineLength = 2000;
  try {
    const val = execFileSync("keep", ["config", "max_inline_length"], {
      encoding: "utf-8",
      timeout: 3000,
      stdio: ["pipe", "pipe", "pipe"],
    }).trim();
    const parsed = parseInt(val, 10);
    if (parsed > 0) maxInlineLength = parsed;
  } catch {
    // keep default is 2000; use that
  }

  // -----------------------------------------------------------------------
  // First-run health check
  // -----------------------------------------------------------------------

  // Verify keep has a working provider configured.
  // This runs once at register time (not every turn).
  let keepHealthy = true;
  try {
    const providers = execFileSync("keep", ["config", "providers"], {
      encoding: "utf-8",
      timeout: 5000,
      stdio: ["pipe", "pipe", "pipe"],
    }).trim();
    if (!providers || providers === "{}") {
      keepHealthy = false;
      api.logger?.warn(
        "[keep] No providers configured. Run `openclaw keep setup` " +
          "or `keep config --setup` to choose embedding and summarization providers.",
      );
    }
  } catch {
    // keep config failed — keep is installed but misconfigured
    keepHealthy = false;
    api.logger?.warn(
      "[keep] Provider check failed. Run `keep config --setup` to configure.",
    );
  }

  // -----------------------------------------------------------------------
  // CLI: `openclaw keep setup`
  // -----------------------------------------------------------------------

  api.registerCli((ctx: any) => {
    const keepCmd = ctx.program
      .command("keep")
      .description("Keep reflective memory management");

    keepCmd
      .command("setup")
      .description("Run keep's interactive setup wizard (providers, models)")
      .action(async () => {
        const { execFileSync: execSync } = await import("child_process");
        try {
          execSync("keep", ["config", "--setup"], {
            stdio: "inherit",
            timeout: 120_000,
          });
        } catch (err: any) {
          if (err.status) {
            process.exit(err.status);
          }
          throw err;
        }
      });

    keepCmd
      .command("doctor")
      .description("Check keep store health and diagnose issues")
      .action(async () => {
        const { execFileSync: execSync } = await import("child_process");
        try {
          execSync("keep", ["doctor"], {
            stdio: "inherit",
            timeout: 30_000,
          });
        } catch (err: any) {
          if (err.status) {
            process.exit(err.status);
          }
          throw err;
        }
      });
  }, { commands: ["keep"] });

  // Shared MCP transport — initialized on gateway_start
  const mcp = new KeepMcpTransport({
    logger: {
      debug: (m: string) => api.logger?.debug?.(m),
      info: (m: string) => api.logger?.info(m),
      warn: (m: string) => api.logger?.warn(m),
      error: (m: string) => api.logger?.error(m),
    },
  });

  // Track first assemble per session for session-start context.
  const sessionFirstAssemble = new Set<string>();

  // Track workspace indexing per session.
  const sessionWorkspaceIndexed = new Set<string>();

  // ---------------------------------------------------------------------------
  // Config helpers
  // ---------------------------------------------------------------------------

  function getConfig(): {
    contextBudgetRatio: number;
    captureHeartbeats: boolean;
    indexPaths: string[];
    indexExclude: string[];
  } {
    const cfg = api.pluginConfig ?? {};
    return {
      contextBudgetRatio: typeof cfg.contextBudgetRatio === "number"
        ? Math.max(0, Math.min(1, cfg.contextBudgetRatio))
        : 0.3,
      captureHeartbeats: cfg.captureHeartbeats === true,
      indexPaths: Array.isArray(cfg.indexPaths) ? cfg.indexPaths : ["./"],
      indexExclude: Array.isArray(cfg.indexExclude) ? cfg.indexExclude : [],
    };
  }

  // ---------------------------------------------------------------------------
  // Workspace directory walking + exclude matching
  // ---------------------------------------------------------------------------

  /** Check if a relative path matches any exclude pattern. */
  function matchesExclude(relPath: string, patterns: string[]): boolean {
    for (const pattern of patterns) {
      // Directory pattern: ends with /
      if (pattern.endsWith("/")) {
        const dirPattern = pattern.slice(0, -1);
        if (relPath === dirPattern || relPath.startsWith(dirPattern + "/")) {
          return true;
        }
        continue;
      }

      // Glob patterns
      if (pattern.includes("*")) {
        const regex = new RegExp(
          "^" + pattern
            .replace(/\*\*/g, ".*")  // ** matches any path
            .replace(/\*/g, "[^/]*") // * matches any non-slash
            .replace(/\./g, "\\.")   // escape dots
          + "$"
        );
        if (regex.test(relPath)) return true;
        continue;
      }

      // Literal match
      if (relPath === pattern) return true;
    }
    return false;
  }

  /** Recursively walk a directory, yielding relative paths. */
  function* walkDir(dirPath: string, baseDir: string, excludes: string[]): Generator<string> {
    try {
      const entries = fs.readdirSync(dirPath, { withFileTypes: true });
      
      for (const entry of entries) {
        // Skip hidden files and directories
        if (entry.name.startsWith(".")) continue;

        const fullPath = path.join(dirPath, entry.name);
        const relPath = path.relative(baseDir, fullPath);

        // Skip if matches exclude pattern
        if (matchesExclude(relPath, excludes)) continue;

        if (entry.isDirectory()) {
          // Skip symlinks
          if (entry.isSymbolicLink()) continue;
          
          // Recursively walk subdirectory
          yield* walkDir(fullPath, baseDir, excludes);
        } else if (entry.isFile()) {
          yield relPath;
        }
      }
    } catch (err: any) {
      api.logger?.debug(`[keep] walkDir error for ${dirPath}: ${err.message}`);
    }
  }

  /** Index workspace paths via MCP. */
  async function indexWorkspacePaths(workspaceDir: string, label: string): Promise<void> {
    if (!mcp.connected) return;

    try {
      const config = getConfig();
      const startTime = Date.now();
      let indexed = 0;

      for (const indexPath of config.indexPaths) {
        const fullPath = path.resolve(workspaceDir, indexPath);
        
        if (!fs.existsSync(fullPath)) {
          api.logger?.debug(`[keep] ${label}: path ${indexPath} does not exist, skipping`);
          continue;
        }

        const stat = fs.statSync(fullPath);
        
        if (stat.isFile()) {
          // Single file
          const relPath = path.relative(workspaceDir, fullPath);
          if (!matchesExclude(relPath, config.indexExclude)) {
            await mcp.flow({
              state: "put",
              params: { uri: `file://${fullPath}` },
            });
            indexed++;
          }
        } else if (stat.isDirectory()) {
          // Directory: walk and index each file
          for (const relPath of walkDir(fullPath, workspaceDir, config.indexExclude)) {
            const filePath = path.join(workspaceDir, relPath);
            await mcp.flow({
              state: "put",
              params: { uri: `file://${filePath}` },
            });
            indexed++;
          }
        }
      }

      const elapsed = Date.now() - startTime;
      api.logger?.info(`[keep] ${label}: indexed ${indexed} files in ${elapsed}ms`);
    } catch (err: any) {
      api.logger?.warn(`[keep] ${label} indexing error: ${err.message}`);
    }
  }

  // -----------------------------------------------------------------------
  // Gateway lifecycle: start/stop MCP transport
  // -----------------------------------------------------------------------

  api.on("gateway_start", async () => {
    try {
      await mcp.connect();
      api.logger?.info("[keep] MCP transport connected");
    } catch (err: any) {
      api.logger?.warn(
        `[keep] MCP connect failed, falling back to CLI: ${err.message}`,
      );
    }
  });

  api.on("gateway_stop", async () => {
    await mcp.disconnect();
  });

  // -----------------------------------------------------------------------
  // Context Engine registration
  // -----------------------------------------------------------------------

  api.registerContextEngine("keep", () => {
    api.logger?.info("[keep] Context engine activated");

    return {
      info: {
        id: "keep",
        name: "keep reflective memory",
        version: "0.101.0",
        ownsCompaction: false, // v0.99: don't own compaction yet
      },

      // -------------------------------------------------------------------
      // bootstrap: initialize keep for this session
      // Practice moment: REFLECT BEFORE
      // -------------------------------------------------------------------
      async bootstrap(params: {
        sessionId: string;
        sessionKey?: string;
        sessionFile: string;
      }) {
        if (!mcp.connected) {
          return { bootstrapped: false, reason: "MCP not connected" };
        }

        try {
          // Index session file if resuming an existing session
          if (params.sessionFile && fs.existsSync(params.sessionFile)) {
            await mcp.flow({
              state: "put",
              params: {
                uri: `file://${params.sessionFile}`,
                tags: { session: params.sessionId },
              },
            });
          }

          // Mark session for first-assemble enrichment
          sessionFirstAssemble.add(params.sessionId);

          return { bootstrapped: true };
        } catch (err: any) {
          api.logger?.error(`[keep] bootstrap error: ${err.message}`);
          return { bootstrapped: false, reason: err.message };
        }
      },

      // -------------------------------------------------------------------
      // ingest: capture messages into keep's now trace
      // Practice moment: CAPTURE (continuous)
      // -------------------------------------------------------------------
      async ingest(params: {
        sessionId: string;
        sessionKey?: string;
        message: any;
        isHeartbeat?: boolean;
      }) {
        const cfg = getConfig();

        // Skip heartbeats unless configured
        if (params.isHeartbeat && !cfg.captureHeartbeats) {
          return { ingested: false };
        }

        if (!mcp.connected) {
          return { ingested: false };
        }

        const role: string = params.message.role || "unknown";

        // Only ingest user and assistant messages.
        // Skip system prompts (huge, not useful in trace) and tool
        // results (noisy, often enormous — the assistant's summary
        // of tool output is captured when we ingest the assistant turn).
        if (!INGEST_ROLES.has(role)) {
          return { ingested: false };
        }

        try {
          const text = extractText(params.message.content);
          if (!text.trim()) {
            return { ingested: false };
          }

          // User messages: 500 chars (the prompt intent).
          // Assistant messages: keep's max_inline_length (the store's own
          // definition of inline-sized content, typically 2000).
          const limit = role === "user" ? 500 : maxInlineLength;
          const content = truncate(text, limit);

          await mcp.flow({
            state: "put",
            params: {
              content,
              id: "now",
              tags: {
                session: params.sessionId,
                role,
              },
            },
          });

          return { ingested: true };
        } catch (err: any) {
          api.logger?.warn(`[keep] ingest error: ${err.message}`);
          return { ingested: false };
        }
      },

      // -------------------------------------------------------------------
      // ingestBatch: capture a completed turn as a unit
      // -------------------------------------------------------------------
      async ingestBatch(params: {
        sessionId: string;
        sessionKey?: string;
        messages: any[];
        isHeartbeat?: boolean;
      }) {
        const cfg = getConfig();
        if (params.isHeartbeat && !cfg.captureHeartbeats) {
          return { ingestedCount: 0 };
        }
        if (!mcp.connected) {
          return { ingestedCount: 0 };
        }

        let count = 0;
        for (const msg of params.messages) {
          const role: string = msg.role || "unknown";
          if (!INGEST_ROLES.has(role)) continue;

          const text = extractText(msg.content);
          if (!text.trim()) continue;

          const limit = role === "user" ? 500 : maxInlineLength;
          try {
            await mcp.flow({
              state: "put",
              params: {
                content: truncate(text, limit),
                id: "now",
                tags: { session: params.sessionId, role },
              },
            });
            count++;
          } catch (err: any) {
            api.logger?.warn(`[keep] ingestBatch item error: ${err.message}`);
          }
        }

        return { ingestedCount: count };
      },

      // -------------------------------------------------------------------
      // assemble: build model context with keep's memory
      // Practice moment: SURFACE WHAT MATTERS
      // -------------------------------------------------------------------
      async assemble(params: {
        sessionId: string;
        sessionKey?: string;
        messages: any[];
        tokenBudget?: number;
      }) {
        // Estimate conversation token cost for accurate totals
        const conversationTokens = params.messages.reduce((sum: number, m: any) => {
          return sum + estimateTokens(extractText(m.content));
        }, 0);

        if (!mcp.connected) {
          return {
            messages: params.messages,
            estimatedTokens: conversationTokens,
          };
        }

        try {
          const cfg = getConfig();

          // Extract last user message for semantic query
          const lastUser = [...params.messages]
            .reverse()
            .find((m: any) => m.role === "user");
          const prompt = lastUser
            ? truncate(extractText(lastUser.content), 500)
            : "";

          // Budget: fraction of total window for keep context
          const totalBudget = params.tokenBudget || 8000;
          const keepBudget = Math.floor(totalBudget * cfg.contextBudgetRatio);

          // First assemble in a session? Include richer context.
          const isFirstAssemble = sessionFirstAssemble.delete(params.sessionId);

          const result = await mcp.flow({
            state: "openclaw-assemble",
            params: {
              prompt: prompt || "session context",
              session_id: params.sessionId,
              ...(isFirstAssemble ? { first_turn: "true" } : {}),
            },
            token_budget: keepBudget,
          });

          // The MCP flow with token_budget returns rendered text.
          // result.text is the token-budgeted rendering, result.data
          // is the raw structured output.
          const contextText =
            result.text ||
            (result.data
              ? formatAssembleData(result.data)
              : "");

          const keepTokens = estimateTokens(contextText);

          return {
            messages: params.messages,
            estimatedTokens: conversationTokens + keepTokens,
            systemPromptAddition: contextText
              ? `\`keep context\`:\n${contextText}`
              : undefined,
          };
        } catch (err: any) {
          api.logger?.warn(`[keep] assemble error: ${err.message}`);
          return {
            messages: params.messages,
            estimatedTokens: conversationTokens,
          };
        }
      },

      // -------------------------------------------------------------------
      // afterTurn: post-turn reflection and background work
      // Practice moment: REFLECT DURING
      // -------------------------------------------------------------------
      async afterTurn(params: {
        sessionId: string;
        sessionKey?: string;
        sessionFile: string;
        messages: any[];
        prePromptMessageCount: number;
        autoCompactionSummary?: string;
        isHeartbeat?: boolean;
        tokenBudget?: number;
        runtimeContext?: Record<string, unknown>;
      }) {
        if (params.isHeartbeat) return;
        if (!mcp.connected) return;

        const workspaceDir = params.runtimeContext?.workspaceDir as string;

        // Index workspace on first turn
        if (workspaceDir && !sessionWorkspaceIndexed.has(params.sessionId)) {
          sessionWorkspaceIndexed.add(params.sessionId);
          // Fire and forget
          indexWorkspacePaths(workspaceDir, "workspace start").catch((err: any) => {
            api.logger?.warn(`[keep] afterTurn workspace indexing error: ${err.message}`);
          });
        }

        // Identify new messages from this turn
        const newMessages = params.messages.slice(params.prePromptMessageCount);
        if (newMessages.length === 0) return;

        // Detect inflection signals in the new messages
        const signals = detectInflection(newMessages);

        if (signals.shouldReflect) {
          api.logger?.debug(
            `[keep] Inflection detected (${signals.reason}), triggering reflection`,
          );
          // Fire and forget — don't block the turn
          mcp.prompt({ name: "reflect" }).catch((err: any) => {
            api.logger?.warn(`[keep] afterTurn reflect error: ${err.message}`);
          });

          // Also re-index workspace on inflection
          if (workspaceDir) {
            indexWorkspacePaths(workspaceDir, "inflection").catch((err: any) => {
              api.logger?.warn(`[keep] afterTurn inflection indexing error: ${err.message}`);
            });
          }
        }

        // If OpenClaw auto-compacted, index the summary as context
        if (params.autoCompactionSummary) {
          mcp
            .flow({
              state: "put",
              params: {
                content: truncate(params.autoCompactionSummary, 2000),
                tags: {
                  type: "compaction-summary",
                  session: params.sessionId,
                },
              },
            })
            .catch((err: any) => {
              api.logger?.warn(
                `[keep] afterTurn compaction index error: ${err.message}`,
              );
            });
        }
      },

      // -------------------------------------------------------------------
      // compact: advisory compaction (ownsCompaction=false for v0.99)
      // Practice moment: REFLECT AFTER
      // -------------------------------------------------------------------
      async compact(params: {
        sessionId: string;
        sessionKey?: string;
        sessionFile: string;
        tokenBudget?: number;
        force?: boolean;
        currentTokenCount?: number;
        compactionTarget?: "budget" | "threshold";
        customInstructions?: string;
        runtimeContext?: Record<string, unknown>;
      }) {
        if (!mcp.connected) {
          return { ok: true, compacted: false, reason: "MCP not connected" };
        }

        try {
          // Index the session transcript for keep's store
          if (params.sessionFile && fs.existsSync(params.sessionFile)) {
            await mcp.flow({
              state: "put",
              params: {
                uri: `file://${params.sessionFile}`,
                tags: { session: params.sessionId },
              },
            });
          }

          // Also re-index workspace during compaction
          const workspaceDir = params.runtimeContext?.workspaceDir as string;
          if (workspaceDir) {
            // Fire and forget
            indexWorkspacePaths(workspaceDir, "compaction refresh").catch((err: any) => {
              api.logger?.warn(`[keep] compact workspace indexing error: ${err.message}`);
            });
          }

          if (params.currentTokenCount) {
            api.logger?.debug(
              `[keep] compact advisory: ${params.currentTokenCount} tokens, ` +
                `target=${params.compactionTarget || "budget"}, ` +
                `budget=${params.tokenBudget}`,
            );
          }

          return { ok: true, compacted: false, reason: "advisory" };
        } catch (err: any) {
          api.logger?.warn(`[keep] compact error: ${err.message}`);
          return { ok: false, compacted: false, reason: err.message };
        }
      },

      async dispose() {
        sessionFirstAssemble.clear();
        sessionWorkspaceIndexed.clear();
        // Transport cleanup handled by gateway_stop hook
      },
    };
  });

  // -----------------------------------------------------------------------
  // Legacy hooks (session_end only)
  // -----------------------------------------------------------------------

  // No CE onSessionEnd lifecycle exists, so this must stay as a hook
  api.on(
    "session_end",
    async (_event: any, ctx: any) => {
      const key = ctx?.sessionId || ctx?.sessionKey;
      if (!key) return;

      if (mcp.connected) {
        try {
          await mcp.flow({
            state: "move",
            params: {
              name: `session-${key}`,
              tags: { session: key },
            },
          });
          api.logger?.info(`[keep] Session ${key} archived via MCP`);
          return;
        } catch (err: any) {
          api.logger?.error(`[keep] session_end MCP error: ${err.message}`);
        }
      } else {
        api.logger?.warn("[keep] session_end: MCP not connected, cannot archive");
      }
    },
    { priority: 10 },
  );

  api.logger?.info("[keep] Registered: context engine + session_end hook");
}

// ---------------------------------------------------------------------------
// Inflection detection
// ---------------------------------------------------------------------------

type InflectionSignal = {
  shouldReflect: boolean;
  reason: string;
};

/** Simple heuristic inflection detection on new turn messages. */
function detectInflection(messages: any[]): InflectionSignal {
  // Look at user messages for explicit signals
  for (const msg of messages) {
    if (msg.role !== "user") continue;
    const text = extractText(msg.content).toLowerCase();

    // Explicit transition markers
    if (
      /\b(let'?s move on|that'?s done|moving on|next topic|switching to|wrapping up|let'?s wrap)\b/.test(
        text,
      )
    ) {
      return { shouldReflect: true, reason: "explicit-transition" };
    }

    // Commitment language (strong signals of a decision made)
    if (
      /\b(i'?ll do|let'?s do|we should|go ahead|ship it|merge it|deploy|let'?s build)\b/.test(
        text,
      )
    ) {
      return { shouldReflect: true, reason: "commitment" };
    }
  }

  // Long assistant response (>2000 chars) suggests substantial work completed
  for (const msg of messages) {
    if (msg.role !== "assistant") continue;
    const text = extractText(msg.content);
    if (text.length > 2000) {
      return { shouldReflect: true, reason: "substantial-response" };
    }
  }

  return { shouldReflect: false, reason: "" };
}

// ---------------------------------------------------------------------------
// Format helpers
// ---------------------------------------------------------------------------

/** Format structured flow data into readable text for system prompt injection. */
function formatAssembleData(data: Record<string, unknown>): string {
  if (!data || typeof data !== "object") return "";

  const parts: string[] = [];

  // Intentions (now)
  if (data.intentions && typeof data.intentions === "object") {
    const intentions = data.intentions as any;
    if (intentions.summary) {
      parts.push(`## Current intentions\n${intentions.summary}`);
    }
  }

  // Similar items
  if (data.similar && typeof data.similar === "object") {
    const similar = data.similar as any;
    const results = similar.results || similar;
    if (Array.isArray(results) && results.length > 0) {
      const items = results
        .slice(0, 7)
        .map(
          (r: any) =>
            `- ${r.id} (${r.score?.toFixed(2) || "?"}) ${truncate(r.summary || "", 120)}`,
        )
        .join("\n");
      parts.push(`## Related\n${items}`);
    }
  }

  // Meta sections (learnings, todos)
  if (data.meta && typeof data.meta === "object") {
    const meta = data.meta as any;
    const sections = meta.sections || meta;
    if (typeof sections === "object") {
      for (const [key, items] of Object.entries(sections)) {
        if (!Array.isArray(items) || items.length === 0) continue;
        const formatted = (items as any[])
          .slice(0, 5)
          .map(
            (item: any) =>
              `- ${truncate(item.summary || item.id || "", 120)}`,
          )
          .join("\n");
        parts.push(`## ${key}\n${formatted}`);
      }
    }
  }

  // Edges
  if (data.edges && typeof data.edges === "object") {
    const edges = data.edges as any;
    const edgeMap = edges.edges || edges;
    if (typeof edgeMap === "object") {
      for (const [predicate, items] of Object.entries(edgeMap)) {
        if (!Array.isArray(items) || items.length === 0) continue;
        const formatted = (items as any[])
          .slice(0, 3)
          .map(
            (item: any) =>
              `- ${item.id || ""}: ${truncate(item.summary || "", 100)}`,
          )
          .join("\n");
        parts.push(`## ${predicate}\n${formatted}`);
      }
    }
  }

  return parts.join("\n\n");
}