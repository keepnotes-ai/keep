/**
 * keep — OpenClaw plugin
 *
 * Hooks:
 *   before_prompt_build → prompt-aware context injection
 *   after_compaction    → index workspace memory files into keep
 *   session_end         → archive session versions from now
 *
 * The after_compaction hook uses `keep put` with file-stat fast-path:
 * unchanged files (same mtime+size) are skipped without reading.
 * This makes it safe to run on every compaction even with many files.
 */

import { execFileSync } from "child_process";
import path from "node:path";
import fs from "node:fs";

function keepAvailable(): boolean {
  try {
    execFileSync("keep", ["config"], { timeout: 3000, stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

function runKeep(args: string[], input?: string): string | null {
  try {
    return execFileSync("keep", args, {
      encoding: "utf-8",
      timeout: 5000,
      input: input ?? "",
    }).trim();
  } catch {
    return null;
  }
}

function runKeepLong(args: string[], timeoutMs: number = 60000): string | null {
  try {
    return execFileSync("keep", args, {
      encoding: "utf-8",
      timeout: timeoutMs,
      stdio: ["pipe", "pipe", "pipe"],
    }).trim();
  } catch {
    return null;
  }
}

export default function register(api: any) {
  if (!keepAvailable()) {
    api.logger?.warn("[keep] keep CLI not found, plugin inactive");
    return;
  }

  // Before prompt build: update intentions with user prompt, injecting prompt-aware context.
  // The update itself returns context (similar items, meta), so one call does both.
  api.on(
    "before_prompt_build",
    async (event: any, ctx: any) => {
      const sid = ctx?.sessionId || ctx?.sessionKey;

      // Separate conversation metadata from user text.
      // OpenClaw prompts may start with "Conversation info..." fenced block.
      // We index only the user text (+ timestamp) but return both to the agent.
      let convInfo = "";
      let userText = event.prompt || "";
      const fenceEnd = userText.indexOf("```\n\n");
      if (fenceEnd !== -1 && userText.startsWith("Conversation info")) {
        convInfo = userText.slice(0, fenceEnd + 4); // includes closing fence
        userText = userText.slice(fenceEnd + 5);    // after fence + blank line
      }

      let now: string | null;
      const trimmed = userText.trim();
      if (trimmed) {
        const truncated = trimmed.slice(0, 500);
        // Update + get context in one call (set_now outputs context with similar/meta)
        const args = ["now", "-n", "10"];
        if (sid) args.push("-t", `session=${sid}`);
        now = runKeep(args, truncated);
      } else {
        now = runKeep(["now", "-n", "10"]);
      }
      if (!now) return;

      const prefix = convInfo ? `${convInfo}\n\n` : "";
      return {
        prependContext: `${prefix}\`keep now\`:\n${now}`,
      };
    },
    { priority: 10 },
  );

  // After compaction: index workspace memory files into keep.
  // Memory flush writes files right before compaction, so they're fresh here.
  // Uses `keep put` with file-stat fast-path (mtime+size check) — unchanged
  // files skip without even reading the file. Safe to run on every compaction.
  // Also indexes MEMORY.md if it exists at the workspace root.
  api.on(
    "after_compaction",
    async (_event: any, ctx: any) => {
      const workspaceDir = ctx?.workspaceDir;
      if (!workspaceDir) return;

      const memoryDir = path.join(workspaceDir, "memory");
      const memoryMd = path.join(workspaceDir, "MEMORY.md");
      let indexed = 0;

      // Index memory/ directory (all files, directory mode)
      if (fs.existsSync(memoryDir)) {
        api.logger?.debug("[keep] Indexing memory/ after compaction");
        if (runKeepLong(["put", `${memoryDir}/`], 30000)) indexed++;
      }

      // Index MEMORY.md (single file)
      if (fs.existsSync(memoryMd)) {
        api.logger?.debug("[keep] Indexing MEMORY.md after compaction");
        if (runKeepLong(["put", memoryMd], 10000)) indexed++;
      }

      if (indexed > 0) {
        api.logger?.info("[keep] Post-compaction memory sync complete");
      }
    },
    { priority: 20 },
  );

  // Session end: archive this session's versions out of now.
  // Moves versions tagged with this session ID to their own item,
  // keeping now clean for the next session.
  api.on(
    "session_end",
    async (event: any, ctx: any) => {
      const key = ctx?.sessionId || ctx?.sessionKey;
      if (!key) return;
      runKeepLong(["move", `session-${key}`, "-t", `session=${key}`]);
    },
    { priority: 10 },
  );

  api.logger?.info(
    "[keep] Registered hooks: before_prompt_build, after_compaction, session_end",
  );
}
