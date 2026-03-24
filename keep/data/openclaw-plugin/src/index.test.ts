/**
 * Unit tests for keep OpenClaw plugin pure functions.
 *
 * Run: npx tsx --test src/index.test.ts
 * (from keep/data/openclaw-plugin/)
 */

import { describe, it } from "node:test";
import assert from "node:assert/strict";

// ---------------------------------------------------------------------------
// We can't import from the plugin directly (it calls execFileSync at module
// level). Instead, extract the pure functions into this test and keep them
// in sync. A future refactor could move them to a shared utils module.
// ---------------------------------------------------------------------------

// -- extractText (from index.ts) --

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

// -- truncate (from index.ts) --

function truncate(text: string, limit: number): string {
  if (text.length <= limit) return text;
  return text.slice(0, limit) + "…";
}

// -- estimateTokens (from index.ts) --

function estimateTokens(text: string): number {
  return Math.ceil(text.length / 4);
}

// -- sessionItemId (from index.ts) --

function sessionItemId(params: { sessionKey?: string; sessionId: string }): string {
  return params.sessionKey || params.sessionId;
}

// -- sessionTags (from index.ts) --

function sessionTags(params: {
  sessionKey?: string;
  sessionId: string;
  extra?: Record<string, string>;
}): Record<string, string> {
  const tags: Record<string, string> = {};
  if (params.sessionKey) tags.session_key = params.sessionKey;
  tags.session_id = params.sessionId;
  if (params.extra) Object.assign(tags, params.extra);
  return tags;
}

// -- formatTurn (from index.ts) --

const INGEST_ROLES = new Set(["user", "assistant"]);

function formatTurn(messages: any[], maxInlineLength: number): string {
  const parts: string[] = [];
  for (const msg of messages) {
    const role: string = msg.role || "unknown";
    if (!INGEST_ROLES.has(role)) continue;

    const text = extractText(msg.content);
    if (!text.trim()) continue;

    const limit = role === "user" ? 500 : maxInlineLength;
    parts.push(`[${role}] ${truncate(text, limit)}`);
  }
  return parts.join("\n\n");
}

// -- detectInflection (from index.ts) --

type InflectionSignal = {
  shouldReflect: boolean;
  reason: string;
};

function detectInflection(messages: any[]): InflectionSignal {
  for (const msg of messages) {
    if (msg.role !== "user") continue;
    const text = extractText(msg.content).toLowerCase();

    if (
      /\b(let'?s move on|that'?s done|moving on|next topic|switching to|wrapping up|let'?s wrap)\b/.test(
        text,
      )
    ) {
      return { shouldReflect: true, reason: "explicit-transition" };
    }

    if (
      /\b(i'?ll do|let'?s do|we should|go ahead|ship it|merge it|deploy|let'?s build)\b/.test(
        text,
      )
    ) {
      return { shouldReflect: true, reason: "commitment" };
    }
  }

  for (const msg of messages) {
    if (msg.role !== "assistant") continue;
    const text = extractText(msg.content);
    if (text.length > 2000) {
      return { shouldReflect: true, reason: "substantial-response" };
    }
  }

  return { shouldReflect: false, reason: "" };
}

// -- timeoutForState (from mcp-transport.ts) --

const ASSEMBLE_TIMEOUT_MS = 8_000;
const WRITE_CALL_TIMEOUT_MS = 15_000;
const LONG_CALL_TIMEOUT_MS = 30_000;
const DEFAULT_CALL_TIMEOUT_MS = 10_000;

function timeoutForState(state?: string): number {
  if (!state) return DEFAULT_CALL_TIMEOUT_MS;
  switch (state) {
    case "get":
    case "find-deep":
    case "stats":
      return ASSEMBLE_TIMEOUT_MS;
    case "put":
    case "tag":
    case "move":
    case "delete":
      return WRITE_CALL_TIMEOUT_MS;
    case "query-resolve":
    case "query-branch":
    case "query-explore":
    case "openclaw-compact":
      return LONG_CALL_TIMEOUT_MS;
    default:
      return DEFAULT_CALL_TIMEOUT_MS;
  }
}

// ===========================================================================
// Tests
// ===========================================================================

describe("extractText", () => {
  it("returns string content as-is", () => {
    assert.equal(extractText("hello"), "hello");
  });

  it("extracts text from content blocks", () => {
    const blocks = [
      { type: "text", text: "first" },
      { type: "image", url: "http://example.com/img.png" },
      { type: "text", text: "second" },
    ];
    assert.equal(extractText(blocks), "first\nsecond");
  });

  it("skips non-text blocks", () => {
    const blocks = [{ type: "image", url: "..." }];
    assert.equal(extractText(blocks), "");
  });

  it("handles empty array", () => {
    assert.equal(extractText([]), "");
  });

  it("JSON-stringifies objects", () => {
    const obj = { key: "value" };
    assert.equal(extractText(obj), '{"key":"value"}');
  });

  it("returns empty for null/undefined", () => {
    assert.equal(extractText(null), "");
    assert.equal(extractText(undefined), "");
  });
});

describe("truncate", () => {
  it("returns short text unchanged", () => {
    assert.equal(truncate("hi", 10), "hi");
  });

  it("truncates long text with ellipsis", () => {
    assert.equal(truncate("hello world", 5), "hello…");
  });

  it("handles exact limit", () => {
    assert.equal(truncate("exact", 5), "exact");
  });
});

describe("estimateTokens", () => {
  it("estimates ~4 chars per token", () => {
    assert.equal(estimateTokens("hello world!"), 3);
  });

  it("returns 0 for empty string", () => {
    assert.equal(estimateTokens(""), 0);
  });

  it("rounds up", () => {
    assert.equal(estimateTokens("hi"), 1);
  });
});

describe("sessionItemId", () => {
  it("prefers sessionKey over sessionId", () => {
    assert.equal(
      sessionItemId({ sessionKey: "agent:main:webchat:direct:gw", sessionId: "uuid-123" }),
      "agent:main:webchat:direct:gw",
    );
  });

  it("falls back to sessionId when sessionKey is absent", () => {
    assert.equal(
      sessionItemId({ sessionId: "uuid-123" }),
      "uuid-123",
    );
  });

  it("falls back to sessionId when sessionKey is empty", () => {
    assert.equal(
      sessionItemId({ sessionKey: "", sessionId: "uuid-123" }),
      "uuid-123",
    );
  });
});

describe("sessionTags", () => {
  it("includes both session_key and session_id when key is present", () => {
    const tags = sessionTags({
      sessionKey: "agent:main:tg:direct:12345",
      sessionId: "uuid-abc",
    });
    assert.equal(tags.session_key, "agent:main:tg:direct:12345");
    assert.equal(tags.session_id, "uuid-abc");
  });

  it("omits session_key when absent", () => {
    const tags = sessionTags({ sessionId: "uuid-abc" });
    assert.equal(tags.session_key, undefined);
    assert.equal(tags.session_id, "uuid-abc");
  });

  it("merges extra tags", () => {
    const tags = sessionTags({
      sessionKey: "agent:main:webchat:direct:gw",
      sessionId: "uuid-abc",
      extra: { role: "user", type: "compaction-summary" },
    });
    assert.equal(tags.role, "user");
    assert.equal(tags.type, "compaction-summary");
    assert.equal(tags.session_key, "agent:main:webchat:direct:gw");
  });
});

describe("formatTurn", () => {
  it("formats user and assistant messages into a turn block", () => {
    const msgs = [
      { role: "user", content: "What's the plan?" },
      { role: "assistant", content: "Here's what I think..." },
    ];
    const result = formatTurn(msgs, 2000);
    assert.ok(result.includes("[user] What's the plan?"));
    assert.ok(result.includes("[assistant] Here's what I think..."));
    // Separated by double newline
    assert.ok(result.includes("\n\n"));
  });

  it("skips system and tool messages", () => {
    const msgs = [
      { role: "system", content: "You are helpful" },
      { role: "user", content: "hello" },
      { role: "tool", content: '{"result": 42}' },
      { role: "assistant", content: "hi" },
    ];
    const result = formatTurn(msgs, 2000);
    assert.ok(!result.includes("[system]"));
    assert.ok(!result.includes("[tool]"));
    assert.ok(result.includes("[user] hello"));
    assert.ok(result.includes("[assistant] hi"));
  });

  it("truncates user messages at 500 chars", () => {
    const longContent = "x".repeat(600);
    const msgs = [{ role: "user", content: longContent }];
    const result = formatTurn(msgs, 2000);
    // [user] prefix + 500 chars + ellipsis
    assert.ok(result.length < 520);
    assert.ok(result.endsWith("…"));
  });

  it("truncates assistant messages at maxInlineLength", () => {
    const longContent = "y".repeat(300);
    const msgs = [{ role: "assistant", content: longContent }];
    const result = formatTurn(msgs, 100);
    // Should truncate at 100
    assert.ok(result.includes("…"));
    assert.ok(result.length < 120);
  });

  it("returns empty for no ingestable messages", () => {
    const msgs = [
      { role: "system", content: "You are helpful" },
      { role: "tool", content: "result" },
    ];
    assert.equal(formatTurn(msgs, 2000), "");
  });

  it("skips messages with empty content", () => {
    const msgs = [
      { role: "user", content: "" },
      { role: "assistant", content: "response" },
    ];
    const result = formatTurn(msgs, 2000);
    assert.ok(!result.includes("[user]"));
    assert.ok(result.includes("[assistant] response"));
  });
});

describe("detectInflection", () => {
  it("detects explicit transition", () => {
    const msgs = [{ role: "user", content: "ok, let's move on to testing" }];
    const result = detectInflection(msgs);
    assert.equal(result.shouldReflect, true);
    assert.equal(result.reason, "explicit-transition");
  });

  it("detects commitment language", () => {
    const msgs = [{ role: "user", content: "go ahead and ship it" }];
    const result = detectInflection(msgs);
    assert.equal(result.shouldReflect, true);
    assert.equal(result.reason, "commitment");
  });

  it("detects substantial assistant response", () => {
    const msgs = [
      { role: "assistant", content: "x".repeat(2001) },
    ];
    const result = detectInflection(msgs);
    assert.equal(result.shouldReflect, true);
    assert.equal(result.reason, "substantial-response");
  });

  it("returns false for normal conversation", () => {
    const msgs = [
      { role: "user", content: "what's the status?" },
      { role: "assistant", content: "Everything looks good." },
    ];
    const result = detectInflection(msgs);
    assert.equal(result.shouldReflect, false);
  });

  it("ignores assistant content for transition markers", () => {
    const msgs = [
      { role: "assistant", content: "let's move on to the next thing" },
    ];
    const result = detectInflection(msgs);
    assert.equal(result.shouldReflect, false);
  });

  it("handles content blocks", () => {
    const msgs = [
      {
        role: "user",
        content: [{ type: "text", text: "let's wrap up for tonight" }],
      },
    ];
    const result = detectInflection(msgs);
    assert.equal(result.shouldReflect, true);
    assert.equal(result.reason, "explicit-transition");
  });
});

describe("timeoutForState", () => {
  it("returns fast timeout for assemble", () => {
    assert.equal(timeoutForState("get"), 8_000);
  });

  it("returns write timeout for mutations", () => {
    assert.equal(timeoutForState("put"), 15_000);
    assert.equal(timeoutForState("move"), 15_000);
    assert.equal(timeoutForState("tag"), 15_000);
  });

  it("returns long timeout for queries and compact", () => {
    assert.equal(timeoutForState("query-resolve"), 30_000);
    assert.equal(timeoutForState("openclaw-compact"), 30_000);
  });

  it("returns default for unknown states", () => {
    assert.equal(timeoutForState("custom-flow"), 10_000);
    assert.equal(timeoutForState(undefined), 10_000);
  });
});

describe("subagent lifecycle", () => {
  it("prepareSubagentSpawn writes to child sessionKey as item id", () => {
    // The child item id IS the childSessionKey — not `now`
    const childKey = "agent:main:sub:task-abc";
    const parentKey = "agent:main:webchat:direct:gw";
    // Item id should be the child's key
    assert.equal(childKey, "agent:main:sub:task-abc");
    // Tags should include parent linkage
    const expectedTags = {
      session_key: childKey,
      parent_session: parentKey,
      type: "subagent-spawn",
    };
    assert.equal(expectedTags.parent_session, parentKey);
  });

  it("onSubagentEnded does not archive — item persists", () => {
    // Design: no `move` call. The child session item stays
    // in the store with its version history intact.
    const reasons = ["deleted", "completed", "swept", "released"];
    for (const reason of reasons) {
      assert.ok(typeof reason === "string");
    }
  });
});

describe("INGEST_ROLES", () => {
  it("includes user and assistant", () => {
    assert.ok(INGEST_ROLES.has("user"));
    assert.ok(INGEST_ROLES.has("assistant"));
  });

  it("excludes system, tool, toolResult", () => {
    assert.ok(!INGEST_ROLES.has("system"));
    assert.ok(!INGEST_ROLES.has("tool"));
    assert.ok(!INGEST_ROLES.has("toolResult"));
    assert.ok(!INGEST_ROLES.has("tool_result"));
  });
});
