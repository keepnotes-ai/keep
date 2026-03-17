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

// -- formatAssembleData (from index.ts) --

function formatAssembleData(data: Record<string, unknown>): string {
  if (!data || typeof data !== "object") return "";

  const parts: string[] = [];

  if (data.intentions && typeof data.intentions === "object") {
    const intentions = data.intentions as any;
    if (intentions.summary) {
      parts.push(`## Current intentions\n${intentions.summary}`);
    }
  }

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

// -- timeoutForState (from mcp-transport.ts) --

const ASSEMBLE_TIMEOUT_MS = 8_000;
const WRITE_CALL_TIMEOUT_MS = 15_000;
const LONG_CALL_TIMEOUT_MS = 30_000;
const DEFAULT_CALL_TIMEOUT_MS = 10_000;

function timeoutForState(state?: string): number {
  if (!state) return DEFAULT_CALL_TIMEOUT_MS;
  switch (state) {
    case "openclaw-assemble":
    case "get-context":
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
    // assistant saying "let's move on" shouldn't trigger — only user
    // (but it IS >2000? no, it's short)
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

describe("formatAssembleData", () => {
  it("formats intentions", () => {
    const data = {
      intentions: { summary: "Working on context engine" },
    };
    const result = formatAssembleData(data);
    assert.ok(result.includes("## Current intentions"));
    assert.ok(result.includes("Working on context engine"));
  });

  it("formats similar items", () => {
    const data = {
      similar: {
        results: [
          { id: "doc-1", score: 0.92, summary: "Design document" },
          { id: "doc-2", score: 0.85, summary: "Implementation notes" },
        ],
      },
    };
    const result = formatAssembleData(data);
    assert.ok(result.includes("## Related"));
    assert.ok(result.includes("doc-1 (0.92)"));
    assert.ok(result.includes("doc-2 (0.85)"));
  });

  it("formats meta sections", () => {
    const data = {
      meta: {
        sections: {
          learnings: [{ summary: "Always validate before deploying" }],
          todo: [{ summary: "Fix the auth flow" }],
        },
      },
    };
    const result = formatAssembleData(data);
    assert.ok(result.includes("## learnings"));
    assert.ok(result.includes("## todo"));
    assert.ok(result.includes("Always validate"));
    assert.ok(result.includes("Fix the auth"));
  });

  it("formats edges", () => {
    const data = {
      edges: {
        edges: {
          references: [
            { id: "doc-a", summary: "Referenced doc" },
          ],
        },
      },
    };
    const result = formatAssembleData(data);
    assert.ok(result.includes("## references"));
    assert.ok(result.includes("doc-a"));
  });

  it("handles empty data", () => {
    assert.equal(formatAssembleData({}), "");
  });

  it("handles null", () => {
    assert.equal(formatAssembleData(null as any), "");
  });

  it("limits similar to 7 items", () => {
    const data = {
      similar: {
        results: Array.from({ length: 10 }, (_, i) => ({
          id: `doc-${i}`,
          score: 0.9 - i * 0.05,
          summary: `Item ${i}`,
        })),
      },
    };
    const result = formatAssembleData(data);
    assert.ok(result.includes("doc-6"));
    assert.ok(!result.includes("doc-7"));
  });
});

describe("timeoutForState", () => {
  it("returns fast timeout for assemble", () => {
    assert.equal(timeoutForState("openclaw-assemble"), 8_000);
    assert.equal(timeoutForState("get-context"), 8_000);
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

describe("INGEST_ROLES", () => {
  const INGEST_ROLES = new Set(["user", "assistant"]);

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
