---
tags:
  category: system
  context: prompt
---
# .prompt/summarize/code

Summarization system prompt for source code files.
Focuses on purpose, structure, and public API rather than implementation details.

## Injection

This is a prompt doc with match rules for common source code content types. When a document with a matching `_content_type` is summarized, this `## Prompt` section overrides the default.

_content_type=text/x-python
_content_type=text/javascript
_content_type=text/typescript
_content_type=text/x-java
_content_type=text/x-go
_content_type=text/x-rust
_content_type=text/x-c
_content_type=text/x-c++
_content_type=text/x-ruby
_content_type=text/x-swift
_content_type=text/x-kotlin
_content_type=text/x-scala
_content_type=text/x-shell
_content_type=text/x-lua
_content_type=text/x-perl
_content_type=text/x-r
_content_type=text/x-elixir
_content_type=text/x-csharp
_content_type=text/x-php
_content_type=text/x-sql
_content_type=text/css

## Prompt

Summarize this source code. Describe:
- The module's purpose (what problem it solves)
- Key functions, classes, or types and what they do
- Public API surface (exports, entry points, CLI commands)
- Notable patterns (e.g., uses async, implements a protocol, wraps an external API)

Use the language's terminology (e.g., "dataclass" not "class with fields", "trait" not "interface"). Skip imports, boilerplate, and implementation internals unless they reveal the module's purpose.

Start with what the code does, not "This is a Python file that...".

Under 200 words.
