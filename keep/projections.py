"""Internal projection planning helpers.

This module extracts the current token-budgeted find-context renderer into
an explicit plan model. The first pass keeps existing behavior intact so the
renderer can become a thin formatter over the planned sections.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .types import SYSTEM_TAG_PREFIX, Item


def _tok(text: str) -> int:
    return len(text) // 4


def _tag_display_value(value) -> str:
    if isinstance(value, list):
        return "[" + ", ".join(str(v) for v in value) + "]"
    return str(value)


def _base_id(id_value: str) -> str:
    return id_value.split("@")[0] if "@" in id_value else id_value


def _deep_key(deep_item: Item) -> str:
    return (
        deep_item.tags.get("_anchor_id")
        or f"{deep_item.id}|{deep_item.tags.get('_focus_version', '')}|"
           f"{deep_item.tags.get('_focus_part', '')}|"
           f"{deep_item.tags.get('_focus_summary', '')}"
    )


@dataclass
class FindContextSectionPlan:
    """A rendered section candidate within one find-context block."""

    kind: str
    lines: list[str] = field(default_factory=list)
    phase: str = ""
    policy: str = ""
    requested_tokens: int = 0
    allocated_tokens: int = 0
    skipped_lines: int = 0


@dataclass
class FindContextBudgetDecision:
    """A trace record describing how the allocator handled one section."""

    kind: str
    phase: str
    policy: str
    requested_tokens: int
    allocated_tokens: int
    kept_lines: int
    skipped_lines: int
    accepted: bool


@dataclass
class FindContextBlockPlan:
    """The planned sections for one top-level result item."""

    item: Item
    sections: list[FindContextSectionPlan] = field(default_factory=list)


@dataclass
class FindContextRenderPlan:
    """The full plan consumed by the find-context formatter."""

    blocks: list[FindContextBlockPlan]
    token_budget: int
    tokens_remaining: int
    compact_mode: bool
    used_deep_groups: bool
    budget_trace: list[FindContextBudgetDecision] = field(default_factory=list)


class _FindContextBudgetAllocator:
    """Explicit allocator for the current token-budgeted find-context plan."""

    def __init__(self, total_tokens: int):
        self.total_tokens = total_tokens
        self.remaining_tokens = total_tokens
        self.trace: list[FindContextBudgetDecision] = []

    def force_section(
        self,
        *,
        kind: str,
        phase: str,
        lines: list[str],
        line_costs: list[int] | None = None,
        policy: str = "force",
    ) -> FindContextSectionPlan:
        costs = self._line_costs(lines, line_costs)
        requested_tokens = sum(costs)
        self.remaining_tokens -= requested_tokens
        section = FindContextSectionPlan(
            kind=kind,
            lines=list(lines),
            phase=phase,
            policy=policy,
            requested_tokens=requested_tokens,
            allocated_tokens=requested_tokens,
            skipped_lines=0,
        )
        self._record(
            kind=kind,
            phase=phase,
            policy=policy,
            requested_tokens=requested_tokens,
            allocated_tokens=requested_tokens,
            kept_lines=len(lines),
            skipped_lines=0,
            accepted=bool(lines),
        )
        return section

    def fit_line(
        self,
        *,
        kind: str,
        phase: str,
        line: str,
        policy: str = "fit-line",
    ) -> FindContextSectionPlan | None:
        return self.fit_section(
            kind=kind,
            phase=phase,
            header=None,
            lines=[line],
            require_body=True,
            policy=policy,
        )

    def fit_section(
        self,
        *,
        kind: str,
        phase: str,
        header: str | None,
        lines: list[str],
        require_body: bool,
        header_cost: int | None = None,
        policy: str = "fit-section",
    ) -> FindContextSectionPlan | None:
        if not lines and header is None:
            return None

        requested_tokens = 0
        if header is not None:
            requested_tokens += header_cost if header_cost is not None else _tok(header)
        requested_tokens += sum(self._line_costs(lines, None))

        if header is None:
            kept_lines = 0
            allocated_tokens = 0
            kept_body: list[str] = []
            for line in lines:
                cost = _tok(line)
                if self.remaining_tokens - cost < 0:
                    break
                kept_body.append(line)
                allocated_tokens += cost
                kept_lines += 1
            accepted = kept_lines > 0 if require_body else True
            if not accepted:
                self._record(
                    kind=kind,
                    phase=phase,
                    policy=policy,
                    requested_tokens=requested_tokens,
                    allocated_tokens=0,
                    kept_lines=0,
                    skipped_lines=len(lines),
                    accepted=False,
                )
                return None
            self.remaining_tokens -= allocated_tokens
            section = FindContextSectionPlan(
                kind=kind,
                lines=kept_body,
                phase=phase,
                policy=policy,
                requested_tokens=requested_tokens,
                allocated_tokens=allocated_tokens,
                skipped_lines=len(lines) - kept_lines,
            )
            self._record(
                kind=kind,
                phase=phase,
                policy=policy,
                requested_tokens=requested_tokens,
                allocated_tokens=allocated_tokens,
                kept_lines=kept_lines,
                skipped_lines=len(lines) - kept_lines,
                accepted=True,
            )
            return section

        actual_header_cost = header_cost if header_cost is not None else _tok(header)
        if self.remaining_tokens - actual_header_cost < 0:
            self._record(
                kind=kind,
                phase=phase,
                policy=policy,
                requested_tokens=requested_tokens,
                allocated_tokens=0,
                kept_lines=0,
                skipped_lines=len(lines) + 1,
                accepted=False,
            )
            return None

        budget_after_header = self.remaining_tokens - actual_header_cost
        kept_body: list[str] = []
        body_cost = 0
        for line in lines:
            cost = _tok(line)
            if budget_after_header - cost < 0:
                break
            budget_after_header -= cost
            body_cost += cost
            kept_body.append(line)

        if require_body and not kept_body:
            self._record(
                kind=kind,
                phase=phase,
                policy=policy,
                requested_tokens=requested_tokens,
                allocated_tokens=0,
                kept_lines=0,
                skipped_lines=len(lines) + 1,
                accepted=False,
            )
            return None

        allocated_tokens = actual_header_cost + body_cost
        self.remaining_tokens -= allocated_tokens
        section_lines = [header, *kept_body]
        section = FindContextSectionPlan(
            kind=kind,
            lines=section_lines,
            phase=phase,
            policy=policy,
            requested_tokens=requested_tokens,
            allocated_tokens=allocated_tokens,
            skipped_lines=len(lines) - len(kept_body),
        )
        self._record(
            kind=kind,
            phase=phase,
            policy=policy,
            requested_tokens=requested_tokens,
            allocated_tokens=allocated_tokens,
            kept_lines=len(section_lines),
            skipped_lines=len(lines) - len(kept_body),
            accepted=True,
        )
        return section

    def _line_costs(self, lines: list[str], line_costs: list[int] | None) -> list[int]:
        if line_costs is not None:
            return list(line_costs)
        return [_tok(line) for line in lines]

    def _record(
        self,
        *,
        kind: str,
        phase: str,
        policy: str,
        requested_tokens: int,
        allocated_tokens: int,
        kept_lines: int,
        skipped_lines: int,
        accepted: bool,
    ) -> None:
        self.trace.append(FindContextBudgetDecision(
            kind=kind,
            phase=phase,
            policy=policy,
            requested_tokens=requested_tokens,
            allocated_tokens=allocated_tokens,
            kept_lines=kept_lines,
            skipped_lines=skipped_lines,
            accepted=accepted,
        ))


def render_find_context_plan(plan: FindContextRenderPlan) -> str:
    return "\n".join(
        "\n".join(line for section in block.sections for line in section.lines)
        for block in plan.blocks
    )


def plan_find_context_render(
    items: list[Item],
    keeper=None,
    token_budget: int = 4000,
    show_tags: bool = False,
    deep_primary_cap: int | None = None,
) -> FindContextRenderPlan:
    """Build the current token-budgeted find-context render plan."""
    _MIN_ITEMS_FOR_DETAIL = 2

    if not items or token_budget <= 0:
        return FindContextRenderPlan(
            blocks=[],
            token_budget=token_budget,
            tokens_remaining=max(token_budget, 0),
            compact_mode=False,
            used_deep_groups=False,
            budget_trace=[],
        )

    deep_groups = getattr(items, "deep_groups", {})
    compact_mode = bool(deep_groups) and token_budget <= 300
    candidate_items = list(items)
    budget = _FindContextBudgetAllocator(token_budget)

    if deep_primary_cap is not None and deep_groups and len(candidate_items) > deep_primary_cap:
        def _has_deep_group(it: Item) -> bool:
            bid = _base_id(it.id)
            return bid in deep_groups or it.id in deep_groups

        candidate_items.sort(key=lambda it: (
            not bool(it.tags.get("_entity")),
            not _has_deep_group(it),
        ))
        candidate_items = candidate_items[:deep_primary_cap]

    blocks: list[FindContextBlockPlan] = []

    for item in candidate_items:
        if budget.remaining_tokens <= 0:
            break

        focus = item.tags.get("_focus_summary")
        display_summary = focus if focus else item.summary
        line = f"- {item.id}"
        if item.score is not None:
            line += f" ({item.score:.2f})"
        date = (item.tags.get("_created") or item.tags.get("_updated", ""))[:10]
        if date:
            line += f"  [{date}]"
        line += f"  {display_summary}"

        block = FindContextBlockPlan(item=item)
        block.sections.append(budget.force_section(
            kind="summary",
            phase="summary",
            lines=[line],
        ))
        blocks.append(block)

    if deep_groups and budget.remaining_tokens > 0:
        def _append_line(block: FindContextBlockPlan, kind: str, line: str) -> bool:
            section = budget.fit_line(kind=kind, phase="deep", line=line)
            if section is None:
                return False
            block.sections.append(section)
            return True

        def _append_section(
            block: FindContextBlockPlan,
            kind: str,
            header: str,
            lines: list[str],
        ) -> bool:
            section = budget.fit_section(
                kind=kind,
                phase="deep",
                header=header,
                lines=lines,
                require_body=True,
            )
            if section is None:
                return False
            block.sections.append(section)
            return True

        def _thread_radius() -> int:
            budget_hint = min(token_budget, budget.remaining_tokens)
            if budget_hint <= 450:
                return 0
            if budget_hint <= 900:
                return 1
            return 2

        def _add_deep_window(
            parent_id: str,
            deep_items: list[Item],
            block: FindContextBlockPlan,
        ) -> None:
            if compact_mode:
                return
            if keeper is None or budget.remaining_tokens <= 0:
                return

            focus_versions = sorted({
                int(v)
                for v in (di.tags.get("_focus_version") for di in deep_items)
                if v and str(v).isdigit()
            })
            focus_parts = sorted({
                int(p)
                for p in (di.tags.get("_focus_part") for di in deep_items)
                if p and str(p).isdigit()
            })

            if focus_versions:
                radius = _thread_radius()
                around_map = {}
                for fv in focus_versions:
                    try:
                        around = keeper.list_versions_around(
                            parent_id, int(fv), radius=radius,
                        )
                    except Exception:
                        around = []
                    for version in around:
                        around_map[version.version] = version
                around = [around_map[key] for key in sorted(around_map)]

                if around and radius == 0 and len(around) == 1 and len(focus_versions) == 1:
                    version0 = around[0]
                    focus_v = str(focus_versions[0])
                    anchor_item = next(
                        (di for di in deep_items if di.tags.get("_focus_version") == focus_v),
                        None,
                    )
                    deep_summary = (
                        anchor_item.tags.get("_focus_summary", anchor_item.summary)
                        if anchor_item else ""
                    )
                    if (
                        str(version0.version) == focus_v
                        and (version0.summary or "").strip() == (deep_summary or "").strip()
                    ):
                        around = []

                thread_lines = []
                for version in around:
                    prefix = "*" if int(version.version) in focus_versions else "-"
                    thread_lines.append(f"      {prefix} @V{{{version.version}}} {version.summary}")
                _append_section(block, "thread", "      Thread:", thread_lines)

            if budget.remaining_tokens <= 0:
                return
            try:
                parts = keeper.list_parts(parent_id)
            except Exception:
                parts = []
            if not parts:
                return

            selected = []
            if focus_parts:
                part_map = {part.part_num: part for part in parts}
                if 0 in part_map:
                    selected.append(part_map[0])
                for fp in focus_parts:
                    for pn in (fp - 1, fp, fp + 1):
                        if pn in part_map and part_map[pn] not in selected:
                            selected.append(part_map[pn])
            else:
                part0 = next((part for part in parts if part.part_num == 0), None)
                if part0:
                    selected.append(part0)

            story_lines = [f"      - @P{{{part.part_num}}} {part.summary}" for part in selected]
            _append_section(block, "story", "      Story:", story_lines)

        rendered_map: dict[str, FindContextBlockPlan] = {}
        block_order: list[int] = []
        for block in blocks:
            parent_id = _base_id(block.item.id)
            rendered_map[parent_id] = block
            rendered_map[block.item.id] = block
            bid = id(block)
            if bid not in block_order:
                block_order.append(bid)

        bundles: dict[tuple[int, str], dict[str, Any]] = {}
        for group_key, group in deep_groups.items():
            block = rendered_map.get(group_key)
            if not block:
                continue
            bid = id(block)
            for deep_item in group:
                parent_id = _base_id(deep_item.id)
                bkey = (bid, parent_id)
                bucket = bundles.setdefault(
                    bkey,
                    {"block": block, "parent_id": parent_id, "items": []},
                )
                bucket["items"].append(deep_item)

        for bucket in bundles.values():
            items_for_parent = bucket["items"]
            deduped: list[Item] = []
            seen_local: set[str] = set()
            for deep_item in sorted(items_for_parent, key=lambda di: di.score or 0, reverse=True):
                dkey = _deep_key(deep_item)
                if dkey in seen_local:
                    continue
                seen_local.add(dkey)
                deduped.append(deep_item)
            bucket["items"] = deduped
            bucket["score"] = max((di.score or 0.0) for di in deduped) if deduped else 0.0

        bundles_by_block: dict[int, list[dict[str, Any]]] = {}
        for (bid, _parent_id), bucket in bundles.items():
            bundles_by_block.setdefault(bid, []).append(bucket)
        for group_list in bundles_by_block.values():
            group_list.sort(key=lambda b: float(b.get("score", 0.0)), reverse=True)

        ordered_bundles: list[dict[str, Any]] = []
        used_bundle_keys: set[tuple[int, str]] = set()

        for bid in block_order:
            group_list = bundles_by_block.get(bid, [])
            if not group_list:
                continue
            first = group_list[0]
            bkey = (bid, str(first["parent_id"]))
            if bkey not in used_bundle_keys:
                used_bundle_keys.add(bkey)
                ordered_bundles.append(first)

        tail: list[dict[str, Any]] = []
        for bid, group_list in bundles_by_block.items():
            for bucket in group_list:
                bkey = (bid, str(bucket["parent_id"]))
                if bkey in used_bundle_keys:
                    continue
                tail.append(bucket)
        tail.sort(key=lambda b: float(b.get("score", 0.0)), reverse=True)
        ordered_bundles.extend(tail)

        max_anchors_per_bundle = 1 if token_budget <= 900 else 2
        seen_deep: set[str] = set()
        for bucket in ordered_bundles:
            if budget.remaining_tokens <= 0:
                break
            block = bucket["block"]
            parent_id = str(bucket["parent_id"])
            deep_items = bucket["items"]

            emitted: list[Item] = []
            for deep_item in deep_items[:max_anchors_per_bundle]:
                dkey = _deep_key(deep_item)
                if dkey in seen_deep:
                    continue
                ddate = (deep_item.tags.get("_created") or deep_item.tags.get("_updated", ""))[:10]
                ddate_part = f"  [{ddate}]" if ddate else ""
                deep_summary = deep_item.tags.get("_focus_summary", deep_item.summary)
                line = f"    - {deep_item.id}{ddate_part}  {deep_summary}"
                if not _append_line(block, "deep-anchor", line):
                    break
                seen_deep.add(dkey)
                emitted.append(deep_item)

            if emitted:
                _add_deep_window(parent_id, emitted, block)

    if len(blocks) >= _MIN_ITEMS_FOR_DETAIL and keeper and budget.remaining_tokens > 0 and not compact_mode:
        for block in blocks:
            if budget.remaining_tokens <= 0:
                break

            item = block.item

            if show_tags and budget.remaining_tokens > 0:
                user_tags = {
                    k: v for k, v in item.tags.items()
                    if not k.startswith(SYSTEM_TAG_PREFIX)
                }
                if user_tags:
                    pairs = ", ".join(
                        f"{k}: {_tag_display_value(v)}"
                        for k, v in sorted(user_tags.items())
                    )
                    line = f"  {{{pairs}}}"
                    block.sections.append(budget.force_section(
                        kind="tags",
                        phase="detail",
                        lines=[line],
                    ))

            if budget.remaining_tokens > 30:
                focus_part = item.tags.get("_focus_part")
                parts = keeper.list_parts(item.id)
                other_parts = [
                    part for part in parts
                    if not focus_part or str(part.part_num) != str(focus_part)
                ]
                if other_parts:
                    section = budget.fit_section(
                        kind="parts",
                        phase="detail",
                        header="  Key topics:",
                        lines=[f"  - {part.summary}" for part in other_parts],
                        require_body=False,
                        header_cost=4,
                        policy="fit-section-legacy-header",
                    )
                    if section is not None:
                        block.sections.append(section)

            if budget.remaining_tokens > 30:
                focus_version = item.tags.get("_focus_version")
                if focus_version and focus_version.isdigit():
                    versions = keeper.list_versions_around(
                        item.id, int(focus_version), radius=2,
                    )
                else:
                    versions = keeper.list_versions(item.id, limit=5)
                    versions = list(reversed(versions))
                if versions:
                    version_lines = []
                    for version in versions:
                        stamp = (
                            version.tags.get("_created")
                            or version.tags.get("_updated", "")
                        )[:10]
                        stamp_part = f"  [{stamp}]" if stamp else ""
                        version_lines.append(
                            f"  - @V{{{version.version}}}{stamp_part}  {version.summary}"
                        )
                    section = budget.fit_section(
                        kind="versions",
                        phase="detail",
                        header="  Context:",
                        lines=version_lines,
                        require_body=False,
                        header_cost=4,
                        policy="fit-section-legacy-header",
                    )
                    if section is not None:
                        block.sections.append(section)

    return FindContextRenderPlan(
        blocks=blocks,
        token_budget=token_budget,
        tokens_remaining=budget.remaining_tokens,
        compact_mode=compact_mode,
        used_deep_groups=bool(deep_groups),
        budget_trace=budget.trace,
    )
