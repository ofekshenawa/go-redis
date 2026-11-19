"""Render a run-bound telemetry review comment; publishing and approval stay elsewhere."""

import json
import re
import unicodedata
from collections import defaultdict

COMMENT_PREFIX = "<!-- telemetry-contract:"
MAX_COMMENT_CHARS = 59_999
STATUSES = {
    "pending": "Waiting for telemetry approval.",
    "approved": "Telemetry approved for this run.",
    "reusing": "Matching prior approval found; final validation pending.",
    "reused": "Prior telemetry approval reused; the exact delta and policy are unchanged.",
    "rejected": "Telemetry review rejected for this run.",
    "stale": "This report is outdated; use the latest run before approving.",
    "cancelled": "This telemetry run was cancelled.",
    "failed": "This telemetry run failed; approval cannot waive a collection or validation failure.",
}


def _number(value, field):
    if type(value) is not int or not 0 < value < 2**63:
        raise ValueError(f"Invalid {field}.")
    return value


def _sha(value, size, field):
    if not isinstance(value, str) or not re.fullmatch(rf"[0-9a-fA-F]{{{size}}}", value):
        raise ValueError(f"Invalid {field}.")
    return value.lower()


def comment_marker(manifest):
    """Stable identity across status updates, scoped to source run, attempt and delta."""
    run = _number(manifest.get("run_id"), "collection run ID")
    attempt = _number(manifest.get("run_attempt", 1), "run attempt")
    digest = _sha(manifest.get("delta_sha256"), 64, "delta hash")
    return f"{COMMENT_PREFIX}run={run}:attempt={attempt}:delta={digest} -->"


def _source(value):
    """One literal code line, with no fence delimiters, mentions or invisible controls."""
    if not isinstance(value, str):
        raise ValueError("Telemetry names, types and units must be strings.")
    quoted = json.dumps(value, ensure_ascii=False)[1:-1]
    result = []
    for char in quoted:
        if char in "`@<>" or unicodedata.category(char).startswith("C") or char in "\u2028\u2029":
            point = ord(char)
            result.append(f"\\u{point:04x}" if point <= 0xFFFF else f"\\U{point:08x}")
        else:
            result.append(char)
    return "".join(result)


def _attribute(value):
    if not isinstance(value, dict):
        raise ValueError("Invalid attribute declaration.")
    name, kind = value.get("name"), value.get("type")
    if not isinstance(name, str) or not name or not isinstance(kind, str) or not kind:
        raise ValueError("Invalid attribute name or type.")
    return name, kind


def _metric(value):
    if not isinstance(value, dict):
        raise ValueError("Invalid metric.")
    for key in ("package", "name", "instrument", "unit"):
        if not isinstance(value.get(key), str) or (key != "unit" and not value[key]):
            raise ValueError(f"Invalid metric {key}.")
    if not isinstance(value.get("attributes"), list) or type(value.get("external_attributes", False)) is not bool:
        raise ValueError("Invalid metric attributes.")
    attributes = [_attribute(a) for a in value["attributes"]]
    if len(set(attributes)) != len(attributes):
        raise ValueError("Duplicate metric attributes.")
    return set(attributes)


def _changes(delta):
    if not isinstance(delta, dict) or delta.get("version") != 1 or type(delta.get("version")) is not int:
        raise ValueError("Unsupported telemetry delta.")
    changes = delta.get("changes")
    if not isinstance(changes, list) or type(delta.get("has_changes")) is not bool or delta["has_changes"] != bool(changes):
        raise ValueError("Invalid telemetry delta changes.")
    for change in changes:
        if not isinstance(change, dict) or change.get("entity") not in ("metric", "attribute_declaration"):
            raise ValueError("Invalid telemetry change entity.")
        if "before" not in change or "after" not in change:
            raise ValueError("Telemetry changes require explicit before and after values.")
        before, after = change.get("before"), change.get("after")
        expected = {"add": (False, True), "remove": (True, False), "modify": (True, True)}
        if change.get("action") not in expected or (before is not None, after is not None) != expected[change["action"]]:
            raise ValueError("Invalid telemetry before/after change.")
        validate = _metric if change["entity"] == "metric" else _attribute
        for value in (before, after):
            if value is not None:
                validate(value)
    return changes


def _field(lines, label, before, after):
    if before == after:
        lines.append(f"  {label}: {_source(before) or '(none)'}")
    else:
        if before is not None:
            lines.append(f"- {label}: {_source(before) or '(none)'}")
        if after is not None:
            lines.append(f"+ {label}: {_source(after) or '(none)'}")


def _metric_diff(change):
    before, after = change["before"], change["after"]
    lines = []
    for label, key in (("metric", "name"), ("instrument", "instrument"), ("unit", "unit")):
        _field(lines, label, before[key] if before else None, after[key] if after else None)
    if before and after and before["package"] != after["package"]:
        _field(lines, "package", before["package"], after["package"])
    if (before and before.get("external_attributes")) or (after and after.get("external_attributes")):
        _field(lines, "caller-provided attributes", ("yes" if before.get("external_attributes") else "no") if before else None,
               ("yes" if after.get("external_attributes") else "no") if after else None)
    old = _metric(before) if before else set()
    new = _metric(after) if after else set()
    removed, added = old - new, new - old
    for sign, values in (("-", removed), ("+", added)):
        for name, kind in sorted(values):
            lines.append(f"{sign} attribute/label: {_source(name)} ({_source(kind)})")
    return lines, removed, added


def _diff_block(lines):
    return "```diff\n" + "\n".join(lines) + "\n```\n\n"


def _change_order(change):
    def definition(value):
        if value is None:
            return ()
        return (value["package"], value["name"], value["instrument"], value["unit"],
                value.get("external_attributes", False), tuple(sorted(_metric(value))))
    return ({"remove": 0, "modify": 1, "add": 2}[change["action"]],
            definition(change["before"]), definition(change["after"]))


def _body(changes):
    groups = defaultdict(list)
    shown_removed, shown_added = set(), set()
    for change in changes:
        if change["entity"] == "metric":
            metric = change["after"] or change["before"]
            groups[metric["package"], metric["name"]].append(change)
    sections = []
    for (package, name), items in sorted(groups.items()):
        sections.append(f"### Metric: `{_source(name)}`\n\nPackage: `{_source(package)}`\n\n")
        # Canonical structured values avoid relying on incoming array order or
        # display identities when several variants of one metric have changed.
        for change in sorted(items, key=_change_order):
            lines, removed, added = _metric_diff(change)
            shown_removed.update(removed)
            shown_added.update(added)
            sections.append(_diff_block(lines))
    declarations = set()
    for change in changes:
        if change["entity"] == "attribute_declaration":
            for sign, value, shown in (("-", change["before"], shown_removed), ("+", change["after"], shown_added)):
                if value is not None and _attribute(value) not in shown:
                    declarations.add((sign, *_attribute(value)))
    if declarations:
        sections.append("### Other attribute declarations\n\nChanges not already shown with a metric above.\n\n")
        sections.append(_diff_block([f"{sign} attribute declaration: {_source(name)} ({_source(kind)})"
                                     for sign, name, kind in sorted(declarations, key=lambda row: (row[0] == "+", row[1], row[2]))]))
    return "".join(sections)


def _bounded(header, body, footer, source_url):
    if len(header) + len(body) + len(footer) <= MAX_COMMENT_CHARS:
        return header + body + footer
    notice = ("\n**Report truncated:** Some changes are omitted from this comment. "
              f"Open the [full report and artifacts]({source_url}) to review every change.\n\n")
    budget = MAX_COMMENT_CHARS - len(header) - len(footer) - len(notice) - len("```\n")
    kept, used, inside_fence = [], 0, False
    for line in body.splitlines(keepends=True):
        if used + len(line) > budget:
            break
        kept.append(line)
        used += len(line)
        if line.startswith("```"):
            inside_fence = not inside_fence
    if inside_fence:
        kept.append("```\n")
    return header + "".join(kept) + notice + footer


def render_comment(manifest, delta, status="pending"):
    """Render Markdown without IO; the publisher must verify hashes and live run/PR state."""
    if status not in STATUSES:
        raise ValueError("Unsupported telemetry comment status.")
    marker = comment_marker(manifest)
    repository = manifest.get("repository", "")
    if not isinstance(repository, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,38}/[A-Za-z0-9_.-]{1,100}", repository):
        raise ValueError("Invalid repository.")
    head = _sha(manifest.get("head_sha"), 40, "source head")
    source_run = manifest["run_id"]
    approval_run = _number(manifest.get("approval_run_id", source_run), "approval run ID")
    source_url = f"https://github.com/{repository}/actions/runs/{source_run}"
    approval_url = f"https://github.com/{repository}/actions/runs/{approval_run}"
    changes = _changes(delta)
    status_text = "No telemetry changes; no telemetry approval needed." if not changes and status == "pending" else STATUSES[status]
    header = (f"{marker}\n## Telemetry contract review\n\n**Status:** {status_text}\n\n"
              f"**Source head:** `{head[:12]}` · **Collection run:** [{source_run}]({source_url}) "
              f"· **Attempt:** {manifest.get('run_attempt', 1)}\n\n")
    if approval_run != source_run:
        header += f"**Approval run:** [{approval_run}]({approval_url})\n\n"
    if manifest.get("reused_approval_run_id"):
        previous = _number(manifest["reused_approval_run_id"], "original approval run")
        header += f"**Original human approval:** [{previous}](https://github.com/{repository}/actions/runs/{previous})\n\n"
    header += ("**Scope:** Full PR merge candidate versus its target base at collection time, "
               "not the previous PR commit. Unchanged metric attributes are omitted.\n\n")
    if status == "pending" and changes:
        footer = (f"[Review telemetry — approve or reject]({approval_url})\n\n"
                  "Open this exact run, choose **Review deployments**, select **telemetry-review**, "
                  "then approve or reject in GitHub's native dialog. The link opens the run; it does not approve automatically.\n\n")
    else:
        footer = f"[View this telemetry run]({approval_url})\n\n"
    footer += ("Approval covers the exact telemetry delta and tool/policy context; every new candidate is checked again. "
               "Normal PR code-review requirements still apply. "
               "This source-level comparison does not verify runtime emission.\n")
    return _bounded(header, _body(changes), footer, source_url)
