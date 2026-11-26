"""Render the bot's canonical report and recognize the two allowed task-list edits."""

from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "telemetry-contract"))
from comment_report import COMMENT_PREFIX, _attribute, _changes, _metric_diff, _number, _sha, _source, comment_marker
from gate import validate_rows

ACTIONS = {
    "approve": "- [ ] ✅ Approve these changes",
    "reject": "- [ ] ❌ Reject these changes",
}
CHOICES = "\n".join(ACTIONS.values())
MARKER = "<!-- negative-tests:checkbox "
MAX_LINES_PER_CHECK = 12
MAX_LINE = 260


def _otel_lines(changes):
    changes = _changes({"version": 1, "has_changes": bool(changes), "changes": changes})
    attached = {"-": set(), "+": set()}
    lines = []
    for change in changes:
        if change["entity"] != "metric":
            continue
        metric = change["after"] or change["before"]
        name = _source(metric["name"])
        kind = "label" if metric["instrument"].startswith("Prometheus") else "attribute"
        diff, removed, added = _metric_diff(change)
        attached["-"].update(removed)
        attached["+"].update(added)
        if change["action"] in ("add", "remove"):
            sign = "+" if change["action"] == "add" else "-"
            lines.append(f"{sign} OTel: {name} — {_source(metric['instrument'])}, unit {_source(metric['unit']) or '(none)'}")
        for line in diff:
            sign = line[0]
            if sign not in "+-":
                continue
            field = line[2:]
            if field.startswith("attribute/label: "):
                verb = "added" if sign == "+" else "removed"
                field = f"{verb} {kind} " + field.removeprefix("attribute/label: ")
            elif change["action"] != "modify" and not field.startswith("caller-provided attributes: "):
                continue  # The new/removed metric definition already shows its instrument and unit.
            lines.append(f"{sign} OTel: {name} — {field}")
    for change in changes:
        if change["entity"] == "attribute_declaration":
            for sign, value in (("-", change["before"]), ("+", change["after"])):
                if value and _attribute(value) not in attached[sign]:
                    lines.append(f"{sign} OTel: attribute declaration {_source(value['name'])} ({_source(value['type'])})")
    return lines


def _preview(lines):
    shown = [line if len(line) <= MAX_LINE else line[:MAX_LINE] + "… (full line in report)"
             for line in lines[:MAX_LINES_PER_CHECK]]
    if len(lines) > len(shown):
        shown.append(f"  … {len(lines) - len(shown)} more lines in the full report")
    return shown


def render(manifest, delta, decision=None, reviewer=None, decision_run_id=None):
    """The full report always remains available in the linked collection run."""
    marker = MARKER + comment_marker(manifest).removeprefix(COMMENT_PREFIX)
    repo = manifest["repository"]
    if not re.fullmatch(r"[A-Za-z0-9-]+/[A-Za-z0-9_.-]+", repo):
        raise ValueError("Invalid report repository.")
    head = _sha(manifest["head_sha"], 40, "PR head")
    validate_rows(delta)
    if delta.get("version") != 2 or not delta["has_changes"]:
        raise ValueError("Checkboxes require a two-check report with findings.")
    otel, api = delta["checks"]
    lines = _preview(_otel_lines(otel["diff"])) + _preview(
        ["- Public API: " + _source(line.removeprefix("- ")) for line in api["diff"]])
    run_url = f"https://github.com/{repo}/actions/runs"
    report = (f"{marker}\n## Negative tests\n\n```diff\n" + "\n".join(lines)
              + f"\n```\n\n[Full report and collection log]({run_url}/{manifest['run_id']})"
              + f" · Commit `{head[:12]}`\n\n")
    if decision is None:
        return report + "Choose one action for this report:\n\n" + CHOICES
    if decision not in ACTIONS or not isinstance(reviewer, str) or not re.fullmatch(r"[A-Za-z0-9-]+", reviewer):
        raise ValueError("Invalid report decision or reviewer.")
    status = "Approved" if decision == "approve" else "Rejected"
    receipt = f"**{status}** by `{reviewer}`."
    if decision_run_id is not None:
        receipt += f" [Decision run]({run_url}/{_number(decision_run_id, 'decision run')})"
    return report + receipt


def edited_decision(expected, before, after):
    """Accept one checkbox tick, preserving all text and the order of its lines."""
    if not all(isinstance(body, str) for body in (expected, before, after)):
        return None
    # Markdown from GitHub's browser editor can use CRLF; only a uniform
    # representation of the canonical body is eligible, including the last line.
    if not expected.endswith("\n" + CHOICES) or before not in (expected, expected.replace("\n", "\r\n")):
        return None
    for name, action in ACTIONS.items():
        checked_choices = CHOICES.replace(action, action.replace("[ ]", "[x]", 1))
        checked = expected.removesuffix(CHOICES) + checked_choices
        if after in (checked, checked.replace("\n", "\r\n")):
            return name
    return None
