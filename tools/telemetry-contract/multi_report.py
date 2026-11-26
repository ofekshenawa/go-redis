"""Combine source-level checks without storing a baseline in the repository."""
import argparse
import json
from pathlib import Path


def collect_rows(report, api_file, otel_outcome, api_outcome):
    """Write the combined check results and a Markdown summary in report.

    The OTel delta and apidiff output supply the findings. The workflow step
    outcomes distinguish successful checks with no findings from checks that
    failed to collect data. A failed check gets an error row and blocks review.
    """
    rows = []
    for name, outcome in (("OTel", otel_outcome), ("Public API", api_outcome)):
        if outcome != "success":
            rows.append({"name": name, "result": "error", "diff": ["Collection failed; see the collection log."]})
            continue
        if name == "OTel":
            raw = json.loads((report / "delta.json").read_bytes())
            diff = raw["changes"]
            if raw["version"] != 1 or not isinstance(diff, list) or raw["has_changes"] != bool(diff):
                raise ValueError("Invalid OTel output")
        else:
            diff = sorted(set(api_file.read_text().splitlines()))
        rows.append({"name": name, "result": "needs_review" if diff else "pass", "diff": diff})
    delta = {"version": 2, "has_changes": any(row["result"] == "needs_review" for row in rows), "checks": rows}
    (report / "delta.json").write_text(json.dumps(delta, ensure_ascii=True, sort_keys=True, indent=2) + "\n")
    summary = ["# Negative tests", ""]
    summary.extend(f"- {row['name']}: {row['result']} ({len(row['diff'])} findings)" for row in rows)
    summary.extend(["", "Download the report artifact for the full, structured diff in `delta.json`."])
    (report / "report.md").write_text("\n".join(summary) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("api_file", type=Path)
    parser.add_argument("otel_outcome")
    parser.add_argument("api_outcome")
    args = parser.parse_args()
    collect_rows(args.report, args.api_file, args.otel_outcome, args.api_outcome)
