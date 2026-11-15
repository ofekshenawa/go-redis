"""Bind the report to this PR candidate and this fresh workflow run."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


def prepare(base, candidate, report):
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    pr = event["pull_request"]
    if os.environ["GITHUB_RUN_ATTEMPT"] != "1":
        raise ValueError("Re-runs are not supported: push a new commit or reopen the PR for a fresh report and approval.")
    sha = os.environ["GITHUB_SHA"]
    parents = subprocess.check_output(
        ["git", "-C", candidate, "show", "-s", "--format=%P", sha], text=True
    ).strip().split()
    if parents != [pr["base"]["sha"], pr["head"]["sha"]]:
        raise ValueError("The merge candidate does not match the event base/head. Start a fresh run.")
    tool_sha = subprocess.check_output(
        ["git", "-C", base, "rev-parse", "HEAD"], text=True
    ).strip()
    if tool_sha != parents[0]:
        raise ValueError("The trusted collector must come from the exact base commit.")
    report = Path(report)
    manifest = {
        "repository": os.environ["GITHUB_REPOSITORY"],
        "pr": pr["number"], "base_ref": pr["base"]["ref"],
        "base_sha": parents[0], "head_sha": parents[1], "merge_sha": sha,
        "tool_sha": tool_sha,
        "run_id": int(os.environ["GITHUB_RUN_ID"]), "run_attempt": 1,
        "report_sha256": hashlib.sha256((report / "report.md").read_bytes()).hexdigest(),
        "delta_sha256": hashlib.sha256((report / "delta.json").read_bytes()).hexdigest(),
    }
    (report / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
        summary.write((report / "report.md").read_text())
        summary.write("\n\n### This report\n\n")
        summary.write(f"Base: `{parents[0]}`\n\nPR head: `{parents[1]}`\n\nMerge candidate: `{sha}`\n\n")
        summary.write(f"Report SHA-256: `{manifest['report_sha256']}`\n\n")
        summary.write("If changes are listed, review them here, then choose **Review deployments → telemetry-review → Approve and deploy**. This job only validates telemetry; it deploys nothing.\n")


if __name__ == "__main__":
    try:
        prepare(*sys.argv[1:])
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as error:
        sys.exit(f"telemetry-contract: {error}")
