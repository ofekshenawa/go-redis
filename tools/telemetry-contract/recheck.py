"""Reconcile a GitHub decision against the latest publisher's immutable state."""
import json
import os
from pathlib import Path
import re
import tempfile

import alternative_gate
from controller import latest_status, source_run, status_identity, validate_context, validate_source
from gate import GitHub, Rejected, number, require

def check(api, repo, pr, *, removed=None):
    prefix = f"/repos/{repo}"
    live = api.get(f"{prefix}/pulls/{pr}")
    mode = alternative_gate.MODES.get(live["base"]["ref"])
    require(live["state"] == "open" and mode, "Select an open reaction or label demo PR.")
    head = live["head"]["sha"]
    status = latest_status(api, prefix, {"source_head_sha": head})
    require(status and alternative_gate.bot(status["creator"]), "No trusted status on this head yet.")
    match = re.fullmatch(re.escape(f"https://github.com/{repo}/actions/runs/") + r"([1-9][0-9]*)", status["target_url"])
    require(match, "Status is not owned by a review run.")
    run_id = int(match[1])
    run = api.get(f"{prefix}/actions/runs/{run_id}")
    require(run["repository"]["full_name"] == repo and run["head_repository"]["full_name"] == repo
            and run["path"] == ".github/workflows/telemetry-review.yml" and run["event"] == "workflow_run"
            and run["run_attempt"] == 1 and run["status"] == "completed" and run["conclusion"] == "success",
            "The original trusted publisher has not finished successfully; retry after completion.")
    # This artifact was created by the trusted publisher, not the PR collector.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        api.download(repo, run_id, f"telemetry-review-state-{run_id}-1", d)
        state = json.loads((d / "state.json").read_bytes())
        require(state["repository"] == repo and state["pr"] == pr and state["validated"] is True
                and state["controller_run_id"] == run_id and state["controller_attempt"] == 1
                and state["controller_sha"] == run["head_sha"] and state["source_head_sha"] == head
                and state["decision_mode"] == mode, "Publisher state does not match this candidate.")
        if removed is not None and (mode != "label" or removed != alternative_gate.label_name(pr, state["origin"])):
            print("Ignored a label action for an older report.")
            return
        status_identity(api, prefix, state)
        source = source_run(api, prefix, state["source_run_id"], repo)
        validate_source(source)
        manifest, delta = validate_context(api, prefix, state, d, source)
        alternative_gate.reconcile(api, prefix, state, manifest, delta)

if __name__ == "__main__":
    try:
        event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_bytes())
        if os.environ["GITHUB_EVENT_NAME"] == "workflow_dispatch":
            pr, removed = event["inputs"]["pr"], None
        else:
            require(event["action"] == "unlabeled", "Unsupported PR event.")
            pr, removed = event["number"], event["label"]["name"]
        check(GitHub(os.environ["GITHUB_TOKEN"]), os.environ["GITHUB_REPOSITORY"], number(pr, "PR"), removed=removed)
    except (Rejected, KeyError, TypeError, ValueError, OSError) as exc:
        raise SystemExit(str(exc))
