"""Reuse explicit human approvals, using GitHub artifacts as bounded evidence."""

import hashlib
import json
import os
from pathlib import Path
import tempfile

from gate import ENVIRONMENT, Rejected, approved_reviewer, number, require, sha, validate_artifacts


REVIEW_WORKFLOW = ".github/workflows/telemetry-review.yml"
COLLECTION_WORKFLOW = ".github/workflows/telemetry-contract.yml"
TOOL = "tools/telemetry-contract/"
FILES = {"state.json", "manifest.json", "delta.json", "report.md"}
MAX_BYTES = 20 * 1024 * 1024
INVALID = (Rejected, ValueError, KeyError, TypeError, AttributeError, OSError)


def canonical(value, field=""):
    if isinstance(value, dict):
        return {key: canonical(item, key) for key, item in value.items()}
    if isinstance(value, list):
        values = [canonical(item) for item in value]
        return sorted(values, key=encoded) if field in ("changes", "attributes", "labels") else values
    return value


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def code_version(api, prefix, revision):
    """Hash relevant file contents/modes, never the repository commit itself."""
    sha(revision, 40, "code revision")
    commit = api.get(f"{prefix}/git/commits/{revision}")
    tree_sha = sha(commit["tree"]["sha"], 40, "code tree")
    tree = api.get(f"{prefix}/git/trees/{tree_sha}?recursive=1")
    require(tree.get("sha") == tree_sha and tree.get("truncated") is False,
            "Cannot establish the complete telemetry tool version.")
    entries = {}
    for entry in tree["tree"]:
        path = entry["path"]
        if path not in (COLLECTION_WORKFLOW, REVIEW_WORKFLOW):
            if not path.startswith(TOOL) or entry["type"] == "tree":
                continue
            relative = path.removeprefix(TOOL)
            # Documentation and tests cannot change the collector or gate.
            if (relative.endswith(".md") or relative.startswith(("test_", "testdata/", "__pycache__/"))
                    or relative.endswith(("_test.go", ".pyc"))):
                continue
        require(entry["type"] == "blob" and entry["mode"] in ("100644", "100755"),
                "Telemetry tooling must consist of regular files.")
        entries[path] = [entry["mode"], sha(entry["sha"], 40, "code blob")]
    require({COLLECTION_WORKFLOW, REVIEW_WORKFLOW, TOOL + "main.go", TOOL + "gate.py",
             TOOL + "controller.py", TOOL + "approval_reuse.py"} <= entries.keys(),
            "Telemetry approval reuse is not installed in this revision.")
    return digest(entries)


def context(api, prefix, manifest, controller_sha):
    revisions = {manifest["base_sha"], manifest["merge_sha"], controller_sha}
    versions = {revision: code_version(api, prefix, revision) for revision in revisions}
    environment = api.get(f"{prefix}/environments/{ENVIRONMENT}")
    rules = [rule for rule in environment["protection_rules"] if rule["type"] == "required_reviewers"]
    require(environment.get("name") == ENVIRONMENT and len(rules) == 1 and rules[0].get("reviewers"),
            "Approval reuse requires configured Environment reviewers.")
    reviewers = sorted([item["type"], number(item["reviewer"]["id"], "Environment reviewer")]
                       for item in rules[0]["reviewers"])
    return {"version": 1, "base_tool": versions[manifest["base_sha"]],
            "candidate_tool": versions[manifest["merge_sha"]], "controller": versions[controller_sha],
            "environment": {"id": number(environment["id"], "Environment"), "reviewers": reviewers,
                            "prevent_self_review": rules[0].get("prevent_self_review", False),
                            "can_admins_bypass": environment.get("can_admins_bypass")}}


def fingerprint(manifest, delta, versions):
    return digest({"repository": manifest["repository"], "pr": manifest["pr"],
                   "base_ref": manifest["base_ref"], "context": versions, "delta": canonical(delta)})


def artifact_name(manifest, value):
    return f"telemetry-approval-{number(manifest['pr'], 'PR')}-{sha(value, 64, 'fingerprint')}"


def verified_approval(api, prefix, artifact_id, current, manifest, delta):
    """Verify the original manual run; reused/merely green runs are not evidence."""
    artifact = api.get(f"{prefix}/actions/artifacts/{number(artifact_id, 'approval artifact')}")
    require(artifact.get("id") == artifact_id and artifact.get("name") == current["approval_artifact"]
            and artifact.get("expired") is False and 0 < artifact.get("size_in_bytes", 0) <= MAX_BYTES,
            "Prior approval artifact is missing, expired or invalid.")
    run_id = number(artifact["workflow_run"]["id"], "approval run")
    require(run_id < current["controller_run_id"], "Approval must precede the current run.")
    run = api.get(f"{prefix}/actions/runs/{run_id}")
    require(run.get("id") == run_id and run.get("repository", {}).get("full_name") == current["repository"]
            and run.get("head_repository", {}).get("full_name") == current["repository"]
            and run.get("event") == "workflow_run" and run.get("path") == REVIEW_WORKFLOW
            and run.get("run_attempt") == 1 and run.get("status") == "completed"
            and run.get("conclusion") == "success", "Prior approval is not a successful trusted review run.")
    require(artifact["workflow_run"]["head_sha"] == run["head_sha"], "Approval artifact run identity changed.")
    with tempfile.TemporaryDirectory(prefix="telemetry-approval-", dir=os.environ.get("RUNNER_TEMP")) as temporary:
        root = Path(temporary)
        api.download(current["repository"], run_id, artifact["name"], root)
        files = list(root.iterdir())
        require({p.name for p in files} == FILES and all(p.is_file() and not p.is_symlink() for p in files),
                "Approval artifact must contain only the four review data files.")
        require(sum(p.stat().st_size for p in files) <= MAX_BYTES, "Approval evidence exceeds the size limit.")
        previous = json.loads((root / "state.json").read_bytes())
        old = json.loads((root / "manifest.json").read_bytes())
        require(previous.get("repository") == current["repository"]
                and previous.get("controller_run_id") == run_id and previous.get("controller_attempt") == 1
                and previous.get("controller_sha") == run["head_sha"] and previous.get("validated") is True
                and previous.get("needs_review") is True and previous.get("has_changes") is True
                and not previous.get("reused_approval"), "Evidence did not originate in a manual review.")
        require((old.get("repository"), old.get("pr"), old.get("base_ref"))
                == (manifest["repository"], manifest["pr"], manifest["base_ref"]),
                "Approval belongs to another repository, PR or target.")
        old, old_delta = validate_artifacts(root / "manifest.json", root / "delta.json", root / "report.md", {
            "GITHUB_REPOSITORY": current["repository"], "GITHUB_RUN_ID": str(previous["source_run_id"]),
            "GITHUB_RUN_ATTEMPT": "1", "GITHUB_SHA": old["merge_sha"], "TELEMETRY_TOOL_SHA": old["base_sha"]})
        source = api.get(f"{prefix}/actions/runs/{old['run_id']}")
        require(source.get("id") == old["run_id"] and source.get("event") == "pull_request"
                and source.get("repository", {}).get("full_name") == current["repository"]
                and source.get("path") == COLLECTION_WORKFLOW and source.get("run_attempt") == 1
                and source.get("status") == "completed" and source.get("conclusion") == "success"
                and source.get("head_sha") == old["head_sha"] == previous.get("source_head_sha"),
                "Original collection run is no longer valid.")
        versions = context(api, prefix, old, previous["controller_sha"])
        require(versions == current["approval_context"] == previous.get("approval_context")
                and fingerprint(old, old_delta, versions) == current["fingerprint"] == previous.get("fingerprint")
                and canonical(old_delta) == canonical(delta), "Approved telemetry effect or policy has changed.")
    reviewer = approved_reviewer(api, prefix, run_id)
    # A rerun can begin while evidence is being read. Never accept its old review.
    latest = api.get(f"{prefix}/actions/runs/{run_id}")
    require(latest.get("run_attempt") == 1 and latest.get("status") == "completed"
            and latest.get("conclusion") == "success", "Original review was rerun or is no longer successful.")
    return {"artifact_id": artifact_id, "run_id": run_id, "reviewer": reviewer}


def find_approval(api, prefix, current, manifest, delta):
    """Missing/unverifiable historical evidence requests a fresh manual approval."""
    try:
        listing = api.get(f"{prefix}/actions/artifacts?name={current['approval_artifact']}&per_page=20")
        candidates = listing["artifacts"]
        require(isinstance(candidates, list) and type(listing.get("total_count")) is int
                and listing["total_count"] == len(candidates) <= 20,
                "Approval history is incomplete or exceeds the bounded lookup.")
        if candidates:
            # A newer manual request supersedes older approvals even if it was
            # rejected, cancelled or is still waiting. Never resurrect an older
            # approval by skipping past unsuccessful/expired evidence.
            latest = max(candidates[:20], key=lambda artifact: number(artifact["id"], "artifact"))
            return verified_approval(api, prefix, number(latest["id"], "artifact"), current, manifest, delta)
    except INVALID:
        pass
    return None
