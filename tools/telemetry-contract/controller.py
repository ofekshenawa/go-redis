#!/usr/bin/env python3
"""Trusted workflow_run controller; untrusted collection artifacts are data only."""

import argparse
import json
import os
import re
from pathlib import Path
import sys
import tempfile

import alternative_gate
from comment_report import comment_marker, render_comment
from gate import GitHub, Rejected, approved_reviewer, current_candidate, number, require, sha, validate_artifacts
from approval_reuse import context, fingerprint, artifact_name, find_approval, INVALID


WORKFLOW = ".github/workflows/telemetry-contract.yml"
MAX_BYTES = 20 * 1024 * 1024
REQUIRED_FILES = {"manifest.json", "delta.json", "report.md"}
ALLOWED_FILES = REQUIRED_FILES | {"base.json", "candidate.json", "review.txt"}
STATUS_CONTEXT = "Negative tests / telemetry-contract"


def identity(env):
    repository = env.get("GITHUB_REPOSITORY", "")
    require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository), "Invalid controller repository.")
    require(number(env.get("GITHUB_RUN_ATTEMPT"), "controller attempt") == 1,
            "Controller reruns are not supported; start a fresh collection run.")
    return {"repository": repository, "controller_run_id": number(env.get("GITHUB_RUN_ID"), "controller run"),
            "controller_attempt": 1,
            "controller_sha": sha(env.get("GITHUB_WORKFLOW_SHA"), 40, "trusted controller revision")}


def save(state_dir, state):
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "state.json").write_text(json.dumps(state, sort_keys=True) + "\n")


def outputs(env, **values):
    if env.get("GITHUB_OUTPUT"):
        with open(env["GITHUB_OUTPUT"], "a") as stream:
            for key, value in values.items():
                text = str(value).lower() if isinstance(value, bool) else str(value)
                require("\n" not in text and "\r" not in text, "Invalid workflow output.")
                stream.write(f"{key}={text}\n")


def run_url(state):
    return f"https://github.com/{state['repository']}/actions/runs/{state['controller_run_id']}"


def status_marker(state):
    return f"Telemetry source {state['source_run_id']}: "


def latest_status(api, prefix, state):
    # The combined-status endpoint omits creator. The history endpoint includes
    # it and returns newest first, so stop at the first matching context.
    head = state["source_head_sha"]
    for page in range(1, 11):
        statuses = api.get(f"{prefix}/commits/{head}/statuses?per_page=100&page={page}")
        require(isinstance(statuses, list), "Invalid commit status response.")
        for item in statuses:
            if (item.get("context") or "").casefold() == STATUS_CONTEXT.casefold():
                require(item.get("url", "").endswith(f"/statuses/{head}"), "Status commit does not match.")
                return item
        if len(statuses) < 100:
            return None
    raise Rejected("Too many commit statuses to identify this telemetry result.")


def status_identity(api, prefix, state):
    latest = latest_status(api, prefix, state)
    require(latest is not None and latest.get("target_url") == run_url(state)
            and latest.get("description", "").startswith(status_marker(state))
            and latest.get("creator", {}).get("login") == "github-actions[bot]"
            and latest.get("creator", {}).get("type") == "Bot"
            and number(latest.get("id"), "status ID") >= state["status_id"],
            "Status identity changed or a newer telemetry review superseded this controller.")


def post_status(api, prefix, state, conclusion, summary):
    description = status_marker(state) + " ".join(summary.splitlines())
    return api.post(f"{prefix}/statuses/{state['source_head_sha']}", {
        "context": STATUS_CONTEXT, "state": conclusion, "target_url": run_url(state),
        "description": description[:140],
    })


def start_status(api, prefix, state):
    latest = latest_status(api, prefix, state)
    if latest is not None:
        source = re.match(r"Telemetry source ([0-9]+): ", latest.get("description", ""))
        controller = re.fullmatch(re.escape(f"https://github.com/{state['repository']}/actions/runs/")
                                  + r"([0-9]+)", latest.get("target_url", ""))
        require(source is not None and controller is not None
                and latest.get("creator", {}).get("login") == "github-actions[bot]"
                and latest.get("creator", {}).get("type") == "Bot"
                and (int(source[1]), int(controller[1])) <= (state["source_run_id"], state["controller_run_id"]),
                "A newer or unrelated telemetry status must not be overwritten.")
    return post_status(api, prefix, state, "pending", "Validating collection and publishing its report before review.")


def conclude(api, prefix, state, conclusion, summary):
    # Commit statuses are append-only: refuse to supersede another controller's
    # latest result rather than accidentally turning its newer candidate green.
    status_identity(api, prefix, state)
    post_status(api, prefix, state, conclusion, summary)


def source_run(api, prefix, source_id, repository):
    source = api.get(f"{prefix}/actions/runs/{source_id}")
    require(source.get("id") == source_id and source.get("repository", {}).get("full_name") == repository,
            "Collection run belongs to another repository.")
    sha(source.get("head_sha"), 40, "collection head")
    return source


def validate_source(source):
    require(source.get("event") == "pull_request" and source.get("path") == WORKFLOW
            and source.get("name") == "Telemetry contract", "Unexpected collection workflow or event.")
    require(source.get("run_attempt") == 1, "Collection reruns are not supported.")
    require(source.get("status") == "completed" and source.get("conclusion") == "success",
            "Telemetry collection did not succeed; collection failures cannot be approved.")


def read_artifact(api, prefix, source, state_dir):
    listing = api.get(f"{prefix}/actions/runs/{source['id']}/artifacts?per_page=100")
    expected = f"telemetry-report-{source['id']}-1"
    matches = [item for item in listing.get("artifacts", []) if item.get("name") == expected]
    require(len(matches) == 1, "Expected exactly one immutable collection artifact.")
    artifact = matches[0]
    require(artifact.get("expired") is False and 0 < artifact.get("size_in_bytes", 0) <= MAX_BYTES,
            "Collection artifact is expired, empty or too large.")
    require(artifact.get("workflow_run", {}).get("id") == source["id"]
            and artifact.get("workflow_run", {}).get("head_sha") == source["head_sha"],
            "Artifact is not bound to the expected collection run and head.")
    # GitHub CLI owns redirect handling and safe ZIP extraction. Use an isolated
    # temp directory: downloaded files never overwrite trusted code or state.
    with tempfile.TemporaryDirectory(prefix="telemetry-report-", dir=os.environ.get("RUNNER_TEMP")) as temporary:
        root = Path(temporary)
        api.download(prefix.removeprefix("/repos/"), source["id"], expected, root)
        files = list(root.iterdir())
        require(REQUIRED_FILES <= {p.name for p in files} <= ALLOWED_FILES,
                "Artifact contains missing or unexpected files.")
        require(all(p.is_file() and not p.is_symlink() for p in files),
                "Artifact may contain flat data files only.")
        require(sum(p.stat().st_size for p in files) <= MAX_BYTES,
                "Uncompressed artifact exceeds the size limit.")
        state_dir.mkdir(parents=True, exist_ok=True)
        for name in REQUIRED_FILES:
            (state_dir / name).write_bytes((root / name).read_bytes())


def validate_context(api, prefix, state, state_dir, source):
    raw = json.loads((state_dir / "manifest.json").read_bytes())
    pr_number = number(raw.get("pr"), "PR number")
    live = api.get(f"{prefix}/pulls/{pr_number}")
    require(live.get("head", {}).get("sha") == source["head_sha"],
            "Collection run does not describe the current PR head.")
    head_repo_id = number(source.get("head_repository", {}).get("id"), "source head repository")
    require(live.get("head", {}).get("repo", {}).get("id") == head_repo_id,
            "Collection head repository does not match the PR, including its fork.")
    associations = source.get("pull_requests", [])
    require(isinstance(associations, list), "Invalid collection PR associations.")
    if associations:
        require(any(item.get("number") == pr_number and item.get("head", {}).get("sha") == source["head_sha"]
                    for item in associations), "Collection run is associated with another PR.")
    live_base = live.get("base", {}).get("sha")
    live_merge = live.get("merge_commit_sha")
    synthetic = {"GITHUB_REPOSITORY": state["repository"], "GITHUB_RUN_ID": str(source["id"]),
                 "GITHUB_RUN_ATTEMPT": "1", "GITHUB_SHA": live_merge, "TELEMETRY_TOOL_SHA": live_base}
    manifest, delta = validate_artifacts(state_dir / "manifest.json", state_dir / "delta.json",
                                         state_dir / "report.md", synthetic)
    require(manifest["head_sha"] == source["head_sha"] and manifest["tool_sha"] == manifest["base_sha"],
            "Collection must use the exact target base tool and source head.")
    current_candidate(api, prefix, manifest)
    merge = api.get(f"{prefix}/git/commits/{manifest['merge_sha']}")
    require([parent.get("sha") for parent in merge.get("parents", [])]
            == [manifest["base_sha"], manifest["head_sha"]], "Merge parents do not match the collected base and head.")
    if delta["version"] == 2:
        # The collector scripts themselves always come from B. The PR-triggered
        # entrypoint is also data from the PR, so verify its Git blob before trusting it.
        path = f"{prefix}/contents/{WORKFLOW}?ref="
        base_blob = sha(api.get(path + manifest["base_sha"])["sha"], 40, "base workflow blob")
        require(api.get(path + manifest["merge_sha"])["sha"] == base_blob,
                "Collection workflow changed; review and install the method separately.")
    # This binds the artifacts to a run; it is not proof that an edited PR
    # workflow was honest. Required CODEOWNERS review protects collector changes.
    manifest["run_attempt"] = 1
    manifest["approval_run_id"] = state["controller_run_id"]
    return manifest, delta


def own_comment(comment, manifest, state):
    return (comment.get("user", {}).get("type") == "Bot"
            and comment.get("user", {}).get("login") == "github-actions[bot]"
            and comment.get("body", "").startswith(comment_marker(manifest) + "\n")
            and "(" + run_url(state) + ")" in comment.get("body", ""))


def publish(api, prefix, state, manifest, delta):
    body = render_comment(review_manifest(manifest, state), delta, "reusing" if state.get("reused_approval") else "pending")
    for page in range(1, 11):
        comments = api.get(f"{prefix}/issues/{manifest['pr']}/comments?per_page=100&page={page}")
        for comment in comments:
            if own_comment(comment, manifest, state):
                return api.patch(f"{prefix}/issues/comments/{comment['id']}", {"body": body})
        if len(comments) < 100:
            break
    else:
        raise Rejected("Too many PR comments to identify this report safely.")
    return api.post(f"{prefix}/issues/{manifest['pr']}/comments", {"body": body})


def update_comment(api, prefix, state, manifest, delta, status):
    if not state.get("comment_id"):
        return
    comment = api.get(f"{prefix}/issues/comments/{state['comment_id']}")
    require(own_comment(comment, manifest, state), "Comment identity no longer matches this report and controller.")
    api.patch(f"{prefix}/issues/comments/{state['comment_id']}", {"body": render_comment(review_manifest(manifest, state), delta, status)})


def review_manifest(manifest, state):
    result = dict(manifest)
    if state.get("reused_approval"):
        result["reused_approval_run_id"] = state["reused_approval"]["run_id"]
    return result


def prepare(source_id, state_dir, env, api):
    state_dir = Path(state_dir)
    state = identity(env)
    state.update(source_run_id=number(source_id, "source run"), validated=False)
    prefix = "/repos/" + state["repository"]
    source = source_run(api, prefix, state["source_run_id"], state["repository"])
    state["source_head_sha"] = source["head_sha"]
    status = start_status(api, prefix, state)
    state["status_id"] = number(status.get("id"), "status ID")
    save(state_dir, state)
    outputs(env, status_id=state["status_id"])
    try:
        validate_source(source)
        read_artifact(api, prefix, source, state_dir)
        manifest, delta = validate_context(api, prefix, state, state_dir, source)
        state.update(pr=manifest["pr"], has_changes=delta["has_changes"], needs_review=delta["has_changes"],
                     reused_approval=None, approval_artifact="")
        if delta["has_changes"] and manifest["base_ref"] in alternative_gate.MODES:
            state.update(alternative_gate.publish(api, prefix, state, manifest, delta), needs_review=False)
        elif delta["has_changes"]:
            try:
                state["approval_context"] = context(api, prefix, manifest, state["controller_sha"])
                state["fingerprint"] = fingerprint(manifest, delta, state["approval_context"])
                state["approval_artifact"] = artifact_name(manifest, state["fingerprint"])
                state["reused_approval"] = find_approval(api, prefix, state, manifest, delta)
                state["needs_review"] = state["reused_approval"] is None
            except INVALID:
                # Reuse is optional. Incomplete version/evidence lookup never
                # grants approval; the existing manual path remains available.
                state.pop("approval_context", None)
                state.pop("fingerprint", None)
                state["approval_artifact"] = ""
            comment = publish(api, prefix, state, manifest, delta)
            state.update(comment_id=number(comment.get("id"), "comment ID"), comment_url=comment.get("html_url", ""))
        state["validated"] = True
        save(state_dir, state)
        outputs(env, has_changes=state["has_changes"], needs_review=state["needs_review"], pr=state["pr"],
                state_artifact=state["approval_artifact"] if state["needs_review"] and state["approval_artifact"]
                else f"telemetry-review-state-{state['controller_run_id']}-1", comment_url=state.get("comment_url", ""))
        return state
    except (Rejected, ValueError, KeyError, TypeError, AttributeError, OSError) as exc:
        reason = str(exc) if isinstance(exc, Rejected) else "Invalid collection artifact or API response."
        state["error"] = reason
        save(state_dir, state)
        conclude(api, prefix, state, "failure", reason)
        raise Rejected(reason) from None


def finish(state_dir, publish_result, approval_result, env, api):
    state_dir = Path(state_dir)
    state = json.loads((state_dir / "state.json").read_bytes())
    expected = identity(env)
    require(all(state.get(key) == value for key, value in expected.items()),
            "Controller state belongs to another run or trusted revision.")
    prefix = "/repos/" + state["repository"]
    status_identity(api, prefix, state)
    manifest = delta = None
    status = "failed"
    try:
        require(publish_result == "success" and state.get("validated") is True,
                "Report publication or collection validation did not succeed.")
        source = source_run(api, prefix, state["source_run_id"], state["repository"])
        require(source["head_sha"] == state["source_head_sha"], "Source run identity changed.")
        validate_source(source)
        # Load the immutable report for status updates even if live context became stale.
        manifest = json.loads((state_dir / "manifest.json").read_bytes())
        manifest.update(run_attempt=1, approval_run_id=state["controller_run_id"])
        delta = json.loads((state_dir / "delta.json").read_bytes())
        try:
            manifest, delta = validate_context(api, prefix, state, state_dir, source)
        except Rejected:
            status = "stale"
            raise
        require(state.get("pr") == manifest["pr"] and state.get("has_changes") == delta["has_changes"],
                "Prepared report identity changed.")
        if state.get("decision_mode"):
            require(approval_result == "skipped", "Unexpected Environment decision for this report.")
            alternative_gate.reconcile(api, prefix, state, manifest, delta, poll=True)
            return "Report decision checked."
        if delta["has_changes"]:
            if delta["version"] == 2:
                comment = api.get(f"{prefix}/issues/comments/{state['comment_id']}")
                expected_body = render_comment(review_manifest(manifest, state), delta,
                                               "reusing" if state.get("reused_approval") else "pending")
                require(own_comment(comment, manifest, state) and comment["body"] == expected_body,
                        "The posted report changed after collection.")
            status = "cancelled" if approval_result == "cancelled" else "rejected"
            if state.get("approval_context"):
                versions = context(api, prefix, manifest, state["controller_sha"])
                require(versions == state["approval_context"]
                        and fingerprint(manifest, delta, versions) == state["fingerprint"]
                        and artifact_name(manifest, state["fingerprint"]) == state["approval_artifact"],
                        "Approval effect or policy changed after publication.")
            if state.get("reused_approval"):
                require(approval_result == "skipped" and state.get("needs_review") is False,
                        "Unexpected review job for reused approval.")
                evidence = find_approval(api, prefix, state, manifest, delta)
                require(evidence == state["reused_approval"], "Original approval is no longer reusable; start a fresh run.")
            else:
                require(approval_result == "success", "Telemetry changes were not approved in this controller run.")
                approved_reviewer(api, prefix, state["controller_run_id"])
            status = "stale"
            current_candidate(api, prefix, manifest)
            status = "failed"
            update_comment(api, prefix, state, manifest, delta, "reused" if state.get("reused_approval") else "approved")
        else:
            require(approval_result == "skipped", "Unexpected approval job for an unchanged contract.")
        conclude(api, prefix, state, "success", "Telemetry contract unchanged." if not delta["has_changes"]
                 else "Telemetry change approved for this exact report and current PR candidate. " + state["comment_url"])
        return "Telemetry contract passed."
    except (Rejected, ValueError, KeyError, TypeError, AttributeError, OSError) as exc:
        reason = str(exc) if isinstance(exc, Rejected) else "Invalid controller state or API response."
        # The required status is blocked before any cosmetic comment update.
        conclude(api, prefix, state, "failure", reason)
        if manifest is not None and delta is not None and not state.get("decision_mode"):
            update_comment(api, prefix, state, manifest, delta, status)
        raise Rejected(reason) from None


def fail(source_id, status_id, env, api):
    """Block this controller's status if state transfer/finalization failed."""
    state = identity(env)
    state.update(source_run_id=number(source_id, "source run"), status_id=number(status_id, "status ID"))
    prefix = "/repos/" + state["repository"]
    source = source_run(api, prefix, state["source_run_id"], state["repository"])
    state["source_head_sha"] = source["head_sha"]
    # No downloaded state or report is needed. Identity verification refuses
    # to overwrite a status belonging to a different source/controller.
    conclude(api, prefix, state, "failure",
             "Telemetry finalization failed or was cancelled. No telemetry approval was accepted; start a fresh collection run.")
    return "Telemetry status marked as failed."


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--source-run", required=True)
    prepare_parser.add_argument("--state-dir", required=True)
    finish_parser = commands.add_parser("finish")
    finish_parser.add_argument("--state-dir", required=True)
    finish_parser.add_argument("--publish-result", required=True)
    finish_parser.add_argument("--approval-result", required=True)
    fail_parser = commands.add_parser("fail")
    fail_parser.add_argument("--source-run", required=True)
    fail_parser.add_argument("--status-id", required=True)
    args = parser.parse_args()
    try:
        api = GitHub(os.environ.get("GITHUB_TOKEN"))
        if args.command == "prepare":
            prepare(args.source_run, args.state_dir, os.environ, api)
            print("Telemetry report verified and published.")
        elif args.command == "finish":
            print(finish(args.state_dir, args.publish_result, args.approval_result, os.environ, api))
        else:
            print(fail(args.source_run, args.status_id, os.environ, api))
        return 0
    except (Rejected, ValueError, KeyError, TypeError, AttributeError, OSError) as exc:
        reason = str(exc) if isinstance(exc, Rejected) else "Invalid controller data or API response."
        print("Telemetry controller blocked: " + reason, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
