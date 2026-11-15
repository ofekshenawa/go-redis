#!/usr/bin/env python3
"""Final, trusted gate for the telemetry Environment demo (standard library only).

This process never executes PR code. Its manifest and artifacts must come from the
successful comparison job in this workflow run, using immutable Actions artifacts.
GitHub's review history does not identify a run attempt, so reruns fail closed:
start a fresh workflow run after changing the PR or its target branch instead.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


ENVIRONMENT = "telemetry-review"


class Rejected(Exception):
    """A check that must not become green."""


def require(condition, reason):
    if not condition:
        raise Rejected(reason)


def number(value, field):
    require(not isinstance(value, bool) and str(value).isdigit(), f"Invalid {field}.")
    result = int(value)
    require(result > 0, f"Invalid {field}.")
    return result


def sha(value, length, field):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{%d}" % length, value),
            f"Invalid {field}.")
    return value


def json_file(path):
    try:
        data = Path(path).read_bytes()
        require(len(data) <= 20 * 1024 * 1024, "Artifact is too large.")
        return json.loads(data), data
    except (OSError, UnicodeError, ValueError) as exc:
        raise Rejected("Missing or invalid JSON artifact.") from exc


class GitHub:
    def __init__(self, token):
        require(bool(token), "GITHUB_TOKEN is required.")
        self.token = token

    def get(self, path):
        # The API origin is fixed; artifact contents cannot redirect credentials.
        request = Request("https://api.github.com" + path, headers={
            "Authorization": "Bearer " + self.token,
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "telemetry-environment-demo",
        })
        try:
            with urlopen(request, timeout=30) as response:
                return json.load(response)
        except HTTPError as exc:
            raise Rejected(f"GitHub verification failed (HTTP {exc.code}).") from None
        except (URLError, OSError, ValueError):
            # Never include response bodies, authorization headers, or raw errors.
            raise Rejected("GitHub verification was unavailable or invalid.") from None


def validate_artifacts(manifest_path, delta_path, report_path, env):
    manifest, _ = json_file(manifest_path)
    delta, delta_bytes = json_file(delta_path)
    require(isinstance(manifest, dict), "Invalid manifest.")
    for key in ("base_sha", "head_sha", "merge_sha", "tool_sha"):
        sha(manifest.get(key), 40, key)
    for key in ("delta_sha256", "report_sha256"):
        sha(manifest.get(key), 64, key)
    require(isinstance(manifest.get("base_ref"), str) and manifest["base_ref"],
            "Invalid base_ref.")
    repo = manifest.get("repository", "")
    require(isinstance(repo, str) and re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo),
            "Invalid repository.")
    require(repo == env.get("GITHUB_REPOSITORY"), "Report belongs to another repository.")
    manifest["pr"] = number(manifest.get("pr"), "PR number")
    manifest["run_id"] = number(manifest.get("run_id"), "run ID")
    require(manifest["run_id"] == number(env.get("GITHUB_RUN_ID"), "current run ID"),
            "Report belongs to another workflow run.")
    require(number(manifest.get("run_attempt"), "run attempt") == 1
            and number(env.get("GITHUB_RUN_ATTEMPT"), "current run attempt") == 1,
            "Reruns cannot reuse Environment approvals. Start a fresh workflow run.")
    require(manifest["merge_sha"] == env.get("GITHUB_SHA"),
            "Report does not describe this workflow's merge candidate.")
    require(manifest["tool_sha"] == env.get("TELEMETRY_TOOL_SHA"),
            "Report was produced by a different trusted tool revision.")
    try:
        report_bytes = Path(report_path).read_bytes()
    except OSError as exc:
        raise Rejected("Missing telemetry report.") from exc
    require(bool(report_bytes), "Telemetry report is empty.")
    require(hashlib.sha256(report_bytes).hexdigest() == manifest["report_sha256"],
            "Telemetry report hash does not match the reviewed report.")
    require(hashlib.sha256(delta_bytes).hexdigest() == manifest["delta_sha256"],
            "Telemetry delta hash does not match the reviewed report.")
    require(isinstance(delta, dict) and delta.get("version") == 1
            and isinstance(delta.get("has_changes"), bool)
            and isinstance(delta.get("changes"), list), "Invalid telemetry delta.")
    require(delta["has_changes"] == bool(delta["changes"]), "Inconsistent telemetry delta.")
    for change in delta["changes"]:
        require(isinstance(change, dict)
                and change.get("entity") in ("metric", "attribute_declaration")
                and isinstance(change.get("identity"), str) and change["identity"]
                and change.get("action") in ("add", "remove", "modify")
                and "before" in change and "after" in change,
                "Invalid telemetry change.")
    return manifest, delta


def current_candidate(api, prefix, manifest):
    pr = api.get(f"{prefix}/pulls/{manifest['pr']}")
    require(isinstance(pr, dict), "Invalid PR response.")
    require(pr.get("state") == "open", "PR is no longer open.")
    require(pr.get("head", {}).get("sha") == manifest["head_sha"],
            "PR has a new commit. Start a fresh workflow run and review its report.")
    base = pr.get("base", {})
    require(base.get("ref") == manifest["base_ref"]
            and base.get("repo", {}).get("full_name") == manifest["repository"],
            "PR target changed. Start a fresh workflow run.")
    ref = api.get(f"{prefix}/git/ref/heads/{quote(manifest['base_ref'], safe='')}")
    require(isinstance(ref, dict) and ref.get("object", {}).get("sha") == manifest["base_sha"],
            "Target branch advanced. Update the PR and start a fresh workflow run.")
    require(pr.get("merge_commit_sha") == manifest["merge_sha"],
            "Merge candidate changed or is unavailable. Start a fresh workflow run.")


def approved_reviewer(api, prefix, run_id):
    environment = api.get(f"{prefix}/environments/{ENVIRONMENT}")
    require(isinstance(environment, dict) and environment.get("name") == ENVIRONMENT,
            "The telemetry-review Environment is not configured.")
    environment_id = number(environment.get("id"), "Environment ID")
    require(any(isinstance(rule, dict) and rule.get("type") == "required_reviewers"
                and rule.get("reviewers") for rule in environment.get("protection_rules", [])),
            "telemetry-review must have required reviewers.")
    reviews = api.get(f"{prefix}/actions/runs/{run_id}/approvals")
    require(isinstance(reviews, list), "Invalid workflow approval history.")
    for review in reviews:
        if not isinstance(review, dict) or review.get("state") != "approved":
            continue
        if not any(isinstance(item, dict) and item.get("id") == environment_id
                   for item in review.get("environments", [])):
            continue
        user = review.get("user", {})
        if not isinstance(user, dict) or user.get("type") != "User":
            continue
        login = user.get("login")
        if not isinstance(login, str) or not re.fullmatch(r"[A-Za-z0-9-]+", login):
            continue
        identity = user.get("id")
        if type(identity) is not int or identity <= 0:
            continue
        permission = api.get(f"{prefix}/collaborators/{quote(login, safe='')}/permission")
        if not isinstance(permission, dict):
            continue
        actor = permission.get("user", {})
        if (permission.get("role_name") in ("admin", "maintain")
                and isinstance(actor, dict) and actor.get("type") == "User"
                and actor.get("id") == identity and actor.get("login") == login):
            return login
    raise Rejected("This run has no telemetry-review approval from a repository admin or maintainer.")


def check(manifest_path, delta_path, report_path, collection_result, approval_result, env, api):
    require(collection_result == "success", "Telemetry collection/comparison did not succeed.")
    manifest, delta = validate_artifacts(manifest_path, delta_path, report_path, env)
    prefix = "/repos/" + manifest["repository"]
    current_candidate(api, prefix, manifest)
    if not delta["has_changes"]:
        require(approval_result == "skipped", "Unexpected approval job state for an unchanged contract.")
        return "Telemetry contract is unchanged; no approval is required."
    require(approval_result == "success", "Telemetry changes have not completed the approval job.")
    reviewer = approved_reviewer(api, prefix, manifest["run_id"])
    # Narrow the window in which a commit/retarget can race a reviewer lookup.
    # Strict up-to-date branch protection is still required at merge time.
    current_candidate(api, prefix, manifest)
    return f"Telemetry changes approved for this run by {reviewer}."


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("manifest", "delta", "report", "collection-result", "approval-result"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    try:
        message = check(args.manifest, args.delta, args.report, args.collection_result,
                        args.approval_result, os.environ, GitHub(os.environ.get("GITHUB_TOKEN")))
    except Rejected as exc:
        print(f"Telemetry gate blocked: {exc}", file=sys.stderr)
        return 1
    print(message)
    return 0


if __name__ == "__main__":
    sys.exit(main())
