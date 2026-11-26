#!/usr/bin/env python3
"""Shared report/reviewer validation and a small adapter for the GitHub CLI."""

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
from urllib.parse import quote


ENVIRONMENT = "telemetry-review"


class Rejected(Exception):
    """A check that must not become green."""


def require(condition, reason):
    """Raise Rejected with the supplied reason when a gate condition fails."""
    if not condition:
        raise Rejected(reason)


def number(value, field):
    """Parse a positive integer ID or attempt number, rejecting booleans and malformed values."""
    require(not isinstance(value, bool) and str(value).isdigit(), f"Invalid {field}.")
    result = int(value)
    require(result > 0, f"Invalid {field}.")
    return result


def sha(value, length, field):
    """Validate a lowercase hexadecimal Git object ID or digest of the expected length."""
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{%d}" % length, value),
            f"Invalid {field}.")
    return value


def json_file(path):
    """Read a size-limited artifact and return its parsed JSON and original bytes for hashing."""
    try:
        data = Path(path).read_bytes()
        require(len(data) <= 20 * 1024 * 1024, "Artifact is too large.")
        return json.loads(data), data
    except (OSError, UnicodeError, ValueError) as exc:
        raise Rejected("Missing or invalid JSON artifact.") from exc


class GitHub:
    """Delegate API authentication, HTTP and artifact downloads to GitHub CLI."""
    def __init__(self, token):
        """Configure noninteractive GitHub CLI calls with the workflow's token."""
        require(bool(token), "GITHUB_TOKEN is required.")
        self.env = dict(os.environ, GH_TOKEN=token, GH_HOST="github.com", GH_PROMPT_DISABLED="1")

    def run(self, *args, data=None):
        """Run a GitHub CLI command and return stdout, keeping CLI error output out of the review."""
        try:
            result = subprocess.run(["gh", *args], input=data, text=True, capture_output=True,
                                    env=self.env, timeout=120, check=True)
            return result.stdout
        except (subprocess.SubprocessError, OSError):
            # CLI stderr may include response data; do not expose it in the Check.
            raise Rejected("GitHub CLI request failed or timed out.") from None

    def request(self, method, path, data=None):
        """Send a repository API request via the GitHub CLI and decode its JSON response."""
        require(path.startswith("/repos/") and not path.startswith("//"), "Invalid API path.")
        args = ["api", "--hostname", "github.com", "--method", method, path,
                "-H", "X-GitHub-Api-Version: 2022-11-28"]
        if data is not None:
            args.extend(["--input", "-"])
        try:
            return json.loads(self.run(*args, data=None if data is None else json.dumps(data)))
        except ValueError:
            raise Rejected("Invalid GitHub API response.") from None

    def get(self, path):
        """Fetch a repository API resource and return its decoded JSON."""
        return self.request("GET", path)

    def post(self, path, data):
        """Send a JSON payload to a repository API endpoint and return its response."""
        return self.request("POST", path, data)

    def patch(self, path, data):
        """Update a repository API resource and return its decoded response."""
        return self.request("PATCH", path, data)

    def download(self, repository, source_id, name, destination):
        """Extract one named artifact from the specified workflow run into the destination directory."""
        self.run("run", "download", str(source_id), "--repo", "github.com/" + repository,
                 "--name", name, "--dir", str(destination))


def validate_artifacts(manifest_path, delta_path, report_path, env):
    """Return the manifest and delta after verifying their run identity and the report hashes.

    Check the applicable report format, including required test results for multi-check
    targets. A collection error makes the report ineligible for approval.
    """
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
    multi_target = manifest["base_ref"] in ("negative-tests/environment", "negative-tests/label", "negative-tests/reaction")
    if isinstance(delta, dict) and delta.get("version") == 2:
        validate_rows(delta)
    else:
        require(not multi_target, "This target requires both negative tests.")
        validate_telemetry(delta)
    return manifest, delta


def validate_rows(delta):
    """Require both configured checks and verify that each outcome agrees with its findings.

    An error from either check blocks the report even if the other check passed.
    """
    rows = delta.get("checks")
    require(type(delta.get("has_changes")) is bool and isinstance(rows, list)
            and all(isinstance(row, dict) for row in rows)
            and [row.get("name") for row in rows] == ["OTel", "Public API"],
            "Missing, duplicate or unknown negative tests.")
    for row in rows:
        name, outcome, diff = row["name"], row.get("result"), row.get("diff")
        require(outcome != "error", f"{name} collection failed; see the collection log.")
        require(isinstance(diff, list) and outcome == ("needs_review" if diff else "pass"),
                f"Inconsistent {name} result.")
        if name == "OTel":
            validate_changes(diff)
        else:
            require(all(isinstance(line, str) and line.strip() and len(line.splitlines()) == 1 for line in diff),
                    "Invalid public API diff.")
    require(delta["has_changes"] == any(row["result"] == "needs_review" for row in rows),
            "Inconsistent aggregate result.")


def validate_telemetry(delta):
    """Validate the original telemetry-only report and its summary of whether anything changed."""
    require(isinstance(delta, dict) and delta.get("version") == 1
            and isinstance(delta.get("has_changes"), bool)
            and isinstance(delta.get("changes"), list), "Invalid telemetry delta.")
    require(delta["has_changes"] == bool(delta["changes"]), "Inconsistent telemetry delta.")
    validate_changes(delta["changes"])


def validate_changes(changes):
    """Require each telemetry finding to identify an entity, action, and before/after fields."""
    for change in changes:
        require(isinstance(change, dict)
                and change.get("entity") in ("metric", "attribute_declaration")
                and isinstance(change.get("identity"), str) and change["identity"]
                and change.get("action") in ("add", "remove", "modify")
                and "before" in change and "after" in change,
                "Invalid telemetry change.")



def current_candidate(api, prefix, manifest):
    """Require the live PR head, target branch, base tip, and merge candidate to match the report.

    A push, retarget, or base update can make the collected candidate stale while a
    reviewer is reading the findings.
    """
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
    """Return the login of an authorized human who approved this run's protected Environment.

    Match the Environment ID in GitHub's review history and check the reviewer's
    current repository role. Reject the decision if no matching approval qualifies.
    """
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
        login = reviewer_login(api, prefix, review.get("user", {}))
        if login:
            return login
    raise Rejected("This run has no telemetry-review approval from a repository admin or maintainer.")


def reviewer_login(api, prefix, user):
    """Return a confirmed admin or maintainer login, or None if the identity or role does not match."""
    if not isinstance(user, dict) or user.get("type") != "User":
        return None
    login = user.get("login")
    if not isinstance(login, str) or not re.fullmatch(r"[A-Za-z0-9-]+", login):
        return None
    identity = user.get("id")
    if type(identity) is not int or identity <= 0:
        return None
    permission = api.get(f"{prefix}/collaborators/{quote(login, safe='')}/permission")
    if not isinstance(permission, dict):
        return None
    actor = permission.get("user", {})
    if (permission.get("role_name") in ("admin", "maintain")
            and isinstance(actor, dict) and actor.get("type") == "User"
            and actor.get("id") == identity and actor.get("login") == login):
        return login
    return None
