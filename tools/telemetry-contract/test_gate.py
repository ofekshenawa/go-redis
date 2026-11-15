import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from gate import Rejected, GitHub, approved_reviewer, current_candidate, validate_artifacts
from unittest.mock import patch
import subprocess


class FakeGitHub:
    def __init__(self, responses):
        self.responses = responses
        self.paths = []

    def get(self, path):
        self.paths.append(path)
        result = self.responses[path]
        if isinstance(result, Exception):
            raise result
        return copy.deepcopy(result)


class GateTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.manifest_file, self.delta_file, self.report_file = (
            root / "manifest.json", root / "delta.json", root / "report.md")
        self.delta = {"version": 1, "has_changes": True, "changes": [{
            "entity": "metric", "identity": "test/retries", "action": "add",
            "before": None, "after": {"name": "retries", "instrument": "counter"},
        }]}
        self.report_file.write_text("# Telemetry changes\n+ retries (counter)\n")
        self.manifest = {
            "repository": "owner/repo", "pr": 7, "base_ref": "master",
            "base_sha": "b" * 40, "head_sha": "a" * 40, "merge_sha": "c" * 40,
            "tool_sha": "d" * 40, "run_id": 123, "run_attempt": 1,
        }
        self.env = {
            "GITHUB_REPOSITORY": "owner/repo", "GITHUB_RUN_ID": "123",
            "GITHUB_RUN_ATTEMPT": "1", "GITHUB_SHA": "c" * 40,
            "TELEMETRY_TOOL_SHA": "d" * 40,
        }
        self.prefix = "/repos/owner/repo"
        self.user = {"id": 42, "login": "maintainer", "type": "User"}
        self.responses = {
            self.prefix + "/pulls/7": {
                "state": "open", "head": {"sha": "a" * 40},
                "base": {"ref": "master", "repo": {"full_name": "owner/repo"}},
                "merge_commit_sha": "c" * 40,
            },
            self.prefix + "/git/ref/heads/master": {"object": {"sha": "b" * 40}},
            self.prefix + "/environments/telemetry-review": {
                "name": "telemetry-review", "id": 10,
                "protection_rules": [{"type": "required_reviewers", "reviewers": [self.user]}],
            },
            self.prefix + "/actions/runs/123/approvals": [{
                "state": "approved", "user": self.user,
                "environments": [{"id": 10, "name": "telemetry-review"}],
            }],
            self.prefix + "/collaborators/maintainer/permission": {
                "role_name": "maintain", "permission": "write", "user": self.user,
            },
        }
        self.write_artifacts()

    def write_artifacts(self):
        self.delta_file.write_text(json.dumps(self.delta))
        self.manifest["delta_sha256"] = hashlib.sha256(self.delta_file.read_bytes()).hexdigest()
        self.manifest["report_sha256"] = hashlib.sha256(self.report_file.read_bytes()).hexdigest()
        self.manifest_file.write_text(json.dumps(self.manifest))

    def run_gate(self):
        self.api = FakeGitHub(self.responses)
        manifest, delta = validate_artifacts(self.manifest_file, self.delta_file, self.report_file, self.env)
        current_candidate(self.api, self.prefix, manifest)
        return approved_reviewer(self.api, self.prefix, manifest["run_id"])

    def test_authorized_maintainer_approval(self):
        self.assertEqual("maintainer", self.run_gate())

    def test_authorized_admin_approval(self):
        self.responses[self.prefix + "/collaborators/maintainer/permission"]["role_name"] = "admin"
        self.assertEqual("maintainer", self.run_gate())

    def test_only_builtin_admin_and_maintain_roles_can_approve(self):
        for role in ("write", "read", "triage", "custom-maintainer", ""):
            self.responses[self.prefix + "/collaborators/maintainer/permission"]["role_name"] = role
            with self.subTest(role=role), self.assertRaisesRegex(Rejected, "no telemetry-review approval"):
                self.run_gate()

    def test_bot_cannot_approve(self):
        self.user["type"] = "Bot"
        with self.assertRaisesRegex(Rejected, "no telemetry-review approval"):
            self.run_gate()

    def test_reviewer_identity_must_match_permission_response(self):
        permission = self.responses[self.prefix + "/collaborators/maintainer/permission"]
        permission["user"] = dict(self.user, id=99)
        with self.assertRaisesRegex(Rejected, "no telemetry-review approval"):
            self.run_gate()

    def test_exact_environment_id_is_required(self):
        review = self.responses[self.prefix + "/actions/runs/123/approvals"][0]
        review["environments"][0]["id"] = 999
        with self.assertRaisesRegex(Rejected, "no telemetry-review approval"):
            self.run_gate()

    def test_required_reviewers_must_be_configured(self):
        self.responses[self.prefix + "/environments/telemetry-review"]["protection_rules"] = []
        with self.assertRaisesRegex(Rejected, "required reviewers"):
            self.run_gate()

    def test_no_approval_record_or_rejection_does_not_pass(self):
        for reviews in ([], [{"state": "rejected", "user": self.user, "environments": [{"id": 10}]}]):
            self.responses[self.prefix + "/actions/runs/123/approvals"] = reviews
            with self.subTest(reviews=reviews), self.assertRaisesRegex(Rejected, "no telemetry-review approval"):
                self.run_gate()

    def test_new_head_during_wait_is_stale(self):
        self.responses[self.prefix + "/pulls/7"]["head"]["sha"] = "e" * 40
        with self.assertRaisesRegex(Rejected, "new commit"):
            self.run_gate()

    def test_advanced_base_requires_new_comparison(self):
        self.responses[self.prefix + "/git/ref/heads/master"]["object"]["sha"] = "e" * 40
        with self.assertRaisesRegex(Rejected, "Target branch advanced"):
            self.run_gate()

    def test_retargeted_stacked_pr_requires_new_comparison(self):
        self.responses[self.prefix + "/pulls/7"]["base"]["ref"] = "parent-pr"
        with self.assertRaisesRegex(Rejected, "target changed"):
            self.run_gate()

    def test_closed_pr_is_stale(self):
        self.responses[self.prefix + "/pulls/7"]["state"] = "closed"
        with self.assertRaisesRegex(Rejected, "no longer open"):
            self.run_gate()

    def test_changed_merge_candidate_is_stale(self):
        self.responses[self.prefix + "/pulls/7"]["merge_commit_sha"] = None
        with self.assertRaisesRegex(Rejected, "Merge candidate changed"):
            self.run_gate()

    def test_rerun_does_not_reuse_run_approval(self):
        for manifest_attempt, env_attempt in ((1, "2"), (2, "1"), (2, "2")):
            self.manifest["run_attempt"] = manifest_attempt
            self.env["GITHUB_RUN_ATTEMPT"] = env_attempt
            self.write_artifacts()
            with self.subTest(manifest_attempt=manifest_attempt, env_attempt=env_attempt):
                with self.assertRaisesRegex(Rejected, "Reruns cannot reuse"):
                    self.run_gate()

    def test_artifacts_belong_to_this_run_candidate_and_tool(self):
        for key, value in (("GITHUB_REPOSITORY", "other/repo"), ("GITHUB_RUN_ID", "124"),
                           ("GITHUB_SHA", "e" * 40), ("TELEMETRY_TOOL_SHA", "e" * 40)):
            original = self.env[key]
            self.env[key] = value
            with self.subTest(key=key), self.assertRaises(Rejected):
                self.run_gate()
            self.env[key] = original

    def test_changed_report_cannot_be_approved(self):
        self.report_file.write_text("# A different report\n")
        with self.assertRaisesRegex(Rejected, "report hash"):
            self.run_gate()

    def test_changed_delta_cannot_be_approved(self):
        self.delta_file.write_text('{"version":1,"has_changes":false,"changes":[]}')
        with self.assertRaisesRegex(Rejected, "delta hash"):
            self.run_gate()

    def test_inconsistent_delta_fails_closed(self):
        self.delta["has_changes"] = False
        self.write_artifacts()
        with self.assertRaisesRegex(Rejected, "Inconsistent"):
            self.run_gate()

    def test_invalid_manifest_fails_closed(self):
        for field, value in (("pr", True), ("base_sha", "not-a-sha"), ("base_ref", "")):
            original = self.manifest[field]
            self.manifest[field] = value
            self.write_artifacts()
            with self.subTest(field=field), self.assertRaisesRegex(Rejected, "Invalid"):
                self.run_gate()
            self.manifest[field] = original

    def test_github_api_failure_blocks(self):
        self.responses[self.prefix + "/actions/runs/123/approvals"] = Rejected("GitHub unavailable")
        with self.assertRaisesRegex(Rejected, "GitHub unavailable"):
            self.run_gate()

    def test_numeric_strings_from_workflow_are_accepted(self):
        for field in ("pr", "run_id", "run_attempt"):
            self.manifest[field] = str(self.manifest[field])
        self.write_artifacts()
        self.assertEqual("maintainer", self.run_gate())


class CLITests(unittest.TestCase):
    def test_json_body_is_stdin_and_token_is_environment_only(self):
        with patch("gate.subprocess.run", return_value=subprocess.CompletedProcess([], 0, '{"id":1}', "")) as run:
            result = GitHub("secret").post("/repos/owner/repo/check-runs", {"name": "literal $(not-shell)"})
        args, options = run.call_args
        self.assertEqual(result, {"id": 1})
        self.assertNotIn("secret", args[0])
        self.assertEqual(options["env"]["GH_TOKEN"], "secret")
        self.assertEqual(json.loads(options["input"]), {"name": "literal $(not-shell)"})
        self.assertNotIn("shell", options)
        self.assertIn("github.com", args[0])

    def test_cli_errors_are_redacted(self):
        with patch("gate.subprocess.run", side_effect=subprocess.CalledProcessError(1, ["gh"], stderr="private")):
            with self.assertRaisesRegex(Rejected, "GitHub CLI request failed") as error:
                GitHub("secret").get("/repos/owner/repo")
            self.assertNotIn("private", str(error.exception))

    def test_download_uses_verified_run_and_name(self):
        with patch("gate.subprocess.run", return_value=subprocess.CompletedProcess([], 0, "", "")) as run:
            GitHub("secret").download("owner/repo", 123, "telemetry-report-123-1", "/tmp/isolated")
        self.assertEqual(run.call_args.args[0], ["gh", "run", "download", "123", "--repo", "github.com/owner/repo",
                                              "--name", "telemetry-report-123-1", "--dir", "/tmp/isolated"])


if __name__ == "__main__":
    unittest.main()
