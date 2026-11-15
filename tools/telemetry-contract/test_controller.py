import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
import zipfile

import controller
from gate import Rejected


class API:
    def __init__(self, responses, archive):
        self.responses = responses
        self.archive = archive
        self.writes = []
        self.downloads = []

    def get(self, path):
        response = self.responses[path]
        if isinstance(response, Exception):
            raise response
        return copy.deepcopy(response)

    def post(self, path, data):
        self.writes.append(("POST", path, copy.deepcopy(data)))
        if path.endswith("/check-runs"):
            result = dict(data, id=100, app={"id": controller.CHECK_APP_ID})
            self.responses[path + "/100"] = result
        else:
            result = dict(data, id=200, html_url="https://github.com/owner/repo/pull/7#issuecomment-200",
                          user={"login": "github-actions[bot]", "type": "Bot"})
            self.responses["/repos/owner/repo/issues/comments/200"] = result
            self.responses[path + "?per_page=100&page=1"].append(result)
        return copy.deepcopy(result)

    def patch(self, path, data):
        self.writes.append(("PATCH", path, copy.deepcopy(data)))
        self.responses[path].update(copy.deepcopy(data))
        return copy.deepcopy(self.responses[path])

    def artifact(self, prefix, artifact_id):
        self.downloads.append((prefix, artifact_id))
        return self.archive


def zipped(files):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return stream.getvalue()


class ControllerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state_dir = self.root / "state"
        self.env = {"GITHUB_REPOSITORY": "owner/repo", "GITHUB_RUN_ID": "456",
                    "GITHUB_RUN_ATTEMPT": "1", "GITHUB_WORKFLOW_SHA": "d" * 40,
                    "GITHUB_OUTPUT": str(self.root / "outputs")}
        self.manifest = {"repository": "owner/repo", "pr": 7, "base_ref": "master", "base_sha": "b" * 40,
                         "head_sha": "a" * 40, "merge_sha": "c" * 40, "tool_sha": "b" * 40,
                         "run_id": 123, "run_attempt": 1}
        self.delta = {"version": 1, "has_changes": True, "changes": [{
            "entity": "metric", "identity": "redis/retries", "action": "add", "before": None,
            "after": {"package": "redis", "name": "retries", "instrument": "counter",
                      "unit": "1", "attributes": [{"name": "reason", "type": "string"}]},
        }]}
        self.prefix = "/repos/owner/repo"
        self.source = {"id": 123, "repository": {"id": 1, "full_name": "owner/repo"}, "head_sha": "a" * 40,
                       "head_repository": {"id": 2, "full_name": "fork/repo"}, "run_attempt": 1,
                       "event": "pull_request", "path": controller.WORKFLOW, "name": "Telemetry contract",
                       "status": "completed", "conclusion": "success", "pull_requests": []}
        self.pr = {"number": 7, "state": "open", "head": {"sha": "a" * 40, "repo": {"id": 2}},
                   "base": {"ref": "master", "sha": "b" * 40, "repo": {"full_name": "owner/repo", "id": 1}},
                   "merge_commit_sha": "c" * 40}
        self.artifact = {"id": 300, "name": "telemetry-report-123-1", "expired": False, "size_in_bytes": 1024,
                         "workflow_run": {"id": 123, "head_sha": "a" * 40},
                         "archive_download_url": "https://evil.invalid/ignored"}
        user = {"id": 42, "login": "maintainer", "type": "User"}
        self.responses = {
            self.prefix + "/actions/runs/123": self.source,
            self.prefix + "/actions/runs/123/artifacts?per_page=100": {"artifacts": [self.artifact]},
            self.prefix + "/pulls/7": self.pr,
            self.prefix + "/git/ref/heads/master": {"object": {"sha": "b" * 40}},
            self.prefix + "/git/commits/" + "c" * 40: {"parents": [{"sha": "b" * 40}, {"sha": "a" * 40}]},
            self.prefix + "/issues/7/comments?per_page=100&page=1": [],
            self.prefix + "/environments/telemetry-review": {"name": "telemetry-review", "id": 10,
                 "protection_rules": [{"type": "required_reviewers", "reviewers": [{"type": "User", "reviewer": user}]}]},
            self.prefix + "/actions/runs/456/approvals": [{"state": "approved", "environments": [{"id": 10}], "user": user}],
            self.prefix + "/collaborators/maintainer/permission": {"role_name": "maintain", "user": user},
        }
        self.rebuild()

    def rebuild(self):
        report = b"# Telemetry changes\n+ retries\n"
        delta = json.dumps(self.delta).encode()
        self.manifest.update(report_sha256=hashlib.sha256(report).hexdigest(), delta_sha256=hashlib.sha256(delta).hexdigest())
        self.files = {"manifest.json": json.dumps(self.manifest).encode(), "delta.json": delta,
                      "report.md": report, "base.json": b"{}", "candidate.json": b"{}", "review.txt": b"retries"}
        self.api = API(self.responses, zipped(self.files))

    def prepare(self):
        return controller.prepare(123, self.state_dir, self.env, self.api)

    def finish(self, publication="success", approval="success"):
        return controller.finish(self.state_dir, publication, approval, self.env, self.api)

    def check(self):
        return self.responses[self.prefix + "/check-runs/100"]

    def comment(self):
        return self.responses[self.prefix + "/issues/comments/200"]

    def test_fork_report_is_published_before_approval_with_controller_link(self):
        state = self.prepare()
        self.assertTrue(state["validated"])
        self.assertEqual("in_progress", self.check()["status"])
        self.assertEqual("a" * 40, self.check()["head_sha"])
        self.assertIn("actions/runs/456", self.comment()["body"])
        self.assertIn("Waiting for telemetry approval", self.comment()["body"])
        self.assertEqual([(self.prefix, 300)], self.api.downloads)
        self.assertIn("has_changes=true", (self.root / "outputs").read_text())

    def test_nonfork_source_association_is_checked_and_passes(self):
        self.source["head_repository"]["id"] = 1
        self.pr["head"]["repo"]["id"] = 1
        self.source["pull_requests"] = [{"number": 7, "head": {"sha": "a" * 40}}]
        self.assertTrue(self.prepare()["validated"])

    def test_changed_approved_report_becomes_green_and_updates_comment(self):
        self.prepare()
        self.assertIn("passed", self.finish())
        self.assertEqual("success", self.check()["conclusion"])
        self.assertIn("Telemetry approved for this run", self.comment()["body"])

    def test_unchanged_passes_without_comment_or_review(self):
        self.delta.update(has_changes=False, changes=[])
        self.rebuild()
        self.assertFalse(self.prepare()["has_changes"])
        self.finish(approval="skipped")
        self.assertEqual("success", self.check()["conclusion"])
        self.assertNotIn(self.prefix + "/issues/comments/200", self.responses)

    def test_failed_source_is_not_approvable(self):
        self.source["conclusion"] = "failure"
        with self.assertRaisesRegex(Rejected, "collection did not succeed"):
            self.prepare()
        self.assertEqual("failure", self.check()["conclusion"])
        self.assertFalse(json.loads((self.state_dir / "state.json").read_text())["validated"])
        self.assertFalse(self.api.downloads)

    def test_wrong_workflow_path_or_event_fails(self):
        for key, value in (("path", ".github/workflows/evil.yml"), ("event", "push")):
            previous = self.source[key]
            self.source[key] = value
            with self.subTest(key=key), self.assertRaisesRegex(Rejected, "Unexpected collection"):
                self.prepare()
            self.assertEqual("failure", self.check()["conclusion"])
            self.source[key] = previous

    def test_source_or_controller_rerun_is_rejected(self):
        self.source["run_attempt"] = 2
        with self.assertRaisesRegex(Rejected, "reruns"):
            self.prepare()
        self.source["run_attempt"] = 1
        self.env["GITHUB_RUN_ATTEMPT"] = "2"
        with self.assertRaisesRegex(Rejected, "reruns"):
            self.prepare()

    def test_other_pr_association_fails(self):
        self.source["pull_requests"] = [{"number": 8, "head": {"sha": "a" * 40}}]
        with self.assertRaisesRegex(Rejected, "another PR"):
            self.prepare()

    def test_fork_identity_mismatch_fails(self):
        self.source["head_repository"]["id"] = 99
        with self.assertRaisesRegex(Rejected, "including its fork"):
            self.prepare()

    def test_source_head_mismatch_fails(self):
        self.pr["head"]["sha"] = "e" * 40
        with self.assertRaisesRegex(Rejected, "current PR head"):
            self.prepare()

    def test_missing_expired_or_other_run_artifact_fails(self):
        for key, value in (("name", "different"), ("expired", True), ("workflow_run", {"id": 999, "head_sha": "a" * 40})):
            original = self.artifact[key]
            self.artifact[key] = value
            with self.subTest(key=key), self.assertRaises(Rejected):
                self.prepare()
            self.artifact[key] = original

    def test_zip_traversal_and_unexpected_files_are_rejected(self):
        for name in ("../escape.py", "folder/report.md", "code.py"):
            self.api.archive = zipped(dict(self.files, **{name: b"not executable"}))
            with self.subTest(name=name), self.assertRaisesRegex(Rejected, "unexpected files"):
                self.prepare()
        self.assertFalse((self.root / "escape.py").exists())

    def test_zip_uncompressed_limit_is_enforced(self):
        self.files["base.json"] = b"x" * 2048
        self.api.archive = zipped(self.files)
        self.artifact["size_in_bytes"] = 100
        with patch.object(controller, "MAX_BYTES", 1500):
            with self.assertRaisesRegex(Rejected, "Uncompressed artifact"):
                self.prepare()

    def test_report_hash_mismatch_fails_before_comment(self):
        self.files["report.md"] = b"forged report"
        self.api.archive = zipped(self.files)
        with self.assertRaisesRegex(Rejected, "report hash"):
            self.prepare()
        self.assertNotIn(self.prefix + "/issues/comments/200", self.responses)

    def test_tool_must_equal_exact_base(self):
        self.manifest["tool_sha"] = "f" * 40
        self.rebuild()
        with self.assertRaisesRegex(Rejected, "trusted tool"):
            self.prepare()

    def test_merge_parents_must_be_base_then_head(self):
        self.responses[self.prefix + "/git/commits/" + "c" * 40]["parents"].reverse()
        with self.assertRaisesRegex(Rejected, "Merge parents"):
            self.prepare()

    def test_new_head_while_waiting_blocks_and_marks_stale(self):
        self.prepare()
        self.pr["head"]["sha"] = "e" * 40
        with self.assertRaisesRegex(Rejected, "current PR head"):
            self.finish()
        self.assertEqual("failure", self.check()["conclusion"])
        self.assertIn("outdated", self.comment()["body"])

    def test_advanced_base_while_waiting_blocks(self):
        self.prepare()
        self.responses[self.prefix + "/git/ref/heads/master"]["object"]["sha"] = "e" * 40
        with self.assertRaisesRegex(Rejected, "Target branch advanced"):
            self.finish()
        self.assertEqual("failure", self.check()["conclusion"])

    def test_missing_reviewer_permission_blocks(self):
        self.prepare()
        self.responses[self.prefix + "/collaborators/maintainer/permission"]["role_name"] = "write"
        with self.assertRaisesRegex(Rejected, "no telemetry-review approval"):
            self.finish()
        self.assertEqual("failure", self.check()["conclusion"])

    def test_approval_is_from_controller_not_collection_run(self):
        self.prepare()
        self.responses[self.prefix + "/actions/runs/456/approvals"] = []
        with self.assertRaisesRegex(Rejected, "no telemetry-review approval"):
            self.finish()

    def test_rejected_cancelled_or_pending_cannot_pass(self):
        self.prepare()
        for result in ("failure", "cancelled", "skipped", "pending"):
            with self.subTest(result=result), self.assertRaisesRegex(Rejected, "not approved"):
                self.finish(approval=result)
            self.assertEqual("failure", self.check()["conclusion"])

    def test_failed_publication_blocks_even_with_approval(self):
        self.prepare()
        with self.assertRaisesRegex(Rejected, "publication"):
            self.finish(publication="cancelled")
        self.assertEqual("failure", self.check()["conclusion"])

    def test_other_controller_cannot_finalize(self):
        self.prepare()
        self.env["GITHUB_RUN_ID"] = "999"
        with self.assertRaisesRegex(Rejected, "another run"):
            self.finish()
        self.assertEqual("in_progress", self.check()["status"])

    def test_wrong_external_id_is_not_overwritten(self):
        self.prepare()
        self.check()["external_id"] = "another-controller"
        with self.assertRaisesRegex(Rejected, "Check identity"):
            self.finish()
        self.assertEqual("in_progress", self.check()["status"])

    def test_other_comment_identity_is_not_overwritten(self):
        self.prepare()
        self.comment()["body"] = "An unrelated comment"
        with self.assertRaisesRegex(Rejected, "Comment identity"):
            self.finish()
        self.assertEqual("An unrelated comment", self.comment()["body"])
        self.assertEqual("failure", self.check()["conclusion"])

    def test_comment_from_similar_controller_run_is_not_overwritten(self):
        self.prepare()
        self.comment()["body"] = self.comment()["body"].replace("actions/runs/456", "actions/runs/4567")
        with self.assertRaisesRegex(Rejected, "Comment identity"):
            self.finish()
        self.assertIn("actions/runs/4567", self.comment()["body"])

    def test_repeated_prepare_updates_own_comment(self):
        self.prepare()
        self.prepare()
        comments = [item for item in self.api.writes if item[0] == "POST" and item[1].endswith("/comments")]
        self.assertEqual(1, len(comments))

    def test_artifact_redirect_never_forwards_token(self):
        class Response:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def read(self, limit):
                return b"zip-data"

        requests = []
        class Opener:
            def open(self, request, timeout):
                requests.append(request)
                if len(requests) == 1:
                    raise HTTPError(request.full_url, 302, "Found", {"Location": "https://blob.example/signed?sig=secret"}, None)
                return Response()

        api = controller.GitHub("test-token")
        api.opener = Opener()
        self.assertEqual(b"zip-data", api.artifact(self.prefix, 300))
        self.assertEqual("Bearer test-token", requests[0].get_header("Authorization"))
        self.assertIsNone(requests[1].get_header("Authorization"))


if __name__ == "__main__":
    unittest.main()
