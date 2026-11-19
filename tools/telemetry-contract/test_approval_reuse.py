import copy
import hashlib
import json
import unittest

import approval_reuse as reuse
import controller
from gate import Rejected
import test_controller as fixtures


class ApprovalReuseTests(unittest.TestCase):
    # Reuse the integration fixture without inheriting/rerunning its test cases.
    rebuild = fixtures.ControllerTests.rebuild
    finish = fixtures.ControllerTests.finish
    check = fixtures.ControllerTests.check
    comment = fixtures.ControllerTests.comment

    def setUp(self):
        fixtures.ControllerTests.setUp(self)
        self.receipts = {}
        self.paths = [reuse.COLLECTION_WORKFLOW, reuse.REVIEW_WORKFLOW] + [reuse.TOOL + name for name in (
            "main.go", "scan.go", "go.mod", "go.sum", "gate.py", "controller.py", "approval_reuse.py")]
        for revision in ("b" * 40, "c" * 40, "d" * 40):
            self.version(revision)

    def version(self, revision, changed=None):
        tree_sha = hashlib.sha1(revision.encode()).hexdigest()
        self.responses.setdefault(self.prefix + "/git/commits/" + revision, {}).update(tree={"sha": tree_sha})
        entries = [{"path": path, "mode": "100644", "type": "blob",
                    "sha": hashlib.sha1((path + ("changed" if path == changed else "")).encode()).hexdigest()}
                   for path in self.paths]
        self.responses[f"{self.prefix}/git/trees/{tree_sha}?recursive=1"] = {
            "sha": tree_sha, "truncated": False, "tree": entries}

    def prepare(self):
        try:
            versions = reuse.context(self.api, self.prefix, self.manifest, self.env["GITHUB_WORKFLOW_SHA"])
            name = reuse.artifact_name(self.manifest, reuse.fingerprint(self.manifest, self.delta, versions))
            self.responses.setdefault(f"{self.prefix}/actions/artifacts?name={name}&per_page=20", {"artifacts": []})
        except reuse.INVALID:
            pass
        self.api.receipts = self.receipts
        return controller.prepare(self.source["id"], self.state_dir, self.env, self.api)

    def approve_original(self):
        state = self.prepare()
        self.assertTrue(state["needs_review"])
        self.finish()
        self.assertIn("state_artifact=" + state["approval_artifact"], (self.root / "outputs").read_text())
        self.original = copy.deepcopy(state)
        self.original_files = {name: (self.state_dir / name).read_bytes() for name in reuse.FILES}
        self.receipts[456, state["approval_artifact"]] = self.original_files
        self.receipt = {"id": 900, "name": state["approval_artifact"], "expired": False,
                        "size_in_bytes": 1024, "workflow_run": {"id": 456, "head_sha": "d" * 40}}
        self.responses[self.prefix + "/actions/artifacts/900"] = self.receipt
        self.responses[f"{self.prefix}/actions/artifacts?name={state['approval_artifact']}&per_page=20"] = {
            "artifacts": [self.receipt]}
        self.original_run = {"id": 456, "repository": {"full_name": "owner/repo"},
                             "head_repository": {"full_name": "owner/repo"}, "head_sha": "d" * 40,
                             "event": "workflow_run", "path": reuse.REVIEW_WORKFLOW,
                             "run_attempt": 1, "status": "completed", "conclusion": "success"}
        self.responses[self.prefix + "/actions/runs/456"] = self.original_run

    def next_commit(self, base=None, controller_sha=None):
        self.env.update(GITHUB_RUN_ID="457", GITHUB_WORKFLOW_SHA=controller_sha or "d" * 40,
                        GITHUB_OUTPUT=str(self.root / "new-outputs"))
        self.state_dir = self.root / "next-state"
        self.manifest = dict(self.manifest, head_sha="e" * 40, merge_sha="f" * 40, run_id=124)
        if base:
            self.manifest.update(base_sha=base, tool_sha=base)
        self.source = dict(self.source, id=124, head_sha="e" * 40)
        self.responses[self.prefix + "/actions/runs/124"] = self.source
        self.pr["head"]["sha"] = "e" * 40
        self.pr["base"]["sha"] = self.manifest["base_sha"]
        self.pr["merge_commit_sha"] = "f" * 40
        self.responses[self.prefix + "/git/ref/heads/master"] = {"object": {"sha": self.manifest["base_sha"]}}
        self.responses[self.prefix + "/git/commits/" + "f" * 40] = {"parents": [
            {"sha": self.manifest["base_sha"]}, {"sha": "e" * 40}]}
        for revision in (self.manifest["base_sha"], "f" * 40, self.env["GITHUB_WORKFLOW_SHA"]):
            self.version(revision)
        artifact = copy.deepcopy(self.artifact)
        artifact.update(id=301, name="telemetry-report-124-1", workflow_run={"id": 124, "head_sha": "e" * 40})
        self.responses[self.prefix + "/actions/runs/124/artifacts?per_page=100"] = {"artifacts": [artifact]}
        self.responses[self.prefix + "/actions/runs/457/approvals"] = []
        self.rebuild()

    def latest(self):
        return self.check()

    def change_evidence(self, name, change):
        value = json.loads(self.original_files[name])
        change(value)
        self.original_files[name] = json.dumps(value).encode()

    def test_new_commit_reuses_human_approval_without_new_environment_review(self):
        self.approve_original()
        self.next_commit()
        state = self.prepare()
        self.assertEqual({"artifact_id": 900, "run_id": 456, "reviewer": "maintainer"}, state["reused_approval"])
        self.assertFalse(state["needs_review"])
        self.assertEqual("in_progress", self.latest()["status"])
        self.assertIn("Matching prior approval", self.comment()["body"])
        self.assertNotIn("Review telemetry — approve or reject", self.comment()["body"])
        self.finish(approval="skipped")
        self.assertEqual("success", self.latest()["conclusion"])
        self.assertIn("Original human approval", self.comment()["body"])
        self.assertIn("actions/runs/456", self.comment()["body"])
        self.assertIn("Prior telemetry approval reused", self.comment()["body"])
        self.assertIn("state_artifact=telemetry-review-state-457-1", (self.root / "new-outputs").read_text())

    def test_unrelated_base_and_controller_commits_preserve_approval(self):
        self.approve_original()
        self.next_commit(base="1" * 40, controller_sha="2" * 40)
        self.assertFalse(self.prepare()["needs_review"])
        self.finish(approval="skipped")
        self.assertEqual("success", self.latest()["conclusion"])

    def test_changed_attribute_instrument_unit_or_before_requires_review(self):
        self.approve_original()
        self.next_commit()
        original = copy.deepcopy(self.delta)
        changes = (
            lambda d: d["changes"][0]["after"]["attributes"].append({"name": "new", "type": "bool"}),
            lambda d: d["changes"][0]["after"].update(instrument="histogram"),
            lambda d: d["changes"][0]["after"].update(unit="ms"),
            lambda d: d["changes"][0].update(action="modify", before=d["changes"][0]["after"]),
        )
        for mutate in changes:
            self.delta = copy.deepcopy(original)
            mutate(self.delta)
            self.rebuild()
            with self.subTest(delta=self.delta):
                self.assertTrue(self.prepare()["needs_review"])
                with self.assertRaisesRegex(Rejected, "not approved"):
                    self.finish(approval="skipped")

    def test_base_collector_candidate_policy_or_controller_change_requires_review(self):
        self.approve_original()
        self.next_commit()
        for revision, path in (("b" * 40, reuse.TOOL + "scan.go"), ("f" * 40, reuse.COLLECTION_WORKFLOW),
                               ("d" * 40, reuse.TOOL + "gate.py"), ("d" * 40, reuse.REVIEW_WORKFLOW)):
            self.version(revision, changed=path)
            with self.subTest(path=path):
                self.assertTrue(self.prepare()["needs_review"])
            self.version(revision)

    def test_retarget_requires_review_even_if_delta_is_identical(self):
        self.approve_original()
        self.next_commit()
        self.manifest["base_ref"] = self.pr["base"]["ref"] = "parent"
        self.responses[self.prefix + "/git/ref/heads/parent"] = {"object": {"sha": "b" * 40}}
        self.rebuild()
        self.assertTrue(self.prepare()["needs_review"])

    def test_changed_environment_review_policy_requires_new_approval(self):
        self.approve_original()
        self.next_commit()
        environment = self.responses[self.prefix + "/environments/telemetry-review"]
        environment["protection_rules"][0]["prevent_self_review"] = True
        self.assertTrue(self.prepare()["needs_review"])

    def test_fresh_human_review_works_when_optional_history_lookup_fails(self):
        self.approve_original()
        self.next_commit()
        self.responses[self.prefix + "/actions/artifacts/900"] = Rejected("unavailable")
        state = self.prepare()
        self.assertTrue(state["needs_review"])
        self.responses[self.prefix + "/actions/runs/457/approvals"] = copy.deepcopy(
            self.responses[self.prefix + "/actions/runs/456/approvals"])
        self.finish(approval="success")
        self.assertEqual("success", self.latest()["conclusion"])
        self.assertIn("state_artifact=" + state["approval_artifact"], (self.root / "new-outputs").read_text())

    def test_no_changes_still_pass_without_any_previous_approval(self):
        self.delta.update(has_changes=False, changes=[])
        self.rebuild()
        self.assertFalse(self.prepare()["needs_review"])
        self.finish(approval="skipped")
        self.assertEqual("success", self.check()["conclusion"])

    def test_expired_or_deleted_artifact_requests_manual_review(self):
        self.approve_original()
        self.next_commit()
        for replacement in (dict(self.receipt, expired=True), Rejected("deleted")):
            self.responses[self.prefix + "/actions/artifacts/900"] = replacement
            with self.subTest(replacement=replacement):
                self.assertTrue(self.prepare()["needs_review"])

    def test_missing_role_or_original_environment_approval_requests_manual_review(self):
        self.approve_original()
        self.next_commit()
        permission = self.responses[self.prefix + "/collaborators/maintainer/permission"]
        permission["role_name"] = "write"
        self.assertTrue(self.prepare()["needs_review"])
        permission["role_name"] = "maintain"
        self.responses[self.prefix + "/actions/runs/456/approvals"] = []
        self.assertTrue(self.prepare()["needs_review"])

    def test_untrusted_failed_rerun_or_foreign_workflow_cannot_supply_evidence(self):
        self.approve_original()
        self.next_commit()
        invalid = (("event", "pull_request"), ("path", ".github/workflows/other.yml"),
                   ("conclusion", "failure"), ("run_attempt", 2), ("status", "in_progress"),
                   ("repository", {"full_name": "other/repo"}),
                   ("head_repository", {"full_name": "fork/repo"}))
        for key, value in invalid:
            old = self.original_run[key]
            self.original_run[key] = value
            with self.subTest(key=key):
                self.assertTrue(self.prepare()["needs_review"])
            self.original_run[key] = old

    def test_same_named_artifact_is_not_evidence_for_other_pr_or_target(self):
        self.approve_original()
        self.next_commit()
        before = self.original_files["manifest.json"]
        for key, value in (("pr", 8), ("base_ref", "other"), ("repository", "other/repo")):
            self.change_evidence("manifest.json", lambda data: data.update({key: value}))
            with self.subTest(key=key):
                self.assertTrue(self.prepare()["needs_review"])
            self.original_files["manifest.json"] = before

    def test_auto_reused_run_does_not_create_an_approval_chain(self):
        self.approve_original()
        self.next_commit()
        self.change_evidence("state.json", lambda state: state.update(reused_approval={"run_id": 450}))
        self.assertTrue(self.prepare()["needs_review"])

    def test_original_delta_or_report_tampering_requires_manual_review(self):
        self.approve_original()
        self.next_commit()
        for name in ("delta.json", "report.md"):
            old = self.original_files[name]
            self.original_files[name] = b'{}'
            with self.subTest(name=name):
                self.assertTrue(self.prepare()["needs_review"])
            self.original_files[name] = old

    def test_forged_recorded_policy_is_checked_against_github_file_objects(self):
        self.approve_original()
        self.next_commit(base="1" * 40, controller_sha="2" * 40)
        self.version("d" * 40, changed=reuse.TOOL + "controller.py")
        # Stored hashes still claim the current policy; original code disagrees.
        self.assertTrue(self.prepare()["needs_review"])

    def test_extra_nested_or_symlink_files_cannot_be_approval_evidence(self):
        self.approve_original()
        self.next_commit()
        self.original_files["malicious.py"] = b'raise Exception()'
        self.assertTrue(self.prepare()["needs_review"])
        del self.original_files["malicious.py"]
        original_download = self.api.download
        def download(repository, run, name, destination):
            original_download(repository, run, name, destination)
            if run == 456:
                (destination / "report.md").unlink()
                (destination / "report.md").symlink_to(destination / "delta.json")
        self.api.download = download
        self.assertTrue(self.prepare()["needs_review"])

    def test_reused_approval_is_reverified_at_finalization(self):
        self.approve_original()
        self.next_commit()
        self.assertFalse(self.prepare()["needs_review"])
        self.responses[self.prefix + "/actions/runs/456/approvals"] = []
        with self.assertRaisesRegex(Rejected, "no longer reusable"):
            self.finish(approval="skipped")
        self.assertEqual("failure", self.latest()["conclusion"])

    def test_deleted_receipt_between_publish_and_final_cannot_pass(self):
        self.approve_original()
        self.next_commit()
        self.assertFalse(self.prepare()["needs_review"])
        self.responses[self.prefix + "/actions/artifacts/900"] = Rejected("deleted")
        with self.assertRaisesRegex(Rejected, "no longer reusable"):
            self.finish(approval="skipped")
        self.assertEqual("failure", self.latest()["conclusion"])

    def test_old_run_rerun_during_evidence_read_cannot_pass(self):
        self.approve_original()
        self.next_commit()
        original_get = self.api.get
        def get(path):
            value = original_get(path)
            if path.endswith("/collaborators/maintainer/permission"):
                self.original_run["run_attempt"] = 2
            return value
        self.api.get = get
        self.assertTrue(self.prepare()["needs_review"])

    def test_new_head_or_base_after_reuse_decision_is_still_stale(self):
        self.approve_original()
        self.next_commit()
        self.assertFalse(self.prepare()["needs_review"])
        self.pr["head"]["sha"] = "3" * 40
        with self.assertRaisesRegex(Rejected, "current PR head"):
            self.finish(approval="skipped")
        self.assertEqual("failure", self.latest()["conclusion"])

    def test_collection_failure_is_never_waived_by_old_approval(self):
        self.approve_original()
        self.next_commit()
        self.source["conclusion"] = "failure"
        with self.assertRaisesRegex(Rejected, "collection did not succeed"):
            self.prepare()
        self.assertEqual("failure", self.latest()["conclusion"])

    def test_optional_lookup_outage_requests_manual_review(self):
        self.approve_original()
        self.next_commit()
        name = self.original["approval_artifact"]
        self.responses[f"{self.prefix}/actions/artifacts?name={name}&per_page=20"] = Rejected("unavailable")
        self.assertTrue(self.prepare()["needs_review"])

    def test_incomplete_history_cannot_hide_a_newer_manual_decision(self):
        self.approve_original()
        self.next_commit()
        self.responses[f"{self.prefix}/actions/artifacts?name={self.receipt['name']}&per_page=20"]["total_count"] = 21
        self.assertTrue(self.prepare()["needs_review"])

    def test_truncated_code_tree_disables_reuse_and_receipt_creation(self):
        tree = next(value for key, value in self.responses.items() if "/git/trees/" in key)
        tree["truncated"] = True
        self.assertTrue(self.prepare()["needs_review"])
        self.finish()
        self.assertIn("state_artifact=telemetry-review-state-456-1", (self.root / "outputs").read_text())

    def test_newer_manual_request_blocks_resurrection_of_older_approval(self):
        self.approve_original()
        self.next_commit()
        self.env["GITHUB_RUN_ID"] = "458"
        latest = dict(self.receipt, id=901, workflow_run={"id": 457, "head_sha": "d" * 40})
        self.responses[self.prefix + "/actions/artifacts/901"] = latest
        self.responses[f"{self.prefix}/actions/artifacts?name={self.receipt['name']}&per_page=20"] = {
            "artifacts": [self.receipt, latest]}
        for status, conclusion in (("completed", "failure"), ("completed", "cancelled"), ("waiting", None)):
            self.responses[self.prefix + "/actions/runs/457"] = dict(
                self.original_run, id=457, status=status, conclusion=conclusion)
            with self.subTest(conclusion=conclusion):
                self.assertTrue(self.prepare()["needs_review"])

    def test_new_manual_request_after_reuse_lookup_blocks_finalization(self):
        self.approve_original()
        self.next_commit()
        self.assertFalse(self.prepare()["needs_review"])
        latest = dict(self.receipt, id=901, expired=True)
        self.responses[self.prefix + "/actions/artifacts/901"] = latest
        self.responses[f"{self.prefix}/actions/artifacts?name={self.receipt['name']}&per_page=20"] = {
            "artifacts": [latest, self.receipt]}
        with self.assertRaisesRegex(Rejected, "no longer reusable"):
            self.finish(approval="skipped")
        self.assertEqual("failure", self.latest()["conclusion"])

    def test_canonical_fingerprint_ignores_commit_and_order_not_delta(self):
        versions = {"version": 1, "collector": "same"}
        delta = copy.deepcopy(self.delta)
        delta["changes"][0]["after"]["attributes"].append({"name": "retryable", "type": "bool"})
        expected = reuse.fingerprint(self.manifest, delta, versions)
        delta["changes"][0]["after"]["attributes"].reverse()
        candidate = dict(self.manifest, head_sha="f" * 40, base_sha="1" * 40, run_id=900)
        self.assertEqual(expected, reuse.fingerprint(candidate, delta, versions))
        delta["changes"][0]["after"]["attributes"][0]["type"] = "string"
        self.assertNotEqual(expected, reuse.fingerprint(candidate, delta, versions))

    def test_documentation_and_test_changes_do_not_change_policy_version(self):
        revision = "b" * 40
        expected = reuse.code_version(self.api, self.prefix, revision)
        tree = self.responses[f"{self.prefix}/git/trees/{hashlib.sha1(revision.encode()).hexdigest()}?recursive=1"]
        for name in ("README.md", "test_gate.py", "main_test.go", "testdata/example.go"):
            tree["tree"].append({"path": reuse.TOOL + name, "type": "blob", "mode": "100644", "sha": "f" * 40})
        self.assertEqual(expected, reuse.code_version(self.api, self.prefix, revision))


if __name__ == "__main__":
    unittest.main()
