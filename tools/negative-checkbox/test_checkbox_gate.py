import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import checkbox_gate as guard
import test_controller as fixtures
from test_checkbox_report import findings
from test_comment_report import change, metric


class API(fixtures.API):
    def post(self, path, data):
        if path.endswith("/labels"):
            self.responses["/repos/owner/repo/pulls/7"]["labels"] = [{"name": name} for name in data["labels"]]
            return []
        result = super().post(path, data)
        if path.endswith("/comments"):
            self.responses["/repos/owner/repo/issues/comments/200"]["issue_url"] = "https://api.github.com/repos/owner/repo/issues/7"
        return result

    def run(self, *args):
        assert args[1:3] == ("--method", "DELETE")
        self.responses["/repos/owner/repo/pulls/7"]["labels"].clear()


class CheckboxGateTests(unittest.TestCase):
    def setUp(self):
        f = fixtures.ControllerTests()
        f.setUp()
        self.addCleanup(f.temp.cleanup)
        f.pr["base"]["ref"] = guard.TARGET
        f.pr["labels"] = []
        f.manifest["base_ref"] = guard.TARGET
        f.delta = findings([change(after=metric("retries", [{"name": "reason", "type": "string"}]))],
                           ["- (*Client).Old: removed"])
        f.source.update(path=guard.COLLECT, name="Negative tests - checkbox collection")
        f.rebuild()
        self.f, self.api = f, API(f.responses, f.files)
        self.repo, self.prefix = "owner/repo", f.prefix
        self.state = dict(guard.identity(f.env), pr=7)
        self.actor = {"login": "maintainer", "type": "User", "id": 42}
        f.responses[f.prefix + "/commits/" + f.source["head_sha"] + "/pulls?per_page=100"] = [f.pr]
        f.responses[f.prefix + "/git/ref/heads/negative-tests%2Fcheckbox"] = {"object": {"sha": "b"*40}}
        self.listing = f.prefix + "/actions/artifacts?name=checkbox-decision-pr-7&per_page=50"
        f.responses[self.listing] = {"total_count": 0, "artifacts": []}
        for workflow in [guard.COLLECT, ".github/workflows/telemetry-contract.yml"]:
            for rev in ("b"*40, "c"*40):
                f.responses[f.prefix + f"/contents/{workflow}?ref={rev}"] = {"sha": "d"*40}
        f.responses[f.prefix + "/actions/runs/456"] = self.trusted(456, "workflow_run", guard.PUBLISH)
        f.responses[f.prefix + "/actions/runs/789"] = self.trusted(789, "issue_comment", guard.EDIT)
        self.addCleanup(patch.stopall)
        patch.object(guard, "code_version", return_value="tool contents").start()

    def trusted(self, i, event, path):
        return dict(self.f.source, id=i, event=event, path=path, actor=self.actor, head_sha="d"*40,
                    head_repository={"full_name": self.repo})

    def publish(self):
        f = self.f
        guard.publish(self.api, self.prefix, self.state, f.source, f.state_dir)
        if "comment" in self.state:
            self.api.receipts[(456, "checkbox-state-456")] = {
                p.name:p.read_bytes() for p in f.state_dir.iterdir() if p.is_file()}

    def edit(self, action):
        c = self.f.comment()
        previous = c["body"]
        text = ("✅ Approve" if action == "approve" else "❌ Reject")
        c.update(body=previous.replace("[ ] " + text, "[x] " + text), updated_at="2026-11-26T00:00:00Z")
        return {"action":"edited", "issue":{"number":7,"pull_request":{"url":"https://github.com"}},
                "sender":dict(self.actor), "repository":{"full_name":self.repo}, "comment":dict(c),
                "changes":{"body":{"from":previous}}}

    def decide(self, event):
        with tempfile.TemporaryDirectory() as folder:
            return guard.decide(self.api, self.prefix, self.repo, 789, event, Path(folder))

    def test_approve_then_reuse_after_unrelated_rebase_but_not_attribute_addition(self):
        self.publish()
        self.assertEqual(self.f.check()["state"], "pending")
        self.assertEqual(self.f.pr["labels"], [{"name":guard.LABEL}])
        event = self.edit("approve")
        state, receipt = self.decide(event)
        guard.apply(self.api, self.prefix, state, receipt)
        self.assertEqual(self.f.check()["state"], "success")
        self.assertEqual(self.f.pr["labels"], [])
        self.assertIn("Approved", self.f.comment()["body"])
        self.assertIsNone(self.decide(event))  # Redelivery cannot apply twice.
        self.api.receipts[(789, "checkbox-decision-pr-7")] = {"decision.json":json.dumps(receipt).encode()}
        self.f.responses[self.listing] = {"total_count":1, "artifacts":[{
            "name":"checkbox-decision-pr-7", "id":400, "size_in_bytes":350, "expired":False,
            "workflow_run":{"id":789,"head_sha":"d"*40}}]}
        rebased = dict(receipt["manifest"], head_sha="e"*40, merge_sha="f"*40, base_sha="1"*40)
        kwargs = self.api, self.prefix, state, rebased
        self.assertEqual(guard.reusable(*kwargs, receipt["delta"], receipt["effect"]), receipt)
        updated = findings([change(after=metric("retries",[{"name":"demo.retryable","type":"bool"}]))])
        self.assertIsNone(guard.reusable(*kwargs, updated, receipt["effect"]))
        self.assertIsNone(guard.reusable(*kwargs, receipt["delta"], "different policy"))
        self.f.responses[self.prefix+"/collaborators/maintainer/permission"]["role_name"] = "triage"
        self.assertIsNone(guard.reusable(*kwargs, receipt["delta"], receipt["effect"]))

    def test_reject_keeps_label_and_pr_open(self):
        self.publish()
        state, receipt = self.decide(self.edit("reject"))
        guard.apply(self.api, self.prefix, state, receipt)
        self.assertEqual(self.f.check()["state"], "failure")
        self.assertEqual(self.f.pr["state"],"open")
        self.assertEqual(self.f.pr["labels"],[{"name":guard.LABEL}])
        self.assertIn("Rejected", self.f.comment()["body"])

    def test_collection_error_is_terminal(self):
        self.f.delta["checks"][0].update(result="error", diff=["collector failed"])
        self.f.rebuild()
        self.api.files = self.f.files
        with self.assertRaises(guard.Rejected):
            self.publish()
        self.assertEqual(self.f.check()["state"], "failure")
        self.assertNotIn("comment", self.state)

    def test_clean_removes_indicator_and_does_not_request_review(self):
        self.f.delta = findings()
        self.f.pr["labels"] = [{"name":guard.LABEL}]
        self.f.rebuild()
        self.api.files = self.f.files
        self.publish()
        self.assertEqual(self.f.check()["state"], "success")
        self.assertEqual(self.f.pr["labels"], [])
        self.assertNotIn("comment", self.state)

    def test_manual_label_removal_cannot_approve(self):
        self.publish()
        self.f.pr["labels"] = []
        self.assertIsNone(self.decide({"action":"unlabeled"}))
        self.assertEqual(self.f.check()["state"], "pending")

    def test_foreign_comment_actor_and_edited_diff_cannot_approve(self):
        self.publish()
        event = self.edit("approve")
        event["comment"]["id"] += 1
        self.assertIsNone(self.decide(event))
        event["comment"]["id"] -= 1
        event["sender"].update(type="Bot")
        self.assertIsNone(self.decide(event))
        event["sender"].update(type="User")
        self.f.responses[self.prefix+"/collaborators/maintainer/permission"]["role_name"] = "write"
        self.assertIsNone(self.decide(event))
        self.assertEqual(self.f.comment()["body"], event["changes"]["body"]["from"])
        self.assertEqual(self.f.check()["state"], "pending")

    def test_new_head_or_newer_edit_never_approves_the_old_report(self):
        self.publish()
        event = self.edit("approve")
        self.f.comment()["updated_at"] = "2026-11-26T00:00:05Z"
        self.assertIsNone(self.decide(event))
        self.f.pr["head"]["sha"] = "9"*40
        self.f.responses[self.prefix + "/commits/" + "9"*40 + "/statuses?per_page=100&page=1"] = []
        self.assertIsNone(self.decide(event))
        self.assertEqual(self.f.check()["state"], "pending")

    def test_changed_collector_entrypoint_blocks_the_report(self):
        self.f.responses[self.prefix + f"/contents/{guard.COLLECT}?ref=" + "c"*40]["sha"] = "e"*40
        with self.assertRaises(guard.Rejected):
            self.publish()
        self.assertEqual(self.f.check()["state"], "failure")
