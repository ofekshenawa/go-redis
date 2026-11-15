"""Regression tests for binding report artifacts to a fresh PR candidate."""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import prepare


class PrepareTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.report = self.root / "report"
        self.report.mkdir()
        self.report_bytes = b"# Telemetry changes\n\n+ metric: requests\n"
        self.delta_bytes = b'{"version":1,"has_changes":true,"changes":[]}\n'
        (self.report / "report.md").write_bytes(self.report_bytes)
        (self.report / "delta.json").write_bytes(self.delta_bytes)
        self.base = "a" * 40
        self.head = "b" * 40
        self.merge = "c" * 40
        self.event = self.root / "event.json"
        self.event.write_text(json.dumps({"pull_request": {
            "number": 12,
            "base": {"sha": self.base, "ref": "demo/base"},
            "head": {"sha": self.head},
        }}))
        self.summary = self.root / "summary.md"
        self.env = {
            "GITHUB_EVENT_PATH": str(self.event),
            "GITHUB_RUN_ATTEMPT": "1", "GITHUB_SHA": self.merge,
            "GITHUB_REPOSITORY": "example/client", "GITHUB_RUN_ID": "456",
            "GITHUB_STEP_SUMMARY": str(self.summary),
        }

    def run_prepare(self, parents=None, tool=None):
        parents = [self.base, self.head] if parents is None else parents
        tool = self.base if tool is None else tool
        with patch.dict(os.environ, self.env, clear=True), patch.object(
            prepare.subprocess, "check_output",
            side_effect=[" ".join(parents) + "\n", tool + "\n"],
        ) as git:
            prepare.prepare("base", "candidate", str(self.report))
        return git

    def test_manifest_binds_exact_artifact_bytes_and_candidate(self):
        git = self.run_prepare()
        manifest = json.loads((self.report / "manifest.json").read_text())
        self.assertEqual(manifest, {
            "repository": "example/client", "pr": 12,
            "base_ref": "demo/base", "base_sha": self.base,
            "head_sha": self.head, "merge_sha": self.merge, "tool_sha": self.base,
            "run_id": 456, "run_attempt": 1,
            "report_sha256": hashlib.sha256(self.report_bytes).hexdigest(),
            "delta_sha256": hashlib.sha256(self.delta_bytes).hexdigest(),
        })
        self.assertEqual(git.call_args_list[0].args[0],
                         ["git", "-C", "candidate", "show", "-s", "--format=%P", self.merge])
        summary = self.summary.read_text()
        self.assertTrue(summary.startswith(self.report_bytes.decode()))
        for value in (self.base, self.head, self.merge, manifest["report_sha256"],
                      "Review deployments", "telemetry-review"):
            self.assertIn(value, summary)

    def test_rerun_rejected_before_manifest_or_summary(self):
        self.env["GITHUB_RUN_ATTEMPT"] = "2"
        with self.assertRaisesRegex(ValueError, "Re-runs are not supported"):
            self.run_prepare()
        self.assertFalse((self.report / "manifest.json").exists())
        self.assertFalse(self.summary.exists())

    def test_rejects_stale_base_head_and_non_merge_commits(self):
        for parents in (["d" * 40, self.head], [self.base, "d" * 40],
                        [self.base], [self.head, self.base]):
            with self.subTest(parents=parents), self.assertRaisesRegex(
                ValueError, "merge candidate does not match"
            ):
                self.run_prepare(parents=parents)
        self.assertFalse((self.report / "manifest.json").exists())

    def test_rejects_collector_from_different_commit(self):
        with self.assertRaisesRegex(ValueError, "exact base commit"):
            self.run_prepare(tool="d" * 40)
        self.assertFalse((self.report / "manifest.json").exists())

    def test_missing_report_does_not_produce_approvable_manifest(self):
        (self.report / "report.md").unlink()
        with self.assertRaises(FileNotFoundError):
            self.run_prepare()
        self.assertFalse((self.report / "manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
