import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from approval_reuse import fingerprint
from comment_report import render_comment
from gate import Rejected, validate_rows
from multi_report import collect_rows
from test_comment_report import manifest


class MultiReportTests(unittest.TestCase):
    def test_both_checks_share_one_decision_and_fingerprint(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            api = root / "api.txt"
            api.write_text("Client.LegacyMethod: removed\n")
            attr = {"name": "reason", "type": "string"}
            otel = {"version": 1, "has_changes": True, "changes": [
                {"entity": "attribute_declaration", "identity": "reason", "action": "add", "before": None, "after": attr}]}
            (root / "delta.json").write_text(json.dumps(otel))
            collect_rows(root, api, "success", "success")
            delta = json.loads((root / "delta.json").read_text())
            validate_rows(delta)
            body = render_comment(manifest(), delta)
            self.assertIn("reason", body)
            self.assertIn("Client.LegacyMethod", body)
            self.assertEqual(1, body.count("Approve or reject this report"))
            m = dict(manifest(), pr=1, base_ref="negative-tests/environment")
            prior = fingerprint(m, delta, {})
            delta["checks"][1]["diff"].append("Client.Other: removed")
            self.assertNotEqual(prior, fingerprint(m, delta, {}))
            delta["checks"][0]["result"] = "pass"
            with self.assertRaises(Rejected):
                validate_rows(delta)

    def test_error_and_missing_row_cannot_pass(self):
        with TemporaryDirectory() as root:
            root = Path(root)
            api = root / "api.txt"
            api.write_text("")
            collect_rows(root, api, "failure", "success")
            delta = json.loads((root / "delta.json").read_text())
            self.assertEqual("error", delta["checks"][0]["result"])
            with self.assertRaises(Rejected):
                validate_rows(delta)
            delta["checks"].pop(0)
            with self.assertRaises(Rejected):
                validate_rows(delta)
