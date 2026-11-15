import copy
import unittest

from comment_report import COMMENT_PREFIX, MAX_COMMENT_CHARS, comment_marker, render_comment


def manifest(**overrides):
    value = {"repository": "owner/repo", "run_id": 123, "run_attempt": 1,
             "head_sha": "a" * 40, "delta_sha256": "d" * 64}
    return dict(value, **overrides)


def metric(name="requests", attributes=None, **overrides):
    value = {"package": "example.com/client", "name": name, "instrument": "Int64Counter",
             "unit": "{request}", "attributes": attributes or []}
    return dict(value, **overrides)


def change(entity="metric", action="add", before=None, after=None):
    return {"entity": entity, "identity": "ignored display identity", "action": action,
            "before": before, "after": after}


def delta(*changes):
    return {"version": 1, "has_changes": bool(changes), "changes": list(changes)}


class CommentReportTests(unittest.TestCase):
    def test_added_metric_links_native_approval_to_exact_run(self):
        attr = {"name": "demo.reason", "type": "string"}
        report = render_comment(manifest(), delta(change(after=metric(attributes=[attr])),
                               change("attribute_declaration", after=attr)))
        for expected in ("```diff", "+ metric: requests", "+ instrument: Int64Counter",
                         "+ unit: {request}", "+ attribute/label: demo.reason (string)",
                         "**Source head:** `aaaaaaaaaaaa`", "**Collection run:** [123]",
                         "[Review telemetry — approve or reject](https://github.com/owner/repo/actions/runs/123)",
                         "**Review deployments**", "**telemetry-review**", "does not approve automatically",
                         "Full PR merge candidate versus its target base", "not the previous PR commit"):
            self.assertIn(expected, report)
        self.assertEqual(report.count("demo.reason"), 1)
        self.assertNotIn("Other attribute declarations", report)
        self.assertTrue(report.startswith(comment_marker(manifest())))
        self.assertTrue(report.startswith(COMMENT_PREFIX))

    def test_removed_metrics_include_all_definitions_and_attributes(self):
        report = render_comment(manifest(), delta(
            change(action="remove", before=metric(attributes=[{"name": "status", "type": "string"}])),
            change(action="remove", before=metric("latency", instrument="Float64Histogram", unit="s"))))
        for expected in ("- metric: requests", "- metric: latency", "- instrument: Float64Histogram",
                         "- unit: s", "- attribute/label: status (string)"):
            self.assertIn(expected, report)
        self.assertNotIn("+ metric:", report)

    def test_modified_metric_shows_context_and_type_before_after(self):
        before = metric(attributes=[{"name": "status", "type": "string"}, {"name": "same", "type": "bool"}])
        after = metric(attributes=[{"name": "status", "type": "int64"}, {"name": "same", "type": "bool"}])
        report = render_comment(manifest(), delta(change(action="modify", before=before, after=after)))
        for expected in ("  metric: requests", "  instrument: Int64Counter", "  unit: {request}",
                         "- attribute/label: status (string)", "+ attribute/label: status (int64)"):
            self.assertIn(expected, report)
        self.assertNotIn("attribute/label: same", report)

    def test_modified_instrument_unit_and_external_attributes_are_visible(self):
        before = metric()
        after = metric(instrument="Float64Histogram", unit="s", external_attributes=True)
        report = render_comment(manifest(), delta(change(action="modify", before=before, after=after)))
        for expected in ("- instrument: Int64Counter", "+ instrument: Float64Histogram", "- unit: {request}",
                         "+ unit: s", "- caller-provided attributes: no", "+ caller-provided attributes: yes"):
            self.assertIn(expected, report)

    def test_unattached_declarations_remain_visible(self):
        report = render_comment(manifest(), delta(change("attribute_declaration", action="remove",
                               before={"name": "orphan", "type": "string"})))
        self.assertIn("Other attribute declarations", report)
        self.assertIn("- attribute declaration: orphan (string)", report)

    def test_hostile_source_cannot_close_fences_create_mentions_or_html(self):
        hostile = "x\n```\n@reviewer\r\n<script>[evil](https://evil.example)\u202e\u2028"
        report = render_comment(manifest(), delta(change(after=metric(hostile, [{"name": hostile, "type": "string"}]))))
        self.assertEqual(sum(line.startswith("```") for line in report.splitlines()), 2)
        for forbidden in ("@reviewer", "<script>", "\u202e", "\u2028", "\r"):
            self.assertNotIn(forbidden, report)
        for expected in (r"\n\u0060\u0060\u0060\n\u0040reviewer", r"\u003cscript\u003e", r"\u202e", r"\u2028"):
            self.assertIn(expected, report)

    def test_large_report_is_explicitly_truncated_without_unbalanced_fences(self):
        attrs = [{"name": f"attribute.{i:04d}." + "x" * 100, "type": "string"} for i in range(1000)]
        report = render_comment(manifest(), delta(change(after=metric(attributes=attrs))))
        self.assertLessEqual(len(report), MAX_COMMENT_CHARS)
        self.assertLess(len(report), 60000)
        self.assertIn("**Report truncated:**", report)
        self.assertIn("[full report and artifacts](https://github.com/owner/repo/actions/runs/123)", report)
        self.assertIn("[Review telemetry — approve or reject]", report)
        self.assertEqual(sum(line.startswith("```") for line in report.splitlines()) % 2, 0)
        self.assertIn("attribute.0000", report)
        self.assertNotIn("attribute.0999", report)

    def test_single_oversized_line_still_preserves_marker_and_approval_link(self):
        report = render_comment(manifest(), delta(change(after=metric("x" * 100000))))
        self.assertLess(len(report), 60000)
        self.assertTrue(report.startswith(comment_marker(manifest())))
        self.assertIn("Report truncated", report)
        self.assertIn("Review telemetry — approve or reject", report)

    def test_deterministic_regardless_of_change_and_attribute_order(self):
        first = change(after=metric("z", [{"name": "z", "type": "bool"}, {"name": "a", "type": "string"}]))
        second = change(after=metric("a"))
        original = delta(first, second)
        reordered = copy.deepcopy(original)
        reordered["changes"].reverse()
        reordered["changes"][1]["after"]["attributes"].reverse()
        self.assertEqual(render_comment(manifest(), original), render_comment(manifest(), reordered))

    def test_controller_run_link_does_not_change_source_run_marker(self):
        source = manifest()
        controller = manifest(approval_run_id=456)
        self.assertEqual(comment_marker(source), comment_marker(controller))
        report = render_comment(controller, delta(change(after=metric())))
        self.assertIn("**Collection run:** [123](https://github.com/owner/repo/actions/runs/123)", report)
        self.assertIn("**Approval run:** [456](https://github.com/owner/repo/actions/runs/456)", report)
        self.assertIn("[Review telemetry — approve or reject](https://github.com/owner/repo/actions/runs/456)", report)

    def test_status_update_preserves_identity_but_new_run_attempt_or_delta_does_not(self):
        d = delta(change(after=metric()))
        marker = comment_marker(manifest())
        for status in ("pending", "approved", "rejected", "stale", "cancelled", "failed"):
            self.assertTrue(render_comment(manifest(), d, status).startswith(marker))
        for other in (manifest(run_id=124), manifest(run_attempt=2), manifest(delta_sha256="e" * 64)):
            self.assertNotEqual(marker, comment_marker(other))
        self.assertNotIn("Review telemetry — approve or reject", render_comment(manifest(), d, "stale"))

    def test_empty_delta_needs_no_approval(self):
        report = render_comment(manifest(), delta())
        self.assertIn("No telemetry changes; no telemetry approval needed", report)
        self.assertNotIn("Review telemetry — approve or reject", report)
        self.assertNotIn("```diff", report)

    def test_invalid_identity_or_delta_cannot_forge_comment_links(self):
        d = delta(change(after=metric()))
        for invalid in (manifest(repository="owner/repo)\nhttps://evil"), manifest(run_id="123"),
                        manifest(run_id=True), manifest(head_sha="x" * 40), manifest(delta_sha256="d" * 63),
                        manifest(approval_run_id=-1)):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                render_comment(invalid, d)
        for invalid in ({"version": 2, "has_changes": False, "changes": []},
                        {"version": 1, "has_changes": False, "changes": [d["changes"][0]]},
                        delta(change(action="remove", after=metric())),
                        delta(change(after=metric(attributes=[{"name": "bad", "type": None}])))):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                render_comment(manifest(), invalid)
        with self.assertRaises(ValueError):
            render_comment(manifest(), d, "invented")


if __name__ == "__main__":
    unittest.main()
