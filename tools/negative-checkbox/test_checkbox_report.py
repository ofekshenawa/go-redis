import unittest

from checkbox_report import ACTIONS, edited_decision, render
from gate import Rejected
from test_comment_report import change, manifest, metric


def findings(otel=None, api=None):
    otel = otel or []
    api = api or []
    return {"version": 2, "has_changes": bool(otel or api), "checks": [
        {"name": name, "result": "needs_review" if diff else "pass", "diff": diff}
        for name, diff in (("OTel", otel), ("Public API", api))
    ]}


class CheckboxReportTests(unittest.TestCase):
    def setUp(self):
        key = {"name": "demo.reason", "type": "string"}
        metric_added = change(after=metric("redis.client.demo.retries", [key]))
        self.delta = findings([metric_added, change("attribute_declaration", after=key)],
                              ["- (*Client).NewExplainBuilder: removed"])
        self.body = render(manifest(), self.delta)

    def test_reports_both_real_diffs_and_one_choice_for_the_whole_report(self):
        for expected in ("## Negative tests", "```diff",
                         "+ OTel: redis.client.demo.retries — Int64Counter, unit {request}",
                         "+ OTel: redis.client.demo.retries — added attribute demo.reason (string)",
                         "- Public API: (*Client).NewExplainBuilder: removed",
                         "https://github.com/owner/repo/actions/runs/123"):
            self.assertIn(expected, self.body)
        self.assertIn("delta=" + "d" * 64, self.body)
        self.assertTrue(self.body.startswith("<!-- negative-tests:checkbox run=123:attempt=1:delta="))
        self.assertEqual(self.body.count("demo.reason"), 1)
        self.assertEqual(self.body.count("- [ ]"), 2)

    def test_changes_are_attributed_to_the_right_metric(self):
        previous = metric("server", [], instrument="PrometheusCounter")
        updated = dict(previous, attributes=[{"name": "node", "type": "string"}])
        self.assertIn("+ OTel: server — added label node (string)",
                      render(manifest(), findings([change(action="modify", before=previous, after=updated)])))

    def test_single_tick_only_and_closed_report_is_inert(self):
        for name, line in ACTIONS.items():
            checked = self.body.replace(line, line.replace("[ ]", "[x]"))
            self.assertEqual(edited_decision(self.body, self.body, checked), name)
            closed = render(manifest(), self.delta, name, "maintainer", 321)
            self.assertNotIn("- [ ]", closed)
            self.assertIn("maintainer", closed)
            self.assertIn("runs/321", closed)
            self.assertIsNone(edited_decision(closed, closed, checked))

    def test_no_edit_can_change_diff_marker_context_or_second_checkbox(self):
        all_checked = self.body.replace("[ ]", "[x]")
        tick = self.body.replace("[ ]", "[x]", 1)
        stale = render(manifest(run_id=456), self.delta)
        for before, after in ((self.body, all_checked), (self.body, self.body), (self.body, tick + " "),
                              (self.body, tick.replace("demo.reason", "demo.another")),
                              (stale, stale.replace("[ ]", "[x]", 1)), (tick, tick),
                              (self.body, tick.replace("delta=", "X=")),
                              (self.body, tick + "\n"), (None, tick)):
            self.assertIsNone(edited_decision(self.body, before, after))

    def test_uniform_markdown_line_endings_preserve_exact_checkbox_edit(self):
        tick = self.body.replace("[ ]", "[x]", 1)
        self.assertEqual(edited_decision(self.body, self.body, tick.replace("\n", "\r\n")), "approve")
        self.assertIsNone(edited_decision(self.body, self.body, tick.replace("\n", "\r\n", 1)))

    def test_preview_cannot_create_controls_and_full_report_is_linked(self):
        hostile = "evil\n```\n- [ ] ✅ Approve these changes\n@who" + "x" * 400
        output = render(manifest(), findings([change(after=metric(hostile))],
                                            [f"- removed {i}" for i in range(20)]))
        self.assertEqual(output.count("\n- [ ]"), 2)
        self.assertEqual(output.count("\n```"), 2)
        self.assertNotIn("@who", output)
        self.assertIn("full line in report", output)
        self.assertIn("8 more lines", output)
        self.assertIn("Full report and collection log", output)

    def test_error_and_empty_reports_have_no_checkbox_body(self):
        with self.assertRaises(ValueError):
            render(manifest(), findings())
        self.delta["checks"][0]["result"] = "error"
        with self.assertRaises(Rejected):
            render(manifest(), self.delta)


if __name__ == "__main__":
    unittest.main()
