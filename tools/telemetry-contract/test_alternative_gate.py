import unittest
from unittest.mock import MagicMock, patch

from alternative_gate import body, matches, reaction_approval, label_approval, label_name
from gate import Rejected

BOT = {"type": "Bot", "login": "github-actions[bot]"}
HUMAN = {"id": 7, "type": "User", "login": "demo"}
DELTA = {"version": 2, "checks": [{"name": "Public API", "result": "needs_review", "diff": ["removed Test"]}]}


class AlternativeTests(unittest.TestCase):
    def setUp(self):
        self.manifest = {"repository": "o/r", "pr": 19}
        self.effect = "a" * 64
        self.state = {"comment_id": 2, "label_id": 9, "pr": 19, "origin": 22}
        self.api = MagicMock()
        self.comment = {"body": body(self.manifest, DELTA, "reaction", self.effect, 22, 0),
                        "created_at": "t", "updated_at": "t", "user": BOT}

    def test_exact_unchanged_report(self):
        self.assertEqual(matches(self.comment, self.manifest, DELTA, "reaction", self.effect), (22, 0))
        for key, value in [("user", HUMAN), ("updated_at", "new"), ("body", self.comment["body"]+"edited")]:
            self.assertIsNone(matches(dict(self.comment, **{key:value}),self.manifest,DELTA,"reaction",self.effect))

    def test_new_effect_cannot_reuse_old_comment(self):
        self.assertIsNone(matches(self.comment,self.manifest,DELTA,"reaction","b"*64))

    def test_reaction_reads_only_bound_comment_and_checks_live_role(self):
        def responses(path):
            if path == "/repos/o/r/issues/comments/2/reactions?content=%2B1&per_page=100&page=1":
                return [{"content":"+1","user":HUMAN,"id":3}]
            self.assertEqual(path,"/repos/o/r/collaborators/demo/permission")
            return {"role_name":self.role,"user":HUMAN}
        self.api.get.side_effect = responses
        for self.role in ["read","write","triage"]:
            self.assertIsNone(reaction_approval(self.api,"/repos/o/r",self.state))
        self.role="maintain"
        self.assertEqual(reaction_approval(self.api,"/repos/o/r",self.state),("demo",3))

    def test_label_checks_version_and_actor_even_if_absent(self):
        name=label_name(19,22)
        events=[{"label":{"name": name},"actor": BOT,"event":"labeled","id":23},
                {"label":{"name": name},"actor": HUMAN,"event":"unlabeled","id":24}]
        def responses(path):
            if path.endswith("/labels/"+name): return {"id":9}
            if "/timeline?" in path: return events
            if "/labels?" in path: return []
            return {"role_name":self.role,"user":HUMAN}
        self.api.get.side_effect=responses
        self.role="triage"
        self.assertIsNone(label_approval(self.api,"/repos/o/r",self.state))
        self.api.post.assert_called_once_with("/repos/o/r/issues/19/labels",{"labels":[name]})
        self.role="admin"
        self.assertEqual(label_approval(self.api,"/repos/o/r",self.state),("demo",24))
        events[-1]["label"]={"name":"review-19-123"}
        self.assertIsNone(label_approval(self.api,"/repos/o/r",self.state))
        with self.assertRaises(Rejected):
            label_approval(self.api,"/repos/o/r",dict(self.state,label_id=999))


if __name__ == "__main__":
    unittest.main()
