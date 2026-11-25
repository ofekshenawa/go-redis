"""Human decisions on immutable PR comments, or removals of report-specific labels."""
import re
import time
from urllib.parse import quote

from approval_reuse import context, fingerprint
from comment_report import render_findings
from gate import current_candidate, number, require, reviewer_login

MODES = {"negative-tests/label": "label", "negative-tests/reaction": "reaction"}
MARKER = re.compile(r"<!-- negative-review (label|reaction) ([0-9a-f]{64}) ([1-9][0-9]*) ([0-9]+) -->")
BOT = "github-actions[bot]"


def pages(api, path):
    separator = "&" if "?" in path else "?"
    for page in range(1, 11):
        rows = api.get(f"{path}{separator}per_page=100&page={page}")
        require(isinstance(rows, list), "Invalid GitHub evidence response.")
        yield from rows
        if len(rows) < 100:
            return
    raise ValueError("Evidence exceeds 1,000 records; manual investigation required.")


def label_name(pr, source):
    return f"review-{pr}-{source}"


def body(manifest, delta, mode, effect, origin, label_id):
    url = f"https://github.com/{manifest['repository']}/actions/runs/{origin}"
    if mode == "reaction":
        action = ("React 👍 **on this comment** to approve both rows. The initial run checks every "
                  "15 seconds for one minute. After that use **Run workflow** in "
                  f"[Recheck negative tests](https://github.com/{manifest['repository']}/actions/workflows/negative-decision.yml) "
                  f"on the default branch with PR number **{manifest['pr']}**.")
    else:
        action = (f"To approve both rows, remove **`{label_name(manifest['pr'], origin)}`** "
                  "from this PR’s Labels sidebar.")
    return (f"<!-- negative-review {mode} {effect} {origin} {label_id} -->\n## Negative tests\n\n"
            + render_findings(manifest, delta) + action +
            f"\n\n[Full diff and original report]({url}) · Report `{effect[:12]}`")


def bot(actor):
    return actor.get("type") == "Bot" and actor.get("login") == BOT


def matches(comment, manifest, delta, mode, effect):
    match = MARKER.match(comment.get("body", ""))
    if not match or not bot(comment.get("user", {})) or match.groups()[:2] != (mode, effect):
        return None
    origin, label_id = map(int, match.groups()[2:])
    if (comment.get("created_at") != comment.get("updated_at")
            or comment["body"] != body(manifest, delta, mode, effect, origin, label_id)):
        return None
    return origin, label_id


def publish(api, prefix, state, manifest, delta):
    mode = MODES[manifest["base_ref"]]
    effect = fingerprint(manifest, delta, context(api, prefix, manifest, state["controller_sha"]))
    pr, origin = manifest["pr"], state["source_run_id"]
    history = (c for c in pages(api, f"{prefix}/issues/{pr}/comments")
               if bot(c.get("user", {})) and MARKER.match(c.get("body", "")))
    # Only the latest report is eligible. Returning to an old effect after another
    # report appeared produces a new comment and a new label/reaction target.
    prior = max(history, key=lambda c: c["id"], default={})
    bound = matches(prior, manifest, delta, mode, effect)
    label_id = 0
    if bound:
        origin, label_id = bound
        comment = prior
    else:
        if mode == "label":
            created = api.post(f"{prefix}/labels", {"name": label_name(pr, origin), "color": "b60205",
                               "description": "Remove to approve this exact negative-test report"})
            label_id = number(created["id"], "label ID")
        comment = api.post(f"{prefix}/issues/{pr}/comments",
                           {"body": body(manifest, delta, mode, effect, origin, label_id)})
    return {"decision_mode": mode, "fingerprint": effect, "origin": origin, "label_id": label_id,
            "comment_id": number(comment["id"], "report comment"), "comment_url": comment["html_url"]}


def reaction_approval(api, prefix, state):
    path = f"{prefix}/issues/comments/{state['comment_id']}/reactions?content=%2B1"
    for reaction in pages(api, path):
        if reaction.get("content") == "+1":
            login = reviewer_login(api, prefix, reaction.get("user", {}))
            if login:
                return login, reaction["id"]
    return None


def label_approval(api, prefix, state):
    pr = state["pr"]
    name = label_name(pr, state["origin"])
    repo_label = api.get(f"{prefix}/labels/{name}")
    require(repo_label["id"] == state["label_id"], "The report label was renamed or recreated.")
    labels = list(pages(api, f"{prefix}/issues/{pr}/labels"))
    for label in labels:
        if re.fullmatch(rf"review-{pr}-[0-9]+", label["name"]) and label["name"] != name:
            api.run("api", "-X", "DELETE", f"{prefix}/issues/{pr}/labels/{quote(label['name'], safe='')}")
    if any(label["id"] == state["label_id"] for label in labels):
        return None
    events = [e for e in pages(api, f"{prefix}/issues/{pr}/timeline")
              if e.get("event") in ("labeled", "unlabeled") and (e.get("label") or {}).get("name") == name]
    if (len(events) >= 2 and events[-2]["event"] == "labeled" and bot(events[-2]["actor"])
            and events[-1]["event"] == "unlabeled"):
        login = reviewer_login(api, prefix, events[-1]["actor"])
        if login:
            return login, events[-1]["id"]
    # Absent label alone is never approval. Install the blocking label in this
    # run, including after an unauthorized removal.
    api.post(f"{prefix}/issues/{pr}/labels", {"labels": [name]})
    return None


def reconcile(api, prefix, state, manifest, delta, *, poll=False):
    from controller import conclude, status_identity
    mode, effect = state["decision_mode"], state["fingerprint"]
    require(MODES.get(manifest["base_ref"]) == mode and delta["version"] == 2 and delta["has_changes"],
            "Unexpected decision mode or report.")
    require(fingerprint(manifest, delta, context(api, prefix, manifest, state["controller_sha"])) == effect,
            "Approval policy or report changed; start a new collection.")
    attempts = 5 if mode == "reaction" and poll else 1
    for attempt in range(attempts):
        if attempt:
            time.sleep(15)
        current_candidate(api, prefix, manifest)
        status_identity(api, prefix, state)
        comment = api.get(f"{prefix}/issues/comments/{state['comment_id']}")
        require(matches(comment, manifest, delta, mode, effect) == (state["origin"], state["label_id"])
                and comment.get("issue_url") == f"https://api.github.com{prefix}/issues/{manifest['pr']}",
                "The original decision comment changed or belongs to a different PR.")
        evidence = reaction_approval(api, prefix, state) if mode == "reaction" else label_approval(api, prefix, state)
        print(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {mode} check {attempt+1}: "
              f"{'authorized' if evidence else 'no authorized decision'}", flush=True)
        if evidence:
            current_candidate(api, prefix, manifest)
            conclude(api, prefix, state, "success", f"Report {effect[:12]} approved by {evidence[0]} ({mode} event {evidence[1]}).")
            return
    conclude(api, prefix, state, "pending", f"Waiting for a maintainer on {mode} report {effect[:12]}.")
