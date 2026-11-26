"""Publisher and issue_comment verifier for the checkbox demo's required status."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "telemetry-contract"))
from alternative_gate import bot
from approval_reuse import canonical, code_version, fingerprint
from controller import (conclude, identity, latest_status, outputs, read_artifact, source_run,
                        start_status, status_identity, validate_context)
from gate import GitHub, Rejected, current_candidate, number, require, reviewer_login, sha
from checkbox_report import edited_decision, render

TARGET = "negative-tests/checkbox"
COLLECT = ".github/workflows/negative-checkbox-collect.yml"
PUBLISH = ".github/workflows/negative-checkbox-publish.yml"
EDIT = ".github/workflows/negative-checkbox-edit.yml"
LABEL = "negative-tests: needs-review"
ROOT = Path(__file__).resolve().parents[2]


def belongs(pr, head, fork):
    return (pr.get("state") == "open" and pr["base"]["ref"] == TARGET
            and pr["head"]["sha"] == head and pr["head"]["repo"]["id"] == fork)


def locate(api, prefix, source):
    """Use GitHub's PR association, including fork runs with an empty association list."""
    require(source["event"] == "pull_request" and source["path"] == COLLECT
            and source["name"] == "Negative tests - checkbox collection", "Unexpected collector.")
    head, fork = source["head_sha"], number(source["head_repository"]["id"], "head repository")
    matches = source["pull_requests"]
    if matches:
        candidates = [api.get(f"{prefix}/pulls/{number(item['number'], 'PR')}") for item in matches]
    else:
        candidates = api.get(f"{prefix}/commits/{head}/pulls?per_page=100")
        require(len(candidates) < 100, "Ambiguous collection head.")
    matching = [p for p in candidates if belongs(p, head, fork)]
    require(len(matching) == 1, "No unique current checkbox PR for the collected head.")
    return number(matching[0]["number"], "PR")


def checkbox_policy():
    # Legacy guards hash their own tool directory. Hash the fourth method's new
    # trusted files separately, by content, so an unrelated rebase is reusable.
    return subprocess.check_output(
        ["git", "ls-tree", "-r", "HEAD", "--", PUBLISH, EDIT,
         "tools/negative-checkbox/checkbox_gate.py", "tools/negative-checkbox/checkbox_report.py"],
        cwd=ROOT, text=True)


def validated(api, prefix, state, folder, source):
    require(source["status"] == "completed" and source["conclusion"] == "success"
            and source["run_attempt"] == 1, "Collection failed or was rerun.")
    manifest, delta = validate_context(api, prefix, state, folder, source)
    require(manifest["pr"] == state["pr"] and manifest["base_ref"] == TARGET and delta["version"] == 2,
            "This method requires both checks on the checkbox target.")
    path = f"{prefix}/contents/{COLLECT}?ref="
    blob = sha(api.get(path + manifest["base_sha"])["sha"], 40, "workflow")
    require(api.get(path + manifest["merge_sha"])["sha"] == blob, "Collection workflow changed in the PR.")
    active_code = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    revisions = {r: code_version(api, prefix, r, collecting=False)
                 for r in {manifest["base_sha"], manifest["merge_sha"], active_code}}
    effect = fingerprint(manifest, delta, {"method": "checkbox", "roles": ["maintain", "admin"],
                         "collector": blob, "base": revisions[manifest["base_sha"]],
                         "candidate": revisions[manifest["merge_sha"]], "trusted": revisions[active_code],
                         "checkbox_policy": checkbox_policy()})
    return manifest, delta, effect


def indicator(api, prefix, pr, present):
    path = f"{prefix}/issues/{pr}/labels"
    # Labels are names returned by GitHub. Never infer any decision from them.
    labels = api.get(f"{prefix}/pulls/{pr}")["labels"]
    if present and all(item["name"] != LABEL for item in labels):
        api.post(path, {"labels": [LABEL]})
    elif not present and any(item["name"] == LABEL for item in labels):
        api.run("api", "--method", "DELETE", path + "/" + quote(LABEL, safe=""))


def matching_run(api, prefix, repo, run_id, event, path):
    run = source_run(api, prefix, run_id, repo)
    require(run["head_repository"]["full_name"] == repo and run["event"] == event and run["path"] == path
            and run["run_attempt"] == 1 and run["status"] == "completed" and run["conclusion"] == "success",
            "Expected an original successful trusted workflow run.")
    return run


def receipt_name(pr):
    return f"checkbox-decision-pr-{pr}"


def reusable(api, prefix, state, manifest, delta, effect):
    """Recheck the most recent actual human decision for this PR against the new report."""
    repo, pr = state["repository"], state["pr"]
    name = receipt_name(pr)
    listing = api.get(f"{prefix}/actions/artifacts?name={name}&per_page=50")
    # A missing/expired receipt simply requires a fresh click. Earlier approvals
    # must not supersede a newer rejection or incomplete decision.
    if not listing["artifacts"] or listing["total_count"] > 50:
        return None
    art = max(listing["artifacts"], key=lambda a: number(a["id"], "artifact"))
    if art["expired"] or art["name"] != name or not 0 < art["size_in_bytes"] <= 20 * 1024 * 1024:
        return None
    edit_id = number(art["workflow_run"]["id"], "receipt run")
    try:
        run = matching_run(api, prefix, repo, edit_id, "issue_comment", EDIT)
        require(run["head_sha"] == art["workflow_run"]["head_sha"], "Receipt revision changed.")
        with tempfile.TemporaryDirectory() as tmp:
            api.download(repo, edit_id, name, Path(tmp))
            receipt = json.loads((Path(tmp) / "decision.json").read_bytes())
        actor = receipt["actor"]
        require(receipt["edit_run"] == edit_id and receipt["repository"] == repo and receipt["pr"] == pr
                and receipt["decision"] == "approve" and receipt["effect"] == effect
                and canonical(receipt["delta"]) == canonical(delta)
                and receipt["manifest"]["base_ref"] == manifest["base_ref"]
                and run["actor"]["id"] == actor["id"] and reviewer_login(api, prefix, actor) == actor["login"],
                "Prior receipt is not an authorized approval for this effect.")
        old = api.get(f"{prefix}/issues/comments/{number(receipt['comment'], 'original comment')}")
        require(bot(old["user"]) and old["issue_url"] == f"https://api.github.com{prefix}/issues/{pr}"
                and old["body"] == render(receipt["manifest"], receipt["delta"], "approve", actor["login"], edit_id),
                "Original approved report was altered.")
        return receipt
    except (Rejected, ValueError, KeyError, TypeError, OSError):
        return None


def publish(api, prefix, state, source, folder):
    require(locate(api, prefix, source) == state["pr"], "The collection has been superseded.")
    state.update(source_head_sha=source["head_sha"], source_run_id=source["id"])
    state["status_id"] = number(start_status(api, prefix, state)["id"], "status ID")
    try:
        require(source["conclusion"] == "success", "Collection failed; see source run.")
        read_artifact(api, prefix, source, folder)
        manifest, delta, effect = validated(api, prefix, state, folder, source)
        state["effect"] = effect
        receipt = reusable(api, prefix, state, manifest, delta, effect) if delta["has_changes"] else None
        current_candidate(api, prefix, manifest)
        status_identity(api, prefix, state)
        if delta["has_changes"] and not receipt:
            comment = api.post(f"{prefix}/issues/{state['pr']}/comments", {"body": render(manifest, delta)})
            state["comment"] = number(comment["id"], "comment")
            (folder / "state.json").write_text(json.dumps(state))
            indicator(api, prefix, state["pr"], True)
            outputs(os.environ, state_name=f"checkbox-state-{state['controller_run_id']}")
            print(f"Waiting for a decision on {comment['html_url']} (report {effect}).")
        else:
            indicator(api, prefix, state["pr"], False)
            current_candidate(api, prefix, manifest)
            reason = (f"Report {effect[:12]} approved by {receipt['actor']['login']} in edit run {receipt['edit_run']}."
                      if receipt else "Negative tests passed with no findings.")
            conclude(api, prefix, state, "success", reason)
    except Exception:
        conclude(api, prefix, state, "failure", "Checkbox collection or publication failed. See publisher run.")
        raise


def restore(api, prefix, repo, pr, folder):
    live = api.get(f"{prefix}/pulls/{pr}")
    require(live["state"] == "open" and live["base"]["ref"] == TARGET, "Not an open checkbox demo.")
    head = sha(live["head"]["sha"], 40, "live head")
    status = latest_status(api, prefix, {"source_head_sha": head})
    if not status or status["state"] != "pending":
        return None
    match = re.fullmatch(re.escape(f"https://github.com/{repo}/actions/runs/") + r"([1-9][0-9]*)",
                         status["target_url"])
    require(match and bot(status["creator"]), "Missing publisher for this head.")
    pub_id = int(match[1])
    run = matching_run(api, prefix, repo, pub_id, "workflow_run", PUBLISH)
    api.download(repo, pub_id, f"checkbox-state-{pub_id}", folder)
    state = json.loads((folder / "state.json").read_bytes())
    require((state["pr"], state["repository"], state["controller_run_id"], state["controller_sha"],
             state["controller_attempt"], state["source_head_sha"]) == (pr, repo, pub_id, run["head_sha"], 1, head),
            "Publisher state is for another candidate.")
    status_identity(api, prefix, state)
    return state


def decide(api, prefix, repo, edit_id, event, folder):
    """Verify the human edit twice: before persisting its receipt and before publishing its status."""
    if (event.get("action") != "edited" or not event["issue"].get("pull_request")
            or event["repository"]["full_name"] != repo or not bot(event["comment"]["user"])
            or event["sender"].get("type") != "User"):
        return None
    pr, comment_id = number(event["issue"]["number"], "PR"), number(event["comment"]["id"], "comment")
    state = restore(api, prefix, repo, pr, folder)
    if state is None or state["comment"] != comment_id:
        return None
    source = source_run(api, prefix, state["source_run_id"], repo)
    require(locate(api, prefix, source) == pr, "Collection does not match this PR.")
    manifest, delta, effect = validated(api, prefix, state, folder, source)
    require(effect == state["effect"] and delta["has_changes"], "Report or trusted policy changed.")
    before = event.get("changes", {}).get("body", {}).get("from")
    after = event["comment"]["body"]
    comment = api.get(f"{prefix}/issues/comments/{comment_id}")
    require(bot(comment["user"]) and comment["issue_url"] == f"https://api.github.com{prefix}/issues/{pr}",
            "Expected this publisher's PR comment.")
    if comment["body"] != after or comment["updated_at"] != event["comment"]["updated_at"]:
        return None  # A later edit superseded this event.
    expected = render(manifest, delta)
    decision = edited_decision(expected, before, after)
    actor = event["sender"]
    login = reviewer_login(api, prefix, actor) if decision else None
    if not login:
        # Restore the active, still-pending report so a malformed edit or a click
        # by a collaborator without the required role cannot strand the controls.
        status_identity(api, prefix, state)
        api.patch(f"{prefix}/issues/comments/{comment_id}", {"body": expected})
        return None
    return state, {"repository": repo, "pr": pr, "comment": comment_id, "decision": decision,
                   "actor": {"id": actor["id"], "type": "User", "login": login}, "edit_run": edit_id,
                   "clicked_at": event["comment"]["updated_at"], "effect": effect,
                   "manifest": manifest, "delta": delta}


def apply(api, prefix, state, receipt):
    pr, cid, decision = receipt["pr"], receipt["comment"], receipt["decision"]
    login, edit_id = receipt["actor"]["login"], receipt["edit_run"]
    current_candidate(api, prefix, receipt["manifest"])
    status_identity(api, prefix, state)
    indicator(api, prefix, pr, decision == "reject")
    api.patch(f"{prefix}/issues/comments/{cid}", {"body": render(receipt["manifest"], receipt["delta"],
                                                                decision, login, edit_id)})
    current_candidate(api, prefix, receipt["manifest"])
    conclusion = "success" if decision == "approve" else "failure"
    conclude(api, prefix, state, conclusion, f"{decision} by {login} for {receipt['effect'][:12]} (edit run {edit_id}).")
    print(f"issue_comment.edited by {login}: {decision}; comment {cid}; effect {receipt['effect']}; "
          f"edited_at {receipt['clicked_at']}; status {conclusion}.")


def main():
    action = sys.argv[1]
    state = identity(os.environ)
    repo, edit_id = state["repository"], state["controller_run_id"]
    prefix = "/repos/" + repo
    api = GitHub(os.environ["GITHUB_TOKEN"])
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_bytes())
    if action in ("locate", "publish"):
        require(os.environ["GITHUB_EVENT_NAME"] == "workflow_run", "Expected collection completion.")
        source = source_run(api, prefix, number(event["workflow_run"]["id"], "source run"), repo)
        if action == "locate":
            outputs(os.environ, pr=locate(api, prefix, source))
        else:
            state["pr"] = number(os.environ["PR"], "PR")
            publish(api, prefix, state, source, Path("report-state"))
        return
    require(action in ("inspect", "apply") and os.environ["GITHUB_EVENT_NAME"] == "issue_comment",
            "Expected a native comment edit.")
    with tempfile.TemporaryDirectory() as tmp:
        result = decide(api, prefix, repo, edit_id, event, Path(tmp))
    if not result:
        require(action != "apply", "The click was superseded before its receipt could be applied.")
        print("Ignored edit: no authorized decision on this active report.")
        return
    state, receipt = result
    path = Path("decision.json")
    if action == "inspect":
        path.write_text(json.dumps(receipt, sort_keys=True))
        outputs(os.environ, receipt=receipt_name(state["pr"]))
    else:
        require(json.loads(path.read_bytes()) == receipt, "The persisted decision no longer matches the live edit.")
        apply(api, prefix, state, receipt)


if __name__ == "__main__":
    try:
        main()
    except (Rejected, ValueError, KeyError, OSError, TypeError) as exc:
        sys.exit(str(exc))
