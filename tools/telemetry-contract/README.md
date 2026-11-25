# Negative tests with GitHub Environment review

This foundation publishes the result of a telemetry comparison as
`Negative tests / telemetry-contract` on the pull request's head commit. A
read-only collector supplies a report comparing the target commit with the
proposed merge. The collector and its coverage belong to the method layered
above this branch.

## From collection to review

1. The method's `pull_request` workflow is named `Telemetry contract` and lives
   at `.github/workflows/telemetry-contract.yml`. It writes `delta.json` and
   `report.md` and calls `prepare.py` to bind them to the exact PR, commits and
   collection run. It uploads them with `manifest.json` as
   `telemetry-report-<run ID>-1`.
2. GitHub runs `.github/workflows/telemetry-review.yml` from the repository's
   **default branch** when collection completes. The controller downloads the
   artifact, verifies the run, hashes and current candidate, and publishes a
   PR comment with a readable diff if telemetry changed.
3. With no changes the required status passes automatically. A new change
   waits for a reviewer through the comment's link to the native
   `telemetry-review` Environment. A repository admin or maintainer must be
   among the Environment reviewers and approve the run. The final job checks
   both their permission and the current candidate before publishing success.
   Collection errors, rejection, failed validation and cancelled reviews block
   merging.

An identical change in the same PR, targeting the same branch, can reuse its
original manual approval. The fingerprint covers the canonical metric changes,
tool and workflow contents, and Environment policy. The controller checks the
latest matching review artifact, the original review, and the approver's
current role. A changed delta or policy, a retarget, or missing or expired
evidence requires manual review. A changed commit SHA alone does not invalidate
an otherwise matching approval. Review artifacts are retained for 30 days.

## Repository setup

* Create the `telemetry-review` Environment with required reviewers and
  disable administrator bypass. The single-owner demo uses `@ofekshenawa`.
  The Environment is repository configuration. The YAML refers to it by name.
* The trusted review workflow must be installed on the default branch for
  GitHub to deliver its `workflow_run` events. Each demo PR targets its
  method branch, which supplies the baseline and the trusted collector.
* Require `Negative tests / telemetry-contract` from GitHub Actions on the
  branch receiving demo PRs, with strict up-to-date checks. When migrating
  from `telemetry-contract`, verify a genuine run under the new name before
  switching the required status. Code-owner review protects the workflows
  and tool on production branches.
* The collector runs with read-only permissions. Publishing a comment and a
  commit status uses only the trusted default branch's controller. The report
  is treated as data and the approval job deploys nothing.
* Start a fresh run after rebasing or retargeting. A new push or reopening
  the PR runs collection again. Re-running a workflow attempt is rejected
  because review history identifies a run rather than a specific attempt.
  Installation on a merge queue requires a separate merge-group integration.

Run the infrastructure tests from this directory with
`python3 -m unittest discover -p 'test_*.py'`. The tests cover report binding,
PR comments, Environment review, approval reuse and stale status updates.
