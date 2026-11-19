# Telemetry contract: base comparison with native approval

Every PR is compared with its target branch. There is **no committed telemetry
JSON**, service, database or custom GitHub App. The same static Go analyzer from
the fixed base commit extracts both contracts without running client code.

```text
pull_request (read-only): fixed base B + PR head H → merge candidate M
                  contract(B) vs contract(M) → immutable artifacts
                                      |
workflow_run (trusted default branch): validate source run + candidate
                        /                              \
                    no change                        change
                        |                     post colored diff on PR
                        |                     link to this review run
                        |                              |
                        |                   look for matching human approval
                        |                   found → reuse; otherwise Environment
                        \                              /
                         validate current candidate + reviewer
                         update telemetry-contract on PR head H
```

The comparison succeeds when it finds changes; it produces `has_changes=true`.
Only the approval job waits. The final required check passes after verifying the
current PR, the report hashes, this workflow run and an actual Environment
approval by a repository **admin or maintainer**. An identical telemetry delta
can reuse a verified original human approval from the same PR and target.
Collection errors cannot be
approved. Rejection/cancellation/failure does not produce a successful final gate.
A force-cancel can prevent the final job from running; its absent/cancelled result
must remain merge-blocking.

## Present the demo

The demo repository is `ofekshenawa/go-redis`. Its `master` stays identical to
`redis/go-redis`; `telemetry-demo/base` adds the guard and is the demo repository's
default branch. This lets GitHub run the trusted `workflow_run` publisher while
leaving `master` untouched. Demo PRs target that branch. In production both
workflows must be installed on the normal default branch.

1. Open the no-change PR. Collection completes, the trusted review workflow skips
   human approval, and its `telemetry-contract` Check Run on the PR head is green.
2. Open the metric-change PR's **Conversation**. A bot comment shows the exact
   source head, full PR telemetry delta in colored `diff` blocks, and the link
   **Review telemetry — approve or reject** below it.
3. Click that link. It opens the exact **Telemetry review** run. Choose
   **Review deployments → telemetry-review → Approve and deploy**, or **Reject**.
   Nothing is deployed. The final validator checks admin/maintain permission and
   current B/H/M, updates the PR check, and updates that comment's status.
4. Push a non-telemetry commit: a fresh comparison runs, but an identical delta
   and tool/policy context reuse the original approval. The new comment links to
   that human approval and the Environment job is skipped. Add another attribute:
   the new delta requires manual review. Comments are idempotent per source report.
5. To demonstrate a moved base, advance the demo target and update/rebase the PR.
   The new run compares against the new target. Final validation rejects a stale
   candidate even if its native review was approved.

`telemetry-review` has `@ofekshenawa` as its required reviewer. The demo permits
self-approval because it is a single-user repository; the API still requires the
reviewer to be admin/maintain. Admin bypass is disabled. No secrets are needed.

GitHub's native approval popup has no custom report body. The report now lives
in the PR conversation, with its decision link directly underneath. The link
opens the approval interface; it is not a one-click approval. The report compares
the **whole PR against its target branch**, including earlier commits. Large
comments state explicitly when truncated and link to the full source artifacts.

## GitHub setup and trust boundary

- Create Environment **telemetry-review** with required reviewers. Add only the
  intended maintainers/admins, and disable admin bypass. The final API check also
  verifies the actual approving user's `role_name` and identity.
- Use this Environment on **exactly one job per workflow run**. Review history
  identifies the run and Environment, not an individual report or job.
- Require check **telemetry-contract** and **branches up to date before merging**.
  Apply this to every permitted target branch. The final API check reduces races;
  GitHub's strict branch protection handles the race after it finishes.
- Require code-owner review for the collector, all workflows and CODEOWNERS.
  The collector comes from B; the publisher and final validator come from the
  exact trusted default-branch workflow commit. The collection workflow itself
  still needs protected review. Artifact hashes bind data to a run; they do not
  prove that an unreviewed, modified collection workflow is honest. In production add
  a second eligible code owner for changes authored by the first owner. A single
  owner cannot approve their own normal PR review.
- Collection uses `pull_request`, read-only tokens, pinned actions and no shared
  Actions cache. Forks retain GitHub's workflow-run approval policy.
- Install `.github/workflows/telemetry-review.yml` on the **default branch**.
  Its `workflow_run: completed` publisher has only the read permissions needed
  for evidence plus Checks/PR-comment write permissions. It never checks out or
  executes PR code, scripts from artifacts, or a publisher supplied by a PR.
  GitHub CLI downloads artifacts into an isolated temporary directory. Only
  allowlisted, size-bounded data files are accepted; none are executed.
- The controller validates the source event/repository/workflow/run/attempt,
  artifact identity and hashes, live PR/base and merge parents before publishing
  an approvable comment. The review run is distinct from the collection run;
  both identities are bound into the trusted review state and the Check Run.
- The required check is explicitly created on the **PR head**, because native
  `workflow_run` job checks attach to the default-branch commit. Protect its
  source as GitHub Actions. Do not require the controller's own job names.
- The MVP does **not** implement `merge_group`. Do not disable an existing merge
  queue to install it; first implement/test that integration. An absent queue
  check must block rather than silently pass.

The demo branch can enforce the final check with strict up-to-date validation.
Its single-owner setup demonstrates the approval UX; it is not proof of a
production policy against a malicious workflow author. No production branch
protections or upstream settings are changed by installing these files.

## Base updates, stacked PRs and reruns

The manifest fixes B, H, M, tool revision, collection run ID/attempt and both report
hashes. Trusted review state also binds the separate approval run and controller
revision. The base is the actual PR target, including a parent branch for stacked
PRs. Merge parent first, retarget the child, update it and review its new run.
There is no dependency database. A retarget conservatively requires a new manual
approval even when the delta is unchanged; ordinary base/head SHA changes do not.

A new base/head/target invalidates the old candidate. A base push alone does not
schedule this PR workflow: update/rebase the PR or close/reopen it to start a
fresh run. Strict up-to-date protection prevents an old green check authorizing
an outdated merge.

GitHub's approval-history API does not expose the run attempt associated with an
approval. Therefore **Re-run jobs / Re-run all jobs are rejected in this MVP**.
For a fresh attempt, push a new commit (an empty commit works) or reopen the PR.
This deliberately trades convenience for unambiguous run-to-report approval.

## Approval reuse without a service

Every run still collects and validates the current B/H/M. `approval_reuse.py`
hashes the canonical full delta (including each changed metric's complete
before/after definition) and its repository, PR, target and tool/policy context.
The context hashes Git blob IDs and file modes for the telemetry tool and both
workflows at B, M and the trusted controller revision. It includes dependencies
and configuration, excludes documentation/tests, and never hashes the whole
repository SHA. Rebase/base changes outside these files can preserve approval;
changes to relevant metric conditions, collector or gate cannot.
The Environment ID, configured reviewer identities, self-review policy and admin
bypass setting are also bound to the approval. Changing these requires review.

When **manual** review is needed, the existing state-upload step saves its four
data files as `telemetry-approval-<PR>-<fingerprint>`, retained for 30 days. This
becomes reusable evidence only after explicit approval and successful completion
of that review run. The name is only an index,
not proof: lookup verifies the original repository, workflow event/path, immutable
artifact/run identity, report hashes, actual code versions, explicit Environment
approval and the approving user's current admin/maintain role. A green run without
a human approval is never sufficient. Reused runs do not mint new approval
artifacts or extend the original retention period.

Lookup requires the complete matching history to fit within 20 records; otherwise
it asks for manual review. It considers the newest matching artifact.
A later manual request supersedes older approval even if rejected, cancelled or
still waiting: the controller never falls back past that record to an older
approval. Reused runs use ordinary run-specific state names and do not supersede
the original human decision. Missing, expired, invalid or
unavailable historical evidence falls back to a new manual review. An incomplete
tool-version lookup permits manual review but does not create reusable evidence.
The final job rechecks any selected approval and the live candidate. If evidence
disappears or the candidate changes between publication and finalization, it
fails closed; start a fresh run. No write permission or new service is needed
to read evidence. Existing artifact-upload steps handle persistence.

To revoke reuse, delete the original `telemetry-approval-...` artifact in GitHub
and start a fresh run; existing completed checks are not retroactively
revoked. Remove all matching artifacts if the intent is to require a new
decision, since deleting the latest record can reveal an older approval.
Normal PR review rules still apply.

Install this module and the updated review workflow together on the default
branch and relevant PR targets. Older approvals do not contain this evidence:
the first changed run after installation needs one new manual approval. This
isolated implementation has automated tests; native approval-to-reuse still
requires a GitHub staging run before production adoption.

## Local commands and files

Use the Go version in `go.mod`. From this directory:

```sh
go test ./...
python3 -m unittest discover -p 'test_*.py'
go build -o /tmp/telemetry-contract .
/tmp/telemetry-contract -root /path/to/base -output /tmp/base.json
/tmp/telemetry-contract -root /path/to/merge-candidate -output /tmp/candidate.json
/tmp/telemetry-contract -base /tmp/base.json -candidate /tmp/candidate.json -report-dir /tmp/report
```

Exit `0` means successful collection/comparison, including changes. Exit `2`
means invalid source/input. A completely empty inventory is an error; retiring
all telemetry requires a separately reviewed coverage-policy change.

- `load.go`, `scan.go`: language-specific source extraction.
- `main.go`, `compare.go`: capture CLI, contract normalization, structured delta
  and a short Markdown summary. Other clients can supply the same JSON contract.
- `prepare.py`: binds a completed report to fixed Git commits and this fresh run.
- `gate.py`: shared GitHub CLI adapter and report/candidate/reviewer validation.
- `comment_report.py`: deterministic, escaped colored-diff comment presentation.
- `controller.py`: validate source evidence, publish/update the comment, and
  create/finalize the Check Run on the explicit PR SHA.
- `approval_reuse.py`: deterministic approval identity and original human-evidence
  validation using GitHub APIs/artifacts; no server or database.
- `.github/workflows/telemetry-contract.yml`: unprivileged collection only.
- `.github/workflows/telemetry-review.yml`: trusted publication → approval → final check.

Contracts, delta, report and manifest exist only as per-run artifacts (30 days).
The approval is available in the GitHub run's review history.

## Reusing existing tools

- Native GitHub Environments provide the approval/rejection gate.
- Official `actions/upload-artifact` and `actions/download-artifact` transfer
  report and review state between jobs.
- [GitHub CLI](https://cli.github.com/manual/gh_api) handles authentication and
  API requests; `gh run download` handles artifact transport and extraction.
  The trusted review jobs require `gh`, already installed on GitHub-hosted runners.
- `comment_report.py` is the single detailed human-readable renderer. The Go
  comparator produces `delta.json` and a short count-only `report.md`; it no
  longer maintains a second rich report, `review.txt`, or dynamic approval titles.

The remaining custom code extracts the source contract and validates exactly
which report, candidate and approval may turn the required check green. Replacing
small comment/Check API calls with separate Actions would add orchestration and
state handoffs without removing that policy. The current flow keeps those calls
in one controller using the official CLI, rather than a custom HTTP client.

## What is collected

The initial inventory covers `extra/redisotel-native`, `extra/redisotel`, and
`extra/redisprometheus`. Production modules are discovered from imports, including
new modules in a PR and callers of local telemetry modules. Tests, examples,
`testdata`, doctests, fuzz/e2e fixtures, vendored code and tools are excluded.
Production files excluded by build constraints in a selected module fail the
check rather than silently disappearing. Add explicit support before introducing
platform-specific telemetry.

Each metric records:

- Source package, metric name, numeric instrument type and unit.
- Attribute/label names and their OTel types **per metric**.
- Whether the API accepts caller-provided attributes.

A separate `attribute_declarations` list records OTel attribute constructions,
including ones not attached to a metric, and tracing attributes in the selected
packages. This also detects a newly declared key in unused code. Prometheus
labels appear on their metric with type `string`. Prometheus' existing public
namespace/subsystem options are represented as `{namespace}` and `{subsystem}`;
the metric suffix and label names must be static.

The scanner follows constants, local helpers, common slices/structs, attribute
sets and the existing attribute cache. It reads referenced semantic-convention
helpers from the pinned OTel dependency. All function bodies and conditional
branches are considered, even if no test calls them. Alternative instrument
kinds remain separate entries (for example the legacy cumulative-counter option).
Known APIs are handled explicitly; unsupported metric/attribute APIs, unresolved
metric names/units/attribute keys and indirect telemetry calls stop analysis.

## Coverage limits

This is a **source inventory**, not proof of runtime behavior or arbitrary Go
program equivalence. It uses a conservative union of possible values. Struct
fields are joined by their declared field, and cache entries are joined; distinct
instances/configurations can therefore contribute attributes to the same metric.
For example, the legacy tracing and metrics configuration share fields. This can
report more attributes than one runtime configuration actually emits, and it
cannot detect a change in correlations or a new use already represented by that
union. A refactor that changes the conservative result requires review of the
JSON diff.

It does not compare sample values, attribute values, timestamps, descriptions,
histogram boundaries, emission frequency or whether a registration/recording
path is reachable. Meter scope names and exporter/resource-level transformations
are also outside this version's schema. Application-supplied attribute keys are
intentionally open; the contract records that extension point, not the keys a
consumer might supply. Arbitrary reflection, unsafe code, generated code not
present in the checkout, and new external telemetry wrappers require additional
analysis support and review. External dependencies are not recursively audited
for all of their own telemetry. OpenCensus is outside this OTel/Prometheus guard.


The Go frontend is specific to the supported go-redis patterns. Another client
needs its own language-specific extractor. The comparison/approval design does
not require a checked-in baseline or an approval backend.

References: [Environment review](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/review-deployments),
[workflow review history](https://docs.github.com/en/rest/actions/workflow-runs#get-the-review-history-for-a-workflow-run),
[protected branches](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches).
