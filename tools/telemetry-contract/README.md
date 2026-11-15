# Telemetry contract: base comparison with native approval

Every PR is compared with its target branch. There is **no committed telemetry
JSON**, service, database or custom GitHub App. The same static Go analyzer from
the fixed base commit extracts both contracts without running client code.

```text
fixed base B + PR head H → GitHub merge candidate M
       contract(B) vs contract(M)
                   |
            publish diff + artifacts
            /                     \
       no difference           telemetry change
            |                     |
            |           telemetry-review Environment
            |           Review deployments → approve
            \                     /
             final telemetry-contract check
             validate current candidate + reviewer
```

The comparison succeeds when it finds changes; it produces `has_changes=true`.
Only the approval job waits. The final required check passes after verifying the
current PR, the report hashes, this workflow run and an actual Environment
approval by a repository **admin or maintainer**. Collection errors cannot be
approved. Rejection/cancellation/failure does not produce a successful final gate.
A force-cancel can prevent the final job from running; its absent/cancelled result
must remain merge-blocking.

## Present the demo

The demo repository is `ofekshenawa/go-redis`. Its `master` stays identical to
`redis/go-redis`; `telemetry-demo/base` adds this guard without changing telemetry.
Demo PRs target that branch. In production the same files live on `master`.

1. Open the no-change PR's **Telemetry contract** run. The report says unchanged,
   the approval job is skipped, and `telemetry-contract` succeeds automatically.
2. Open the metric-change PR's run. The waiting job is named after the actual
   change (for example `Approve telemetry: + metric …; attrs +demo.phase`). A
   **Telemetry changes to approve** notice shows the change on the run overview,
   alongside a link to this run's full report artifact. The comparison job's
   summary contains a metric/attribute table, including instrument and unit.
   The run is **Waiting** and exposes **Review deployments**.
3. Select `telemetry-review`, then **Approve and deploy**. Nothing is deployed.
   The final job confirms the reviewer's role and that base/head are still current,
   then passes without another source commit.
4. To demonstrate a new commit while waiting, update the PR. Concurrency cancels
   the old run. The new run produces a new report and needs its own approval.
5. To demonstrate a moved base, advance the demo target and update/rebase the PR.
   The new run compares against the new target. An old candidate is rejected by
   final validation even if someone approves its Environment.

`telemetry-review` has `@ofekshenawa` as its required reviewer. The demo permits
self-approval because it is a single-user repository; the API still requires the
reviewer to be admin/maintain. Admin bypass is disabled. No secrets are needed.

GitHub's native approval popup only lists the Environment; it has no custom
report body. Read the change on the run page before opening that popup. The
report compares the **whole PR against its target branch**, including changes
from earlier commits. Job names are short previews; an explicit `see report`
suffix indicates truncation. `review.txt`, `report.md` and `delta.json` in the
linked artifact provide progressively more detail. Notices and job titles need
no write token or PR comments, including for fork-origin workflows.

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
  The collector and validator come from B, so edits in the PR cannot alter their
  execution. The workflow itself still needs protected review. In production add
  a second eligible code owner for changes authored by the first owner. A single
  owner cannot approve their own normal PR review.
- Use `pull_request`, read-only tokens, pinned action SHAs and no shared Actions
  cache. Forks retain GitHub's workflow-run approval policy. Do not change this to
  `pull_request_target` and execute untrusted code.
- The MVP does **not** implement `merge_group`. Do not disable an existing merge
  queue to install it; first implement/test that integration. An absent queue
  check must block rather than silently pass.

The demo branch can enforce the final check with strict up-to-date validation.
Its single-owner setup demonstrates the approval UX; it is not proof of a
production policy against a malicious workflow author. No production branch
protections or upstream settings are changed by installing these files.

## Base updates, stacked PRs and reruns

The manifest fixes B, H, M, tool revision, workflow run ID/attempt and both report
hashes. The base is the actual PR target, including a parent branch for stacked
PRs. Merge parent first, retarget the child, update it and review its new run.
There is no dependency database or persistent approval reuse.

A new base/head/target invalidates the old candidate. A base push alone does not
schedule this PR workflow: update/rebase the PR or close/reopen it to start a
fresh run. Strict up-to-date protection prevents an old green check authorizing
an outdated merge.

GitHub's approval-history API does not expose the run attempt associated with an
approval. Therefore **Re-run jobs / Re-run all jobs are rejected in this MVP**.
For a fresh attempt, push a new commit (an empty commit works) or reopen the PR.
This deliberately trades convenience for unambiguous run-to-report approval.

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
  and readable Markdown. Other clients can supply the same JSON contract.
- `prepare.py`: binds a completed report to fixed Git commits and this fresh run.
- `gate.py`: small standard-library GitHub API validator; no persistent state.
- `.github/workflows/telemetry-contract.yml`: collection → approval → final check.

Contracts, delta, report and manifest exist only as per-run artifacts (30 days).
The approval is available in the GitHub run's review history.

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
