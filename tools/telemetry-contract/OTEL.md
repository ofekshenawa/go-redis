# OTel negative tests for go-redis

This method compares telemetry in the PR target with telemetry in the merge
candidate. It uses the same Go scanner from the target branch for both trees.
The shared review workflow publishes the result under
**Negative tests / telemetry-contract**.

## Demo stack

| PR target | What to review |
| --- | --- |
| `master` | `negative-tests/infrastructure` provides the shared review workflow, PR comments and the telemetry-review Environment integration. |
| `negative-tests/infrastructure` | `negative-tests/otel` adds the static Go scanner, comparison and read-only collection workflow. CI tests the scanner and scans the repository on this installation PR. |
| `negative-tests/otel` | [#8](https://github.com/ofekshenawa/go-redis/pull/8) changes documentation, [#9](https://github.com/ofekshenawa/go-redis/pull/9) adds a metric, and [#16](https://github.com/ofekshenawa/go-redis/pull/16) demonstrates reuse of an approval within one PR and target. |

The three demo PRs are siblings. To present another method, branch from
`negative-tests/infrastructure` and target that method branch with its own demo
PRs. The fork's default branch, `telemetry-demo/base`, runs the trusted
publisher for the live demo. The reviewer is configured in the repository’s
`telemetry-review` Environment. Retargeting a demo starts a new approval
history for that target.

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
