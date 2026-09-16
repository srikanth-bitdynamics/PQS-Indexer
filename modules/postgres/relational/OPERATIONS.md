# Relational projection operations

This backend currently exposes SQL views. It does not implement the proposal's HTTP Query API.

## Compatible upgrades

Apply the projection, run its resumable backfill, stop ingestion, and activate it. Activation requires backfill through the current published watermark. If ingestion advanced after backfill, catch up before activation. Start ingestion again after activation.

Writers and backfills bind the saved projection to the current package dictionary. Appended enum constructors are decoded using the extended case list. Reordered or removed constructors, renamed/repositioned fields, incompatible nullability, and incompatible Daml primitive types are rejected for an active promoted field. Unknown enum ordinals fail explicitly; they are never substituted with SQL NULL. Optional fields absent from older payloads remain nullable.

New projection definitions persist Daml type metadata as well as SQL types. Existing definitions without that metadata remain readable; compatibility is checked against the available package dictionary and stored enum cases.

Backfill holds the projection session lock across chunk commits. Concurrent apply and activate operations wait, and a second backfill fails with a retryable operator message. A failed backfill preserves previously committed chunks and releases its session lock. Retry the same draft after resolving the failure.

## Public view compatibility

Republishing uses `CREATE OR REPLACE VIEW`, retaining the view identity, owner, table and column grants, comments, and dependent views. Existing column positions remain stable. New columns are appended, including after `payload_json` when that column already exists. Consumers should name their columns explicitly.

Removing a published field or template is intentionally rejected. Leaving an orphaned view would expose typed values that the new writer no longer maintains. To narrow an internal query workload without changing the public contract, retain the published fields in the projection and reduce its configured queries/indexes.

A breaking public change requires a consumer migration during a maintenance window: identify dependent views and grants, migrate the consumers, explicitly remove the obsolete view using `DROP VIEW ... RESTRICT`, and activate the new projection. Reapply the intended grants to any newly created view. Activation never uses `CASCADE` and never drops consumers automatically. There is currently no dedicated CLI command for this breaking migration.

## Writer recovery

Use `--pipeline-ledger-start=Oldest` for resumable ingestion. On an empty datastore it starts at the ledger's available beginning; on retries it resumes at the published checkpoint. Explicit `Genesis` remains a fixed start request and the existing pipeline validator rejects it once it precedes the datastore's first checkpoint. Changing rights on retry creates a new coverage segment; it does not backfill earlier transactions for newly visible parties.

The writer holds a dedicated liveness connection. Each data transaction verifies that connection's PID and backend start time and holds a shared activity lock through commit. A successor writer, schema apply, and projection activation drain outstanding activity before proceeding. Loss of the liveness connection prevents later stale transactions from committing.

## Managed indexes and database migrations

Run build, validate/adopt, then retire. Existing index coverage is retained until every replacement validates. Retiring indexes needed by the current plan can be rebuilt/adopted. An invalid managed index is recreated only after its physical definition and identity match the registry; a same-name external replacement is rejected. Recreating an index records its new physical OID.

The relational backend is still in development and has a single initial migration, V001, including managed index physical identities (`physical_oid`). V001 has the standard copyright header. Recreate development databases initialized with an earlier revision of V001 before testing this consolidated schema.

PostgreSQL logical restores can change physical OIDs. After a restore, stop ingestion and reconcile managed index identities against actual definitions before allowing retirement. Do not blindly erase identities to force an adoption.

## Remaining production acceptance work

Four functional tests verify create/reassign/archive and ACS-seeded/archive lifecycles with both flat and tree streams on this branch. Reassignment does not globally archive the contract, and coverage flags remain partial. These tests do not establish complete assignment history or cover the non-causal event ordering added by newer upstream commits.

Relational pruning/redaction operations, document-to-relational migration tooling, upstream reassignment integration, and the proposal's scale benchmark remain separate deliverables. Backfill honors existing redaction markers, but that is not a complete redaction workflow. A complete functional rerun and measured workload results are required before release.
