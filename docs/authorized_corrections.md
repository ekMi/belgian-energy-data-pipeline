# Authorized and historized corrections — D4

## Default and authorization

The original ingestion command still refuses every CHANGED observation unless
the caller supplies both --authorize-sha256 and --correction-reason. A reason
without a hash, a malformed hash, whitespace-only justification or naive
--extracted-at timestamp is rejected before connecting.

The authorization must be exactly 64 lowercase hexadecimal characters and match
the input bytes. The match is checked under the session ingestion lock, before
deduplication or creation of a batch, including for an already successful file.
A wrong hash creates no batch and changes no data. Permission is a caller
declaration, not an identity/authentication system.

Authorization applies to all actual differences in those exact file bytes,
including transitions from a nullable value to NULL. Review those transitions
explicitly before approving. No values are filled, coalesced or rounded.

## Preview and example

Use the existing read-only preview to review candidates and obtain file_sha256:

    .\.venv\Scripts\python.exe -B src/merge_ods001.py data/raw/corrected_export.csv --sample-size 100

The sample is bounded and chronological, not necessarily exhaustive. Counts
describe the whole input. A preview is not a reservation: classification and
freshness are recalculated under the ingestion lock.

Example: the database has Total Load=10000 for an observation, while a reviewed
Elia export corrects it to 10125. After inspecting that file, copy its exact hash
from the preview into the command below (replace the placeholder; do not approve
a newly computed hash blindly). Execute only against the intended database:

    .\.venv\Scripts\python.exe -B src/ingest_ods001.py data/raw/corrected_export.csv --input-scope partial --authorize-sha256 <reviewed-64-character-sha256> --correction-reason "Reviewed Elia export: Total Load 10000 to 10125 MW" --extracted-at 2026-10-18T10:00:00+00:00 --source-reference "Reviewed ODS001 export"

The old 10000 and new 10125 values, all seven other old/new measures, the previous
producer batch, correcting batch and clock timestamp are archived. Only that
observation's ingestion_batch_id changes. An otherwise identical observation
retains its values and provenance.

The command uses existing PG environment configuration. Tests exclusively use
guarded belgian_energy_test fixtures. No new migration is needed; schema 04 is
required and its scripts are unchanged.

## Freshness policy and limits

extracted_at is declared metadata, not proof of upstream publication or freshness.
A newer date never authorizes a correction by itself. A valid file-bound hash
and justification are always required for CHANGED.

When an incoming extracted_at is supplied, reject the entire lot if it predates
any known extraction date associated with the current version or its correction
history for a changed key. Checking history preserves known evidence even if an
intermediate authorized correction had no extraction date. Equality is allowed
only with explicit authorization.

When dates are unknown, the program cannot establish age: explicit authorization
is accepted as a manual review decision, with the original uncertainty retained
as NULL. Do not invent extraction dates. Unchanged observations do not update
provenance or advance a per-observation freshness watermark. Source revision
identifiers and independently verified freshness await API acquisition.

A file already recorded as SUCCESS remains a no-op, even after later corrections;
replaying it does not restore old values. An incorrect authorization still raises
an error. A different old file not previously loaded is checked under the policy
above; unknown age cannot be inferred from its filesystem timestamp.

## Transaction sequence

The existing ODS001 session advisory lock spans hashing, SUCCESS deduplication,
RUNNING commit, classification, mutation and failure tracking.

Within the data transaction:
1. COPY into the shared temporary table and classify using IS DISTINCT FROM.
2. Evaluate quality; reject unauthorized changes and blocking quality errors.
3. Lock the current changed rows FOR UPDATE and check known extraction dates.
4. INSERT old/new typed values into staging.elia_load_correction.
5. UPDATE using this batch's history rows, verifying old values and old producer
   still match with IS NOT DISTINCT FROM.
6. INSERT NEW observations, preserve UNCHANGED observations.
7. Check archived/updated/inserted counts and commit quality reports, counters and
   SUCCESS together.

rows_received = rows_inserted + rows_unchanged + rows_updated = rows_loaded.
rows_updated counts actual corrections, not history attempts. Errors after
archiving or updating roll back history, updates, insertions and success reports.
FAILED is then persisted through the existing separate transaction, with zero
committed action counters. Prior quality reports are never rewritten.

Session advisory locking serializes cooperating ingestions; row locks protect
the archived versions while updating. External writers ignoring the advisory
protocol are not fully coordinated with classification. No rows are deleted.
Connection loss or process termination may still leave RUNNING as documented
in step C/D3.

Historical quality reports remain valid snapshots. Selecting current rows by
an old ingestion_batch_id no longer reconstructs that batch after corrections;
consult saved reports and raw exports for original input analysis.

## Tests and current verification

    .\.venv\Scripts\python.exe -B -m pytest -p no:cacheprovider -ra
    .\.venv\Scripts\ruff.exe check --no-cache .
    .\.venv\Scripts\ruff.exe format --check --no-cache .

The 215 D3 cases are retained. Additional tests cover all eight individually
changed measures, both nullable columns in both directions, bound authorization,
multiple/successive changes, old SUCCESS replay, known stale inputs, retained
date evidence, full old/new history, provenance/report preservation, rollback
after history/during UPDATE/after UPDATE/during report persistence and concurrent
corrections that recheck freshness after waiting for the session lock.

The implementation session lacks TEST_PG configuration. PostgreSQL tests are
explicitly skipped, not reported as passed; real SQL and concurrency validation
must be completed against the configured test database.
