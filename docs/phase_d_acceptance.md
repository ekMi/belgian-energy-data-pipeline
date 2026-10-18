# Phase D acceptance and consolidated quality — D5

## Current behavior

Ingestion runs two quality scopes in the same observation/history transaction:
incoming_file (the extraction), then consolidated_range (stored values after
INSERT/UPDATE). Both reports and SUCCESS commit together. Existing reports are
never updated. No migration is needed beyond schema 04.

The incoming rules retain their ERROR/WARNING policy and fixed as_of. Partial
inputs disable incoming temporal continuity. All structural rules and other
quality rules still apply.

## Bounded consolidated scope

The consolidated value checks select exactly the business keys present in the
temporary incoming table, joined to their current stored values. They reuse
existing completeness, uniqueness, alignment, interval, negative-value and
median/MAD checks. They do not load the full observation table.

technical.consolidated_matches_input is ERROR if any incoming key is missing
from storage or any of its eight stored measures differs from the accepted input
(IS DISTINCT FROM). Its denominator is incoming keys. Existing insert/archive/
update count checks and SQL constraints remain in force.

technical.consolidated_continuity is WARNING when an actual adjacent stored
timestamp pair differs from 15 minutes. For each incoming key, indexed lateral
queries select its nearest stored predecessor and successor for the same
resolution. UNION deduplicates shared edges. Only these incident edges are
checked, and examples are capped at five.

This implements checks on affected ranges and their boundaries without comparing
sparse input rows directly. For example, input 00:15 and 00:45 causes no false gap
when stored 00:30 exists. If stored 00:30 really is absent, the actual 00:15->00:45
edge is reported. No missing coverage is inferred beyond the first/last stored
observation. Distant edges not touching incoming keys are not rechecked.

No global min-to-max range scan is performed for sparse inputs. The report
parameters record selected_key_count, min/max timestamps (bounding metadata,
not a claim that every key in the interval was selected), edge_count, value
selection policy and continuity policy. At most two edges per input key are
considered. A full historical input still entails full-input checks; there is
no claim of constant cost regardless of input size.

Outlier analysis remains a batch-sample heuristic with no seasonality adjustment;
it is not a global statistical model. All incoming rows, including UNCHANGED,
are checked, so old warnings can reappear in a new scoped report.

## Failure semantics

A consolidated ERROR or report persistence error rolls back inserts, updates,
correction history and success reports. FAILED and the incoming failure report
are persisted through the existing failure path. If consolidated evaluation
completed, its attempted-state report is appended with scope consolidated_range
and parameters.evaluation_state=rolled_back.

A report with evaluation_state=rolled_back describes an abandoned transactional
state, not current stored data. Reports committed with a successful merge use
evaluation_state=committed. If the checker fails before producing a report, no
consolidated report is fabricated. Failure-report persistence remains best effort
during connection/storage failures.

## Diagnostics after corrections

ingestion_batch_id identifies the producer of the current values. It no longer
identifies every row originally present in an export. Diagnostics now offer:

    .\.venv\Scripts\python.exe -B src/check_quality_ods001.py --batch-id 1 --mode historical

This reads and prints all saved runs with original identifiers, scopes, as_of,
parameters and results. It does not recompute or alter history; --persist is
rejected in historical mode. A batch with no saved runs returns an empty list.

    .\.venv\Scripts\python.exe -B src/check_quality_ods001.py --batch-id 1 --mode current --as-of 2026-10-18T12:00:00+00:00

This analyzes only current values still attributed to that successful batch.
The JSON explicitly labels current_batch_versions. Continuity is disabled
because producer-batch membership can be sparse. --no-continuity remains accepted
for CLI compatibility but has no additional effect.

A fully superseded batch can have zero current observations; its results have
zero evaluated rows, not an error or reconstruction of its original extraction.
Add --persist to append a current_batch_versions report, never to overwrite
old reports. Without --persist, both command modes use a read-only transaction.

Reproducing the original input requires the immutable raw export and hash;
saved historical reports remain the original evaluation evidence. Existing
stored_batch scopes are retained with their original meaning at evaluation time.

## Reproducible acceptance (test database only)

1. Configure PostgreSQL 18 and the dedicated disposable belgian_energy_test
   database using docs/integration_tests.md. Set all five TEST_PG variables.
   Do not substitute development PG variables. Never run test fixtures on
   belgian_energy.
2. Run the focused recipe:

       .\.venv\Scripts\python.exe -B -m pytest tests/integration/test_phase_d_acceptance.py -p no:cacheprovider -ra

3. Run the full regression suite and static checks:

       .\.venv\Scripts\python.exe -B -m pytest -p no:cacheprovider -ra
       .\.venv\Scripts\ruff.exe check --no-cache .
       .\.venv\Scripts\ruff.exe format --check --no-cache .

4. Require no PostgreSQL skips for database acceptance. The existing guarded
   fixtures verify the connected database, recreate only its disposable schemas,
   apply migrations 01-04 and clean up. Migration-04-specific tests retain their
   pre-migration setup.

The synthetic end-to-end recipe performs initial ingestion, an extended export,
unchanged replay with different bytes, an authorized correction, a denied
correction and replay of an old successful file. It verifies current values,
history, provenance, consolidated reports and unchanged original reports.
Additional cases check partial-input boundaries, repair of a real gap, exclusion
of distant history, superseded-batch diagnostics, post-merge ERROR rollback and
consolidated-report persistence failure. Existing concurrency, NULL transitions,
hash authorization and migration tests remain in the full suite.

The original 245 scenarios are retained. Assertions expecting one report per
successful ingestion are updated to expect incoming plus consolidated reports;
historical preservation assertions select all original batch runs.

## Implementation-session results and deferred work

The TEST_PG environment variables are absent in this session. PostgreSQL tests
are therefore skipped explicitly; they are not evidence of SQL or transaction
success. Unit tests and CLI help can be verified without a database.

Known limits:
- No API acquisition, orchestration or additional framework.
- No retrospective rewrite or automatic global quality scan.
- No duration repair or automatic recovery of stranded RUNNING batches.
- Publication grace period and statistical thresholds remain provisional.
- Cooperating ingestions serialize; external SQL writers must respect that policy.
- Large inputs still use Python memory for the existing exact quality statistics;
  lateral-neighbor performance should be measured on real PostgreSQL workloads.
- Saved reports characterize an evaluation scope/time, not immutable membership
  of every original CSV row in the current observation table.
