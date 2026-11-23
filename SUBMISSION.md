# My dataset submission

Complete this file in Databricks. Keep README.md as the course navigation page.

## Handoff summary

- **Input used:** `training.to6_challenge3_inputs.returns_corrected` (built from `training.to6_challenge3_inputs.parsed_source`, according to `source_lineage`). `returns_initial` was rejected.
- **Outputs:** `training.to6_c3_aa72a91315d8.return_features`, `.features_train` (2022), `.features_validation` (2023), `.features_test` (2024). Each has exactly `return_id`, `tax_year`, `deduction_to_income`.
- **Ratio:** `deduction_to_income = deduction / income` (double). When income is 0 the ratio is NULL and the return is kept.
- **Checks:** completeness and uniqueness on both batches; feature table readback; split readback (year assignment, one copy of every accepted return, values equal to `return_features`); two complete runs compared row by row.
- **Not verified:** see the last section.

## Inputs and outputs

Full input, lineage and output table names; what one row represents; ratio definition.

**Inputs** (schema `training.to6_challenge3_inputs`)
- `training.to6_challenge3_inputs.returns_initial`: initial delivery. Catalog comment says it has known delivery defects.
- `training.to6_challenge3_inputs.returns_corrected`: corrected delivery. Catalog comment says one row per return.
- `training.to6_challenge3_inputs.parsed_source`: upstream table for both deliveries.
- `training.to6_challenge3_inputs.source_lineage`: supplied lineage view. It maps both deliveries to `parsed_source` (created by SQL). It is a supplied map, not a Catalog UI lineage check.

**Row meaning:** one row is one tax return. `return_id` is the unique identifier. `taxpayer_id` is a filer who can appear in more than one year, so it is not a key.
Fields: `return_id`, `taxpayer_id`, `tax_year`, `income`, `deduction`. Amounts are nonnegative and zero income is valid (from the `parsed_source` Catalog comment).

**Outputs:** output schema `training.to6_c3_aa72a91315d8` (derived from a hash of the workspace user).
- `training.to6_c3_aa72a91315d8.batch_rejections_initial`: rejection evidence for `returns_initial` (3 rows, notebook section 2).
- `training.to6_c3_aa72a91315d8.return_features`: 12 rows, one per accepted return (notebook section 3).
- `training.to6_c3_aa72a91315d8.features_train`: 4 rows, `tax_year` 2022.
- `training.to6_c3_aa72a91315d8.features_validation`: 4 rows, `tax_year` 2023.
- `training.to6_c3_aa72a91315d8.features_test`: 4 rows, `tax_year` 2024.

All four tables have exactly `return_id`, `tax_year`, `deduction_to_income`. `taxpayer_id`, `income` and `deduction` are left out under the need-to-know rule.

**Ratio definition:** `deduction_to_income = deduction / income`, stored as a double. A zero-income return stays in the table with a NULL ratio; it is not filtered out and not set to zero.

## Checks and decisions

Source references; initial rejection reference, affected-record export and reasons; replacement
checks; independently derived nonzero expected value; saved-table readback including zero income.

**Source references:** Unity Catalog table comments and the `source_lineage` view, printed in notebook section 1 with sample rows from both deliveries.

**Full-batch profile** (notebook section 1, all rows checked, not just samples):

| Batch | Rows (2022/2023/2024) | Missing required values | Duplicate return IDs | Zero income | Matches `parsed_source` |
| --- | --- | --- | --- | --- | --- |
| `returns_initial` | 13 (5/4/4) | 1 (R02, income NULL) | R01 appears twice (exact copy) | R03, R09 | No: 2 rows not upstream, 1 upstream row missing (R02, income 200) |
| `returns_corrected` | 12 (4/4/4) | 0 | none | R03, R09 | Yes, row for row |

**Batch decision (notebook section 2, cell 10, run 2026-11-23 07:31 UTC)**

Both batches go through the same `check_batch` function, which applies two data-contract rules:
1. Completeness: every required column (`return_id`, `taxpayer_id`, `tax_year`, `income`, `deduction`) must be non-NULL.
2. Uniqueness: each `return_id` must appear only once.

| Batch | Rows | Distinct return_id | Rows with NULL required | Duplicate return_ids | Result |
| --- | --- | --- | --- | --- | --- |
| `returns_initial` | 13 | 12 | 1 | 1 | Fail (asserted) |
| `returns_corrected` | 12 | 12 | 0 | 0 | Pass (asserted) |

The cell asserts these outcomes, so it would stop if `returns_initial` unexpectedly passed or `returns_corrected` failed.

**Initial rejection reference and reasons**
- Rejected: `training.to6_challenge3_inputs.returns_initial`.
- Completeness: R02 has NULL `income` (the upstream value is 200).
- Uniqueness: R01 appears twice as an exact duplicate row.
- Affected records (3 rows, each tagged with `rejection_rule`) are saved to `training.to6_c3_aa72a91315d8.batch_rejections_initial` and exported to `outputs/batch-rejections-initial.csv`.
- The decision and both check results are recorded in `outputs/batch-decision.json` (`"decision": "USE returns_corrected"`).

**Replacement checks:** `returns_corrected` passes both rules (12 unique return_ids, no NULLs) and matches `parsed_source` row for row (section 1). It is the batch used for the feature table in section 3.

**Carried forward**
- Keep zero-income returns (R03, R09). They are valid and must not be filtered out or treated as missing.
- Check uniqueness and join on `return_id`. Split year tables on `tax_year` (4 returns per year expected).

**Feature table checks (notebook section 3, read back from the saved table)**
- Schema is exactly `return_id`, `tax_year`, `deduction_to_income`.
- 12 rows, equal to the 12 distinct accepted `return_id`s; no duplicate `return_id`.
- Nonzero expected value derived from the source: R01 has deduction 20 and income 100 in `returns_corrected`, so the expected ratio is 20 / 100 = 0.2. The saved value is 0.2.
- Zero-income cases: R03 (2022) and R09 (2024) are present with a NULL ratio.

**Year split checks (notebook section 4-A, read back from the saved tables)**
- The split tables are built from the saved `return_features` table, written in overwrite mode.
- Before writing, the cell checks that no feature row has a `tax_year` outside 2022 to 2024, so no return can be left out.
- Each split holds only its own year (0 rows in the wrong year) and has no duplicate `return_id`.
- Across the three splits: 12 rows and 12 distinct `return_id`s, so no return is in more than one split. `exceptAll` in both directions against `return_features` returns 0 rows, so IDs, years and values match exactly.
- Zero-income R03 is in `features_train` and R09 is in `features_test`, both with a NULL ratio.

| Split table | Year | Rows | NULL ratio rows |
| --- | --- | --- | --- |
| `features_train` | 2022 | 4 | 1 (R03) |
| `features_validation` | 2023 | 4 | 0 |
| `features_test` | 2024 | 4 | 1 (R09) |

## Two successful runs

For each run: notebook URL and execution timestamp, or Jobs URL/ID. Name the retained exports
or comparison evidence showing identical return IDs and values in the feature and year tables.
Explain how you checked year assignment, one copy of every accepted return and no appended copies.

Both runs were complete top-to-bottom interactive runs of the notebook (every code cell, sections 1 to 4) and both finished without errors. No Jobs were used.

| Run | Notebook URL | Execution timestamp (UTC) | Snapshot file | Snapshot SHA-256 |
| --- | --- | --- | --- | --- |
| 1 | https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866559883 | 2026-11-23T07:49:21+00:00 | `outputs/runs/run-previous.csv` | `920db8546e8dea08e506e347c86ed49253fc000f707e592d4ad6466d95d55e1f` |
| 2 | https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866559883 | 2026-11-23T07:51:03+00:00 | `outputs/runs/run-latest.csv` | `920db8546e8dea08e506e347c86ed49253fc000f707e592d4ad6466d95d55e1f` |

**Evidence:** at the end of each run, cell 4-B reads all four saved tables back from the Catalog and writes every row (table name, `return_id`, `tax_year`, `deduction_to_income`) to a snapshot CSV. The previous snapshot is kept as `run-previous.csv`. The cell then compares the two files value by value and asserts they are identical. The result is in `outputs/runs/run-comparison.json`: 24 rows compared, same return IDs per table, identical IDs and values, same row counts. The two snapshot files are also byte-identical (same SHA-256). Run metadata is in `outputs/runs/run-log.json`.

**How each property was checked**
- Year assignment: each split table is read back and must have 0 rows outside its year (asserted on both runs).
- One copy of every accepted return: the three splits together have 12 rows and 12 distinct IDs, and match `return_features` exactly in both directions; `return_features` has the same 12 IDs as `returns_corrected`.
- No appended copies: every table is written in overwrite mode. After run 2, row counts are still 12 / 4 / 4 / 4, the same as after run 1, and the snapshots match. Delta history shows only `CREATE OR REPLACE TABLE AS SELECT` operations for the split tables (all versions) and for versions 2 to 4 of `return_features` (version 2 from the earlier section 3 fix run, versions 3 and 4 from runs 1 and 2), with no appends. Older `return_features` versions were not inspected.
- A separate check outside the notebook joined `returns_corrected` to the union of the three splits on `return_id` and `tax_year` and found 0 unmatched rows.

## Final files and limitations

List final exports under outputs/, distinguishing practice files. State anything not verified.

- `outputs/connection-practice.json`: practice file (connection check only).
- `outputs/input-sample-rows.csv`: first 5 rows by `return_id` from `returns_initial` and from `returns_corrected` (10 rows, with a `source_table` column). The notebook reads it back and checks 10 rows, 5 per table.
- `outputs/batch-rejections-initial.csv`: the 3 affected records from `returns_initial` with their rejection rule.
- `outputs/batch-decision.json`: batch decision, reason and the check results for both batches.
- `outputs/return_features.csv`: the saved `return_features` table (12 rows).
- `outputs/features_train.csv`, `outputs/features_validation.csv`, `outputs/features_test.csv`: the saved split tables (4 rows each).
- `outputs/runs/run-previous.csv`, `outputs/runs/run-latest.csv`: the IDs-and-values snapshots of runs 1 and 2.
- `outputs/runs/run-comparison.json`, `outputs/runs/run-log.json`: the comparison result and run metadata.

All exports are rewritten on every complete run, so they always reflect the latest run.

**Not verified or limitations**
- Lineage comes from the supplied `source_lineage` view; it was not checked in the Catalog UI.
- The completeness check also requires `taxpayer_id`, which CHALLENGE.md does not list as required. No row in either batch is missing `taxpayer_id`, so the result is the same.
- In `run-log.json` and `run-comparison.json`, the run 1 entry shows `snapshot_file` as `run-latest.csv` because it was recorded before that file was renamed. Run 1's snapshot is now `outputs/runs/run-previous.csv`.
- These checks do not show that every source of model leakage is gone; no labels or prediction-time availability were assessed.
- The commit and push to GitHub, and checking that the commit contains the notebook, this file and the exports, still need to be done.
