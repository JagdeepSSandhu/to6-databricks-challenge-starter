# My dataset submission

Complete this file in Databricks. Keep README.md as the course navigation page.

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
- Feature and year tables are not built yet. The ratio definition is still to be added.

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

Still to do: the independently derived nonzero expected value and the saved-table readback (including zero income).

## Two successful runs

For each run: notebook URL and execution timestamp, or Jobs URL/ID. Name the retained exports
or comparison evidence showing identical return IDs and values in the feature and year tables.
Explain how you checked year assignment, one copy of every accepted return and no appended copies.

## Final files and limitations

List final exports under outputs/, distinguishing practice files. State anything not verified.

- `outputs/connection-practice.json`: practice file (connection check only).
- `outputs/input-sample-rows.csv`: first 5 rows by `return_id` from `returns_initial` and from `returns_corrected` (10 rows, with a `source_table` column). The notebook reads it back and checks 10 rows, 5 per table.
- `outputs/batch-rejections-initial.csv`: the 3 affected records from `returns_initial` with their rejection rule.
- `outputs/batch-decision.json`: batch decision, reason and the check results for both batches.

**Not verified yet:** lineage was taken from the supplied view, not checked in the Catalog UI. No feature or year tables have been built, and the two successful runs have not been recorded.
