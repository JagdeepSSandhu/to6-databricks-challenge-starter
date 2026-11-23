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

**Outputs:** output schema `training.to6_c3_aa72a91315d8` (derived from a hash of the workspace user). Feature and year tables are not built yet. The ratio definition is still to be added.

## Checks and decisions

Source references; initial rejection reference, affected-record export and reasons; replacement
checks; independently derived nonzero expected value; saved-table readback including zero income.

**Source references:** Unity Catalog table comments and the `source_lineage` view, printed in notebook section 1 with sample rows from both deliveries.

**Full-batch profile** (notebook section 1, all rows checked, not just samples):

| Batch | Rows (2022/2023/2024) | Missing required values | Duplicate return IDs | Zero income | Matches `parsed_source` |
| --- | --- | --- | --- | --- | --- |
| `returns_initial` | 13 (5/4/4) | 1 (R02, income NULL) | R01 appears twice (exact copy) | R03, R09 | No: 2 rows not upstream, 1 upstream row missing (R02, income 200) |
| `returns_corrected` | 12 (4/4/4) | 0 | none | R03, R09 | Yes, row for row |

**Decisions so far**
- Reject `returns_initial`: duplicate `return_id` R01 and a NULL required `income` on R02.
- `returns_corrected` is the candidate batch. The same checks still need to be run again as the formal acceptance step in notebook section 2.
- Keep zero-income returns (R03, R09). They are valid and must not be filtered out or treated as missing.
- Check uniqueness and join on `return_id`. Split year tables on `tax_year` (4 returns per year expected).

Still to do: affected-record export for the rejected batch, the independently derived nonzero expected value, and the saved-table readback.

## Two successful runs

For each run: notebook URL and execution timestamp, or Jobs URL/ID. Name the retained exports
or comparison evidence showing identical return IDs and values in the feature and year tables.
Explain how you checked year assignment, one copy of every accepted return and no appended copies.

## Final files and limitations

List final exports under outputs/, distinguishing practice files. State anything not verified.

- `outputs/connection-practice.json`: practice file (connection check only).
- `outputs/input-sample-rows.csv`: first 5 rows by `return_id` from `returns_initial` and from `returns_corrected` (10 rows, with a `source_table` column). The notebook reads it back and checks 10 rows, 5 per table.

**Not verified yet:** lineage was taken from the supplied view, not checked in the Catalog UI. No feature or year tables have been built, and the two successful runs have not been recorded.
