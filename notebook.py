# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Prepare the next team's dataset
# MAGIC Follow CHALLENGE.md. These headings are a starting point; implement and verify each stage yourself.

# COMMAND ----------

import hashlib

INPUT_SCHEMA = "training.to6_challenge3_inputs"
INITIAL_TABLE = f"{INPUT_SCHEMA}.returns_initial"
CORRECTED_TABLE = f"{INPUT_SCHEMA}.returns_corrected"
LINEAGE_VIEW = f"{INPUT_SCHEMA}.source_lineage"
workspace_user = spark.sql("SELECT current_user() AS username").first()["username"]
OUTPUT_SCHEMA = "training.to6_c3_" + hashlib.sha256(workspace_user.encode("utf-8")).hexdigest()[:12]
print({"input_schema": INPUT_SCHEMA, "output_schema": OUTPUT_SCHEMA})

# COMMAND ----------

from pathlib import Path
import json

# Copy this Git folder's absolute path from its Workspace menu; include /Workspace/.
repo_root = Path("/Workspace/Users/jagdeep.s.sandhu.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
practice_result = spark.sql("SELECT 1 AS connection_check").first().asDict()
assert practice_result == {"connection_check": 1}
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/connection-practice.json").write_text(
    json.dumps(practice_result) + "\n", encoding="utf-8"
)

# COMMAND ----------

OUTPUT_SCHEMA

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understand the input
# MAGIC Record Catalog descriptions, sample observations and the supplied lineage source.

# COMMAND ----------

# DBTITLE 1,Source references: lineage view, Catalog comments, sample rows
import pyspark.sql.functions as F
# Source references: supplied lineage view and Catalog table comments
lineage = spark.table(LINEAGE_VIEW)
display(lineage)

table_comments = {
    t: spark.sql(f"DESCRIBE TABLE EXTENDED {t}")
        .where(F.col("col_name") == "Comment").select("data_type").first()["data_type"]
    for t in [INITIAL_TABLE, CORRECTED_TABLE, f"{INPUT_SCHEMA}.parsed_source"]
}

print(table_comments)
print("\n")

for t in [INITIAL_TABLE, CORRECTED_TABLE]:
    print(f"sample rows: {t}")
    display(spark.table(t).orderBy("return_id").limit(5))

# COMMAND ----------

# DBTITLE 1,Full-batch profile: years, missing values, duplicates, zero income
# Full-batch profile of both delivered tables (all rows, not just the samples above)
REQUIRED_COLS = ["return_id", "taxpayer_id", "tax_year", "income", "deduction"]
UPSTREAM_TABLE = f"{INPUT_SCHEMA}.parsed_source"

def profile(table_name):
    df = spark.table(table_name)
    any_missing = F.lit(False)
    for c in REQUIRED_COLS:
        any_missing = any_missing | F.col(c).isNull()

    print(f"===== {table_name} =====")
    print("Returns by year (NULL tax_year row = whole-batch total)")
    display(
        df.rollup("tax_year").agg(
            F.count("*").alias("rows"),
            F.countDistinct("return_id").alias("distinct_return_ids"),
            F.sum(any_missing.cast("int")).alias("rows_missing_required"),
            F.sum((F.col("income") == 0).cast("int")).alias("zero_income_rows"),
        ).orderBy(F.col("tax_year").asc_nulls_last())
    )

    print("Missing required values by column")
    display(df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in REQUIRED_COLS]))
    display(df.where(any_missing))

    print("Duplicate return_id values")
    dup_ids = df.groupBy("return_id").count().where("count > 1")
    display(df.join(dup_ids.select("return_id"), "return_id").orderBy("return_id"))

    print("Zero-income records (valid per Catalog description; must be kept)")
    display(df.where(F.col("income") == 0).orderBy("return_id"))

    # Row-level comparison with the upstream source named in the lineage view
    up = spark.table(UPSTREAM_TABLE)
    print({
        "rows_not_in_upstream": df.exceptAll(up).count(),
        "upstream_rows_missing_from_delivery": up.exceptAll(df).count(),
    })

for t in [INITIAL_TABLE, CORRECTED_TABLE]:
    profile(t)


# COMMAND ----------

# DBTITLE 1,Source reference and findings
# MAGIC %md
# MAGIC ### Source reference and input findings
# MAGIC **Sources:** Unity Catalog table comments on `training.to6_challenge3_inputs` tables and the supplied view `source_lineage` (a supplied map, not a Catalog UI lineage check).
# MAGIC * **Upstream:** both `returns_initial` and `returns_corrected` were created by SQL from `parsed_source`.
# MAGIC * **Row meaning:** one row = one tax return. **Unique identifier:** `return_id`. `taxpayer_id` identifies a filer and can recur across years, so it is *not* a key.
# MAGIC * **Relevant fields:** `return_id`, `taxpayer_id`, `tax_year`, `income`, `deduction` (all INT except the IDs). Amounts are nonnegative; **zero income is valid**.
# MAGIC
# MAGIC **Full-batch results (cell above)**
# MAGIC
# MAGIC | Batch | Rows | Distinct return_id | 2022 / 2023 / 2024 rows | Missing required | Duplicate IDs | Zero income |
# MAGIC | --- | --- | --- | --- | --- | --- | --- |
# MAGIC | returns_initial | 13 | 12 | 5 / 4 / 4 | 1 (R02 income NULL) | R01 appears twice (exact copy) | 2 (R03, R09) |
# MAGIC | returns_corrected | 12 | 12 | 4 / 4 / 4 | 0 | none | 2 (R03, R09) |
# MAGIC
# MAGIC **How this affects the next steps**
# MAGIC * `returns_initial` fails the contract (duplicate `return_id` R01 and a NULL required `income` on R02); keep this as rejection evidence in Section 2. Against `parsed_source` it has 2 rows not upstream (extra R01 copy, R02 with NULL income) and is missing 1 upstream row (R02, income 200).
# MAGIC * `returns_corrected` passes the same checks and matches `parsed_source` row-for-row, so it is the candidate batch; re-run these same checks on it in Section 2 before using it.
# MAGIC * Zero-income returns (R03, R09) are legitimate and must **not** be filtered out or treated as missing in transformations or features.
# MAGIC * Uniqueness checks and joins must use `return_id`, not `taxpayer_id`; year tables should split on `tax_year` (4 returns per year expected).

# COMMAND ----------

# DBTITLE 1,Export 5 sample rows per batch to outputs CSV
# Export the first 5 rows (ordered by return_id) of each delivered batch to one CSV
sample_frames = [
    spark.table(t).orderBy("return_id", "tax_year").limit(5)
        .select(F.lit(t).alias("source_table"), "*")
    for t in [INITIAL_TABLE, CORRECTED_TABLE]
]
sample_pdf = sample_frames[0].unionByName(sample_frames[1]).toPandas()
# Keep integer amounts as integers in the CSV (the NULL income would otherwise turn the column into 100.0 style floats)
sample_pdf = sample_pdf.astype({"tax_year": "Int64", "income": "Int64", "deduction": "Int64"})

sample_csv = repo_root / "outputs" / "input-sample-rows.csv"
sample_pdf.to_csv(sample_csv, index=False)

# Read back to verify what was written
import pandas as pd
check = pd.read_csv(sample_csv)
print(sample_csv.read_text())
assert len(check) == 10 and check["source_table"].value_counts().eq(5).all()
print(sample_csv)
display(check)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Decide which batch is usable
# MAGIC Keep the initial rejection evidence and apply the same checks to the replacement.

# COMMAND ----------

import json
from pyspark.sql import functions as F

# ── Data-contract rules ─────────────────────────────────────────────
# 1. COMPLETENESS – every required column must be non-NULL.
# 2. UNIQUENESS   – return_id must be unique (no duplicate rows).
REQUIRED_COLS = ["return_id", "taxpayer_id", "tax_year", "income", "deduction"]

def check_batch(table_name):
    """Run completeness & uniqueness checks. Return (pass/fail, details dict, rejection_df or None)."""
    df = spark.table(table_name)
    issues = []
    rejection_frames = []

    # ── Rule 1: Completeness ──────────────────────────────────────
    any_null = F.lit(False)
    for c in REQUIRED_COLS:
        any_null = any_null | F.col(c).isNull()
    missing_df = df.where(any_null)
    n_missing = missing_df.count()
    if n_missing > 0:
        issues.append(f"COMPLETENESS failure: {n_missing} row(s) with NULL required value(s)")
        rejection_frames.append(
            missing_df.select("*").withColumn("rejection_rule", F.lit("COMPLETENESS – NULL in required column"))
        )

    # ── Rule 2: Uniqueness on return_id ───────────────────────────
    dup_ids = df.groupBy("return_id").count().where("count > 1")
    n_dups = dup_ids.count()
    if n_dups > 0:
        dup_rows = df.join(dup_ids.select("return_id"), "return_id")
        issues.append(f"UNIQUENESS failure: {n_dups} return_id(s) appear more than once")
        rejection_frames.append(
            dup_rows.select("*").withColumn("rejection_rule", F.lit("UNIQUENESS – duplicate return_id"))
        )

    # ── Combine rejections ────────────────────────────────────────
    if rejection_frames:
        from functools import reduce
        rejections = reduce(lambda a, b: a.unionByName(b), rejection_frames)
    else:
        rejections = None

    passed = len(issues) == 0
    detail = {
        "table": table_name,
        "total_rows": df.count(),
        "distinct_return_ids": df.select("return_id").distinct().count(),
        "rows_with_null_required": n_missing,
        "duplicate_return_ids": n_dups,
        "passed": passed,
        "issues": issues,
    }
    return passed, detail, rejections

# ═══════════════════════════════════════════════════════════════════
# 2-A  Assess returns_initial  (expect REJECTION)
# ═══════════════════════════════════════════════════════════════════
initial_ok, initial_detail, initial_rejections = check_batch(INITIAL_TABLE)
print("── returns_initial ──")
print(json.dumps(initial_detail, indent=2))
assert not initial_ok, "Expected returns_initial to FAIL the data contract"

print("\nRejected records (initial batch):")
display(initial_rejections.orderBy("return_id"))

# Persist rejection evidence so it survives re-runs and is available for export
initial_rejections.orderBy("return_id").write.mode("overwrite").saveAsTable(
    f"{OUTPUT_SCHEMA}.batch_rejections_initial"
)
print(f"Rejection evidence saved → {OUTPUT_SCHEMA}.batch_rejections_initial")

# ═══════════════════════════════════════════════════════════════════
# 2-B  Assess returns_corrected  (expect PASS)
# ═══════════════════════════════════════════════════════════════════
corrected_ok, corrected_detail, corrected_rejections = check_batch(CORRECTED_TABLE)
print("\n── returns_corrected ──")
print(json.dumps(corrected_detail, indent=2))
assert corrected_ok, (
    f"returns_corrected also failed the data contract: {corrected_detail['issues']}"
)
print("\n✅ returns_corrected passes both COMPLETENESS and UNIQUENESS checks.")
print("   → This batch will be used for the feature table in Section 3.")

# ═══════════════════════════════════════════════════════════════════
# 2-C  Export rejection evidence to the Git repo outputs folder
# ═══════════════════════════════════════════════════════════════════
rej_pdf = spark.table(f"{OUTPUT_SCHEMA}.batch_rejections_initial").toPandas()
rej_pdf = rej_pdf.astype({"tax_year": "Int64", "income": "Int64", "deduction": "Int64"})

rej_csv = repo_root / "outputs" / "batch-rejections-initial.csv"
rej_pdf.to_csv(rej_csv, index=False)

import pandas as pd
check = pd.read_csv(rej_csv)
print(f"\nRejection CSV written ({len(check)} rows): {rej_csv}")
print(rej_csv.read_text())

# Summary JSON for downstream evidence
decision = {
    "decision": "USE returns_corrected",
    "reason": (
        "returns_initial violates the data contract: "
        "(1) COMPLETENESS – return R02 has NULL income (a required field); "
        "(2) UNIQUENESS – return_id R01 appears twice (exact duplicate row). "
        "These records are preserved in batch_rejections_initial and the CSV export. "
        "returns_corrected passes both rules with 12 unique return_ids and no NULLs."
    ),
    "initial_check": initial_detail,
    "corrected_check": corrected_detail,
}
decision_path = repo_root / "outputs" / "batch-decision.json"
decision_path.write_text(json.dumps(decision, indent=2) + "\n", encoding="utf-8")
print(f"\nDecision JSON written: {decision_path}")
print(json.dumps(decision, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# DBTITLE 1,Build, save and verify feature table
from pyspark.sql import functions as F

# ═══════════════════════════════════════════════════════════════════
# 3-A  Build the feature table from the accepted batch
# ═══════════════════════════════════════════════════════════════════
# Source: returns_corrected (passed data-contract checks in Section 2)
# Columns kept: return_id, tax_year, deduction_to_income (derived)
# Dropped:      taxpayer_id (filer identifier — need-to-know rule)
#               income, deduction (raw amounts — need-to-know rule)
# Zero-income handling: deduction / 0 → NULL (not filtered out)

corrected_df = spark.table(CORRECTED_TABLE)

features_df = (
    corrected_df
    .withColumn(
        "deduction_to_income",
        F.when(F.col("income") == 0, F.lit(None).cast("double"))
         .otherwise((F.col("deduction") / F.col("income")).cast("double")),
    )
    .select("return_id", "tax_year", "deduction_to_income")
)

# Quick sanity display before saving
print("Feature table preview (all rows):")
display(features_df.orderBy("return_id"))

# ═══════════════════════════════════════════════════════════════════
# 3-B  Save as a managed Delta table
# ═══════════════════════════════════════════════════════════════════
FEATURE_TABLE = f"{OUTPUT_SCHEMA}.return_features"

features_df.write.mode("overwrite").saveAsTable(FEATURE_TABLE)
print(f"\n✅ Feature table saved → {FEATURE_TABLE}")

# ═══════════════════════════════════════════════════════════════════
# 3-C  Verify by reading the saved table back
# ═══════════════════════════════════════════════════════════════════
saved_df = spark.table(FEATURE_TABLE)
saved_rows = saved_df.count()
saved_cols = set(saved_df.columns)

# -- Schema check: exactly 3 columns, no PII or raw amounts
assert saved_cols == {"return_id", "tax_year", "deduction_to_income"}, (
    f"Unexpected columns: {saved_cols}"
)
print(f"Schema check passed: {sorted(saved_cols)}")

# -- Row count: must equal the accepted batch (12 unique returns)
expected_count = corrected_df.select("return_id").distinct().count()  # 12
assert saved_rows == expected_count, (
    f"Row count mismatch: saved {saved_rows}, expected {expected_count}"
)
print(f"Row count check passed: {saved_rows} rows (matches {expected_count} accepted returns)")

# -- Spot-check a nonzero ratio against a value derived from source
# Pick a known return from returns_corrected and compute expected ratio
sample_row = (
    corrected_df
    .where((F.col("income") > 0) & (F.col("deduction") > 0))
    .orderBy("return_id")
    .first()
)
expected_ratio = round(sample_row["deduction"] / sample_row["income"], 10)
saved_ratio = round(
    saved_df.where(F.col("return_id") == sample_row["return_id"])
    .first()["deduction_to_income"],
    10,
)
assert abs(saved_ratio - expected_ratio) < 1e-9, (
    f"Ratio mismatch for {sample_row['return_id']}: "
    f"saved {saved_ratio}, expected {expected_ratio}"
)
print(
    f"Spot-check passed: {sample_row['return_id']} → "
    f"deduction/income = {sample_row['deduction']}/{sample_row['income']} = "
    f"{expected_ratio} (saved: {saved_ratio})"
)

# -- Zero-income check: these returns must exist with NULL ratio
zero_income_ids = [
    r["return_id"]
    for r in corrected_df.where(F.col("income") == 0)
    .select("return_id")
    .orderBy("return_id")
    .collect()
]
assert len(zero_income_ids) > 0, "Expected at least one zero-income return in source"

for rid in zero_income_ids:
    row = saved_df.where(F.col("return_id") == rid).first()
    assert row is not None, f"Zero-income return {rid} missing from feature table"
    assert row["deduction_to_income"] is None, (
        f"{rid} should have NULL ratio but got {row['deduction_to_income']}"
    )
print(
    f"Zero-income check passed: {zero_income_ids} present with NULL deduction_to_income"
)

# -- No duplicates
assert saved_df.groupBy("return_id").count().where("count > 1").count() == 0, (
    "Duplicate return_ids found in saved feature table"
)
print("Uniqueness check passed: no duplicate return_ids")

print(f"\n✅ All verification checks passed for {FEATURE_TABLE}")
display(saved_df.orderBy("return_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# DBTITLE 1,4-A Save year split tables, verify and export
from pyspark.sql import functions as F
from functools import reduce

# ═══════════════════════════════════════════════════════════════════
# 4-A  Year split tables built from the SAVED feature table
# ═══════════════════════════════════════════════════════════════════
FEATURE_TABLE = f"{OUTPUT_SCHEMA}.return_features"
SPLITS = {"features_train": 2022, "features_validation": 2023, "features_test": 2024}
FEATURE_COLS = ["return_id", "tax_year", "deduction_to_income"]

features = spark.table(FEATURE_TABLE)

# Guard: every saved feature row must belong to exactly one split year
unassigned = features.where(F.col("tax_year").isNull() | ~F.col("tax_year").isin(list(SPLITS.values())))
assert unassigned.count() == 0, "Feature rows with a tax_year outside 2022-2024 would be lost"

# Overwrite (never append) so a rerun replaces the previous contents
for name, year in SPLITS.items():
    (features.where(F.col("tax_year") == year).select(FEATURE_COLS)
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{OUTPUT_SCHEMA}.{name}"))
    print(f"Saved {OUTPUT_SCHEMA}.{name} (tax_year = {year})")

# ── Read back and verify ───────────────────────────────────────────
split_dfs = {name: spark.table(f"{OUTPUT_SCHEMA}.{name}") for name in SPLITS}
summary = []
for name, year in SPLITS.items():
    df = split_dfs[name]
    assert df.columns == FEATURE_COLS, f"{name} columns {df.columns}"
    wrong_year = df.where(F.col("tax_year") != year).count()
    dups = df.groupBy("return_id").count().where("count > 1").count()
    assert wrong_year == 0, f"{name}: {wrong_year} rows not in {year}"
    assert dups == 0, f"{name}: duplicate return_ids"
    summary.append((name, year, df.count(), df.where(F.col("deduction_to_income").isNull()).count()))

union_splits = reduce(lambda a, b: a.unionByName(b), split_dfs.values())
n_features = features.count()
n_accepted = spark.table(CORRECTED_TABLE).select("return_id").distinct().count()
assert n_features == n_accepted, "Feature table does not hold every accepted return once"
assert union_splits.count() == n_features, "Split row total differs from feature table"
assert union_splits.select("return_id").distinct().count() == n_features, "A return_id appears in more than one split"
assert union_splits.exceptAll(features).count() == 0, "Split rows not found in return_features"
assert features.exceptAll(union_splits).count() == 0, "return_features rows missing from the splits"
print(f"\nAll {n_accepted} accepted returns appear exactly once across the splits, each in its assigned year, "
      "with values identical to return_features.")

display(spark.createDataFrame(summary, "split_table string, tax_year int, rows long, null_ratio_rows long"))
print("Zero-income returns in the splits (expect NULL ratio):")
display(union_splits.where(F.col("deduction_to_income").isNull()).orderBy("return_id"))

# ── Small CSV exports of the saved tables (read back from Catalog) ─
export_tables = {"return_features": features, **split_dfs}
for name, df in export_tables.items():
    path = repo_root / "outputs" / f"{name}.csv"
    df.orderBy("return_id").toPandas().to_csv(path, index=False)
    print(f"Exported {path.name}: {len(path.read_text().splitlines()) - 1} rows")

# COMMAND ----------

# DBTITLE 1,4-B Run snapshot and two-run comparison
import hashlib, json
from datetime import datetime, timezone
import pandas as pd

# ═══════════════════════════════════════════════════════════════════
# 4-B  Snapshot this run's IDs and values; compare with the previous run
# ═══════════════════════════════════════════════════════════════════
# Each complete run writes outputs/runs/run-latest.csv; the prior latest becomes run-previous.csv.
run_dir = repo_root / "outputs" / "runs"
run_dir.mkdir(exist_ok=True)
latest, previous = run_dir / "run-latest.csv", run_dir / "run-previous.csv"
log_path = run_dir / "run-log.json"
run_log = json.loads(log_path.read_text()) if log_path.exists() else {}

if latest.exists():
    latest.replace(previous)
    run_log["previous"] = run_log.get("latest")

# Snapshot every saved output table (all rows, all columns), read back from the Catalog
snapshot = reduce(
    lambda a, b: a.unionByName(b),
    [spark.table(f"{OUTPUT_SCHEMA}.{t}").select(F.lit(t).alias("table_name"), *FEATURE_COLS)
     for t in ["return_features", *SPLITS]],
).orderBy("table_name", "return_id")
snapshot.toPandas().to_csv(latest, index=False)

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
run_info = {
    "run_timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "notebook_url": f"{ctx.apiUrl().get()}/editor/notebooks/{ctx.notebookId().get()}",
    "row_counts": {t: spark.table(f"{OUTPUT_SCHEMA}.{t}").count() for t in ["return_features", *SPLITS]},
    "snapshot_file": "outputs/runs/run-latest.csv",
    "snapshot_sha256": hashlib.sha256(latest.read_bytes()).hexdigest(),
}
run_log["latest"] = run_info
log_path.write_text(json.dumps(run_log, indent=2) + "\n")
print("This run:", json.dumps(run_info, indent=2))

if previous.exists():
    prev_df = pd.read_csv(previous, dtype=str, keep_default_na=False)
    curr_df = pd.read_csv(latest, dtype=str, keep_default_na=False)
    identical = prev_df.equals(curr_df)
    comparison = {
        "previous_run": run_log["previous"],
        "latest_run": run_info,
        "rows_compared": len(curr_df),
        "same_return_ids_per_table": all(
            set(prev_df[prev_df.table_name == t].return_id) == set(curr_df[curr_df.table_name == t].return_id)
            for t in curr_df.table_name.unique()
        ),
        "identical_ids_and_values": identical,
        "same_row_counts": run_log["previous"]["row_counts"] == run_info["row_counts"],
    }
    (run_dir / "run-comparison.json").write_text(json.dumps(comparison, indent=2) + "\n")
    print("Comparison with previous run:", json.dumps({k: v for k, v in comparison.items() if not k.endswith("_run")}, indent=2))
    assert identical and comparison["same_row_counts"], "Rerun changed IDs, values or row counts"
    print("\u2705 Two complete runs left identical return IDs and values, with no appended copies.")
else:
    print("First recorded run; run the whole notebook again to produce the comparison.")

# COMMAND ----------

# DBTITLE 1,Handoff
# MAGIC %md
# MAGIC ### Handoff
# MAGIC * **Input used:** `training.to6_challenge3_inputs.returns_corrected` (from `parsed_source`, per `source_lineage`). `returns_initial` was rejected (evidence in `batch_rejections_initial`).
# MAGIC * **Outputs** (schema `training.to6_c3_aa72a91315d8`): `return_features`, `features_train` (2022), `features_validation` (2023), `features_test` (2024). Columns: `return_id`, `tax_year`, `deduction_to_income`.
# MAGIC * **Ratio:** `deduction_to_income = deduction / income`; NULL when income is 0 (the return is kept).
# MAGIC * **Checks:** completeness and uniqueness on both batches; feature readback (schema, 12 rows, R01 = 20/100 = 0.2, zero-income R03/R09 NULL, no duplicates); split readback (correct year only, each accepted return in exactly one split, values identical to `return_features`); two complete runs compared row by row in `outputs/runs/run-comparison.json`.
# MAGIC * **Not verified:** lineage beyond the supplied view; leakage and prediction-time availability (no labels in scope).