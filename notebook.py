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



# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# Your implementation, rerun comparison and exports here.