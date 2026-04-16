# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Data Patch
# MAGIC
# MAGIC Lightweight fixes to `workshop.gold` tables. Run AFTER `generate_workshop_data`.
# MAGIC Idempotent — safe to re-run.

# COMMAND ----------

CATALOG = "workshop"
SCHEMA = "gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Patch 1: Add seating_capacity outliers to dim_restaurants
# MAGIC The original generator only produces values 40-200. Add 8 rows with out-of-range values.

# COMMAND ----------

from pyspark.sql.functions import col, when, lit
import random

random.seed(99)

df = spark.table(f"{CATALOG}.{SCHEMA}.dim_restaurants")

# Pick 8 random rows and set seating_capacity to outlier values
outlier_values = [0, 0, -5, 600, 750, 999, 1200, -10]
row_count = df.count()

# Get 8 random restaurant_ids to patch
sample_ids = [r.restaurant_id for r in df.orderBy("restaurant_id").limit(row_count).collect()]
patch_ids = random.sample(sample_ids, 8)

# Build the update using MERGE
spark.sql(f"""
MERGE INTO {CATALOG}.{SCHEMA}.dim_restaurants AS target
USING (
    SELECT restaurant_id, seating_capacity FROM VALUES
    ('{patch_ids[0]}', {outlier_values[0]}),
    ('{patch_ids[1]}', {outlier_values[1]}),
    ('{patch_ids[2]}', {outlier_values[2]}),
    ('{patch_ids[3]}', {outlier_values[3]}),
    ('{patch_ids[4]}', {outlier_values[4]}),
    ('{patch_ids[5]}', {outlier_values[5]}),
    ('{patch_ids[6]}', {outlier_values[6]}),
    ('{patch_ids[7]}', {outlier_values[7]})
    AS source(restaurant_id, seating_capacity)
) AS source
ON target.restaurant_id = source.restaurant_id
WHEN MATCHED THEN UPDATE SET target.seating_capacity = source.seating_capacity
""")

# Verify
outlier_count = spark.sql(f"""
SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.dim_restaurants
WHERE seating_capacity < 1 OR seating_capacity > 500
""").collect()[0]["cnt"]

print(f"✅ Patch 1: {outlier_count} seating_capacity outliers (expected 8)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

print("=" * 50)
print("PATCH VALIDATION")
print("=" * 50)

# Seating outliers
outliers = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.dim_restaurants WHERE seating_capacity < 1 OR seating_capacity > 500").collect()[0]["cnt"]
print(f"  Seating outliers (<1 or >500): {outliers} {'✅' if outliers >= 8 else '❌'}")

# Orphan restaurant_ids in fact_member_activity (should be ~80 from original generator)
orphans = spark.sql(f"""
SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.fact_member_activity m
LEFT ANTI JOIN {CATALOG}.{SCHEMA}.dim_restaurants d ON m.restaurant_id = d.restaurant_id
""").collect()[0]["cnt"]
print(f"  Orphan restaurant_ids in member_activity: {orphans} {'✅' if orphans >= 70 else '❌'}")

# Null markets in dim_restaurants
nulls = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.dim_restaurants WHERE market IS NULL").collect()[0]["cnt"]
print(f"  Null markets in dim_restaurants: {nulls} {'✅' if nulls >= 10 else '❌'}")

print("=" * 50)
print("✅ Patch complete")
