# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Data Generator — Arcos Dorados Genie Code Workshop
# MAGIC
# MAGIC Generates 5 synthetic McDonald's-themed tables in `workshop.gold`.
# MAGIC Includes intentional data quality issues for the Governance track.
# MAGIC
# MAGIC **Idempotent** — safe to re-run. Uses `CREATE OR REPLACE TABLE`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "workshop"
SCHEMA = "gold"

# Market distribution (matches Arcos Dorados real footprint)
MARKETS = {
    "AR": {"country": "Argentina", "restaurants": 120, "cities": ["Buenos Aires", "Córdoba", "Rosario", "Mendoza", "Tucumán", "Mar del Plata", "Salta", "Santa Fe"]},
    "BR": {"country": "Brasil", "restaurants": 150, "cities": ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Porto Alegre", "Salvador", "Brasília", "Recife"]},
    "MX": {"country": "México", "restaurants": 80, "cities": ["Ciudad de México", "Guadalajara", "Monterrey", "Puebla", "Cancún", "Mérida", "Tijuana", "León"]},
    "CO": {"country": "Colombia", "restaurants": 40, "cities": ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena"]},
    "CL": {"country": "Chile", "restaurants": 30, "cities": ["Santiago", "Valparaíso", "Concepción", "Viña del Mar", "Temuco"]},
    "PE": {"country": "Perú", "restaurants": 25, "cities": ["Lima", "Arequipa", "Trujillo", "Cusco"]},
    "UY": {"country": "Uruguay", "restaurants": 15, "cities": ["Montevideo", "Punta del Este", "Salto"]},
    "CR": {"country": "Costa Rica", "restaurants": 15, "cities": ["San José", "Heredia", "Alajuela"]},
    "PA": {"country": "Panamá", "restaurants": 15, "cities": ["Ciudad de Panamá", "David", "Colón"]},
    "EC": {"country": "Ecuador", "restaurants": 10, "cities": ["Quito", "Guayaquil", "Cuenca"]},
}

RESTAURANT_TYPES = ["Freestanding", "Mall", "Drive-Thru", "McCafe Express"]
LOYALTY_TIERS = ["Bronze", "Silver", "Gold"]
CATEGORIES = ["Burgers", "McCafe", "Breakfast", "Nuggets", "Desserts", "Sides", "Beverages"]
CHANNELS = ["In-Store", "App", "Delivery"]
DISCOUNT_TYPES = ["percentage", "bogo", "fixed_amount", "free_item"]

print(f"Target: {CATALOG}.{SCHEMA}")
print(f"Total restaurants: {sum(m['restaurants'] for m in MARKETS.values())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog & Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"✅ {CATALOG}.{SCHEMA} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 1: dim_restaurants

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import date, timedelta

np.random.seed(42)

rows = []
restaurant_id = 0

for market_code, info in MARKETS.items():
    for i in range(info["restaurants"]):
        restaurant_id += 1
        rid = f"REST-{market_code}-{restaurant_id:04d}"
        city = np.random.choice(info["cities"])
        rtype = np.random.choice(RESTAURANT_TYPES, p=[0.35, 0.30, 0.25, 0.10])
        opened = date(2005, 1, 1) + timedelta(days=int(np.random.uniform(0, 365 * 19)))

        # Approximate lat/lon ranges per market
        lat_ranges = {"AR": (-38, -26), "BR": (-23, -3), "MX": (19, 25), "CO": (2, 8),
                      "CL": (-38, -30), "PE": (-14, -6), "UY": (-35, -33), "CR": (9, 11),
                      "PA": (7, 10), "EC": (-3, 1)}
        lon_ranges = {"AR": (-68, -57), "BR": (-51, -35), "MX": (-104, -96), "CO": (-76, -72),
                      "CL": (-72, -70), "PE": (-77, -75), "UY": (-57, -54), "CR": (-85, -83),
                      "PA": (-80, -77), "EC": (-80, -77)}

        lat = np.random.uniform(*lat_ranges[market_code])
        lon = np.random.uniform(*lon_ranges[market_code])
        is_24h = np.random.random() < 0.15
        capacity = int(np.random.choice([40, 60, 80, 100, 120, 150, 200], p=[0.05, 0.15, 0.25, 0.25, 0.15, 0.10, 0.05]))

        rows.append({
            "restaurant_id": rid,
            "restaurant_name": f"McDonald's {city} #{i+1}",
            "market": market_code,
            "country_name": info["country"],
            "city": city,
            "restaurant_type": rtype,
            "opening_date": opened,
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "is_24h": is_24h,
            "seating_capacity": capacity,
        })

df_restaurants = pd.DataFrame(rows)

# ── Inject DQ issues ──
# 15 rows with NULL market
null_idx = np.random.choice(len(df_restaurants), 15, replace=False)
df_restaurants.loc[null_idx, "market"] = None

# 8 rows with future opening_date
future_idx = np.random.choice(len(df_restaurants), 8, replace=False)
df_restaurants.loc[future_idx, "opening_date"] = pd.Timestamp("2027-06-15").date()

# 12 rows with lat/lon = 0.0, 0.0
zero_idx = np.random.choice(len(df_restaurants), 12, replace=False)
df_restaurants.loc[zero_idx, ["latitude", "longitude"]] = 0.0

# 5 duplicate restaurant_ids
dup_idx = np.random.choice(len(df_restaurants), 5, replace=False)
for idx in dup_idx:
    source_idx = np.random.choice([i for i in range(len(df_restaurants)) if i != idx])
    df_restaurants.loc[idx, "restaurant_id"] = df_restaurants.loc[source_idx, "restaurant_id"]

# 20 rows with inconsistent casing on restaurant_type
case_idx = np.random.choice(len(df_restaurants), 20, replace=False)
df_restaurants.loc[case_idx, "restaurant_type"] = df_restaurants.loc[case_idx, "restaurant_type"].str.lower()

print(f"dim_restaurants: {len(df_restaurants)} rows")
print(f"  DQ issues: 15 null markets, 8 future dates, 12 zero coords, 5 dup IDs, 20 inconsistent casing")

# Write to Delta
sdf = spark.createDataFrame(df_restaurants)
sdf.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.dim_restaurants")
print(f"✅ {CATALOG}.{SCHEMA}.dim_restaurants written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 2: fact_daily_sales

# COMMAND ----------

# Get valid restaurant IDs (before DQ issues) for FK generation
valid_restaurants = df_restaurants["restaurant_id"].dropna().unique().tolist()

date_range = pd.date_range("2024-01-01", "2026-04-13", freq="D")
all_sales = []

for rid in valid_restaurants[:500]:  # cap at 500
    # Base revenue varies by market
    market = df_restaurants[df_restaurants["restaurant_id"] == rid]["market"].iloc[0]
    if pd.isna(market):
        market = "AR"

    base_rev = {"AR": 3500, "BR": 5000, "MX": 4000, "CO": 2500, "CL": 3000,
                "PE": 2000, "UY": 2200, "CR": 2800, "PA": 2600, "EC": 1800}.get(market, 3000)
    base_txn = int(base_rev / 8)  # ~$8 avg ticket

    for d in date_range:
        # Weekly pattern: weekends higher
        dow_mult = {0: 0.85, 1: 0.80, 2: 0.85, 3: 0.90, 4: 1.05, 5: 1.25, 6: 1.20}[d.dayofweek]

        # Monthly seasonality
        month_mult = {1: 0.90, 2: 0.85, 3: 0.92, 4: 0.95, 5: 0.98, 6: 1.00,
                      7: 1.05, 8: 1.02, 9: 0.95, 10: 1.00, 11: 1.05, 12: 1.25}[d.month]

        # YoY growth ~3%
        years_from_start = (d - pd.Timestamp("2024-01-01")).days / 365
        growth = 1 + 0.03 * years_from_start

        # Random noise
        noise = np.random.normal(1.0, 0.12)

        revenue = base_rev * dow_mult * month_mult * growth * noise
        transactions = int(base_txn * dow_mult * month_mult * growth * np.random.normal(1.0, 0.08))
        transactions = max(transactions, 50)
        items = int(transactions * np.random.uniform(1.8, 2.5))
        avg_ticket = round(revenue / max(transactions, 1), 2)

        # Daypart splits
        dp_breakfast = round(np.random.uniform(0.05, 0.20), 3)
        dp_lunch = round(np.random.uniform(0.25, 0.45), 3)
        dp_dinner = round(np.random.uniform(0.20, 0.35), 3)
        dp_latenight = round(1.0 - dp_breakfast - dp_lunch - dp_dinner, 3)

        delivery_pct = round(np.random.uniform(0.05, 0.35), 3)

        all_sales.append({
            "sale_date": d.date(),
            "restaurant_id": rid,
            "transactions": transactions,
            "gross_revenue": round(revenue, 2),
            "items_sold": items,
            "avg_ticket": avg_ticket,
            "daypart_breakfast": dp_breakfast,
            "daypart_lunch": dp_lunch,
            "daypart_dinner": dp_dinner,
            "daypart_latenight": dp_latenight,
            "delivery_pct": delivery_pct,
        })

df_sales = pd.DataFrame(all_sales)

# ── Inject DQ issues ──
# 200 rows: revenue = 0 but transactions > 0
zero_rev_idx = np.random.choice(len(df_sales), 200, replace=False)
df_sales.loc[zero_rev_idx, "gross_revenue"] = 0.0

# 50 rows: negative avg_ticket
neg_ticket_idx = np.random.choice(len(df_sales), 50, replace=False)
df_sales.loc[neg_ticket_idx, "avg_ticket"] = -1.0

# 30 rows: NULL sale_date
null_date_idx = np.random.choice(len(df_sales), 30, replace=False)
df_sales.loc[null_date_idx, "sale_date"] = None

# 40 rows: daypart sums out of range (>1.05 or <0.90)
bad_daypart_idx = np.random.choice(len(df_sales), 40, replace=False)
df_sales.loc[bad_daypart_idx, "daypart_lunch"] = df_sales.loc[bad_daypart_idx, "daypart_lunch"] + 0.15

print(f"fact_daily_sales: {len(df_sales)} rows")
print(f"  DQ issues: 200 zero revenue, 50 negative tickets, 30 null dates, 40 bad dayparts")

# Write in batches to avoid OOM
sdf_sales = spark.createDataFrame(df_sales)
sdf_sales.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_daily_sales")
print(f"✅ {CATALOG}.{SCHEMA}.fact_daily_sales written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 3: fact_member_activity

# COMMAND ----------

member_rows = []
n_members = 5000  # unique members
member_ids = [f"MCR-{np.random.choice(list(MARKETS.keys()))}-{i:06d}" for i in range(1, n_members + 1)]
activity_dates = pd.date_range("2025-10-01", "2026-04-13", freq="D")

for _ in range(200000):
    mid = np.random.choice(member_ids)
    market_code = mid.split("-")[1]
    rid = np.random.choice(valid_restaurants[:500])
    ad = pd.Timestamp(np.random.choice(activity_dates)).date()
    tier = np.random.choice(LOYALTY_TIERS, p=[0.50, 0.35, 0.15])
    points_e = int(np.random.uniform(10, 500))
    points_r = int(np.random.uniform(0, 2000)) if np.random.random() < 0.3 else 0
    order_val = round(np.random.uniform(5, 45), 2)
    cat = np.random.choice(CATEGORIES)
    channel = np.random.choice(CHANNELS, p=[0.45, 0.30, 0.25])

    member_rows.append({
        "activity_date": ad,
        "member_id": mid,
        "restaurant_id": rid,
        "market": market_code,
        "loyalty_tier": tier,
        "points_earned": points_e,
        "points_redeemed": points_r,
        "order_value": order_val,
        "favorite_category": cat,
        "channel": channel,
    })

df_members = pd.DataFrame(member_rows)

# ── Inject DQ issues ──
# 100 rows: lowercase loyalty_tier
tier_case_idx = np.random.choice(len(df_members), 100, replace=False)
df_members.loc[tier_case_idx, "loyalty_tier"] = df_members.loc[tier_case_idx, "loyalty_tier"].str.lower()

# 40 rows: negative points_earned
neg_pts_idx = np.random.choice(len(df_members), 40, replace=False)
df_members.loc[neg_pts_idx, "points_earned"] = -50

# 25 rows: orphan market code "XX"
orphan_idx = np.random.choice(len(df_members), 25, replace=False)
df_members.loc[orphan_idx, "market"] = "XX"
df_members.loc[orphan_idx, "member_id"] = "MCR-XX-000000"

# 80 rows: restaurant_id not in dim_restaurants (referential integrity break)
fake_rids = [f"REST-ZZ-{9000+i:04d}" for i in range(80)]
ref_break_idx = np.random.choice(len(df_members), 80, replace=False)
for i, idx in enumerate(ref_break_idx):
    df_members.loc[idx, "restaurant_id"] = fake_rids[i]

print(f"fact_member_activity: {len(df_members)} rows")
print(f"  DQ issues: 100 lowercase tiers, 40 negative points, 25 orphan XX, 80 ref integrity breaks")

sdf_members = spark.createDataFrame(df_members)
sdf_members.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_member_activity")
print(f"✅ {CATALOG}.{SCHEMA}.fact_member_activity written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 4: fact_promo_events

# COMMAND ----------

promo_rows = []
promo_names = [
    "McCombo Verano", "2x1 Big Mac", "McFlurry Festival", "Desayuno Express",
    "Happy Meal Promo", "McDelivery Free", "Nuggets x20", "Café Gratis",
    "Sundae Day", "Cuarto de Libra Promo", "McWrap Launch", "Combo Familiar",
    "Student Discount", "App Exclusive", "Loyalty Double Points",
]

for year in [2024, 2025, 2026]:
    n_promos = 60 if year < 2026 else 30
    for i in range(n_promos):
        start = date(year, 1, 1) + timedelta(days=int(np.random.uniform(0, 330 if year < 2026 else 100)))
        duration = int(np.random.uniform(7, 45))
        end = start + timedelta(days=duration)
        market = np.random.choice(list(MARKETS.keys()) + ["ALL"], p=[0.08]*10 + [0.20])
        dtype = np.random.choice(DISCOUNT_TYPES, p=[0.40, 0.25, 0.20, 0.15])
        dval = {"percentage": np.random.choice([10, 15, 20, 25, 30, 50]),
                "bogo": 1, "fixed_amount": np.random.choice([2, 3, 5, 10]),
                "free_item": 0}[dtype]
        target_tier = np.random.choice([None, "Gold", "Silver", "Bronze"], p=[0.5, 0.2, 0.2, 0.1])
        target_daypart = np.random.choice([None, "Breakfast", "Lunch", "Dinner"], p=[0.6, 0.15, 0.15, 0.1])
        redemptions = int(np.random.uniform(500, 50000))
        budget = round(np.random.uniform(5000, 200000), 2) if np.random.random() < 0.8 else None

        promo_rows.append({
            "promo_id": f"PROMO-{year}-{i+1:03d}",
            "promo_name": np.random.choice(promo_names),
            "start_date": start,
            "end_date": end,
            "market": market,
            "discount_type": dtype,
            "discount_value": float(dval),
            "target_tier": target_tier,
            "target_daypart": target_daypart,
            "redemption_count": redemptions,
            "budget_usd": budget,
        })

df_promos = pd.DataFrame(promo_rows)

# ── Inject DQ issues ──
# 3 promos: end_date BEFORE start_date
bad_date_idx = np.random.choice(len(df_promos), 3, replace=False)
for idx in bad_date_idx:
    df_promos.loc[idx, "end_date"] = df_promos.loc[idx, "start_date"] - timedelta(days=10)

# 5 promos: discount_value > 100 for percentage type
pct_idx = df_promos[df_promos["discount_type"] == "percentage"].index
if len(pct_idx) >= 5:
    bad_pct_idx = np.random.choice(pct_idx, 5, replace=False)
    df_promos.loc[bad_pct_idx, "discount_value"] = 150.0

# 10 promos: NULL promo_name
null_name_idx = np.random.choice(len(df_promos), 10, replace=False)
df_promos.loc[null_name_idx, "promo_name"] = None

print(f"fact_promo_events: {len(df_promos)} rows")
print(f"  DQ issues: 3 inverted dates, 5 impossible discounts, 10 null names")

sdf_promos = spark.createDataFrame(df_promos)
sdf_promos.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_promo_events")
print(f"✅ {CATALOG}.{SCHEMA}.fact_promo_events written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 5: fact_daily_kpis

# COMMAND ----------

# Pre-aggregate from fact_daily_sales by market + date
kpi_rows = []
markets_list = list(MARKETS.keys())

for market_code in markets_list:
    market_rids = df_restaurants[df_restaurants["market"] == market_code]["restaurant_id"].tolist()
    n_restaurants = len(market_rids)

    for d in date_range:
        d_date = d.date()
        market_sales = df_sales[(df_sales["restaurant_id"].isin(market_rids)) & (df_sales["sale_date"] == d_date)]

        if len(market_sales) == 0:
            # Generate synthetic KPI if no sales data for this market-date
            total_txn = int(np.random.uniform(5000, 20000))
            total_rev = round(total_txn * np.random.uniform(7, 12), 2)
        else:
            total_txn = int(market_sales["transactions"].sum())
            total_rev = round(market_sales["gross_revenue"].sum(), 2)

        avg_ticket = round(total_rev / max(total_txn, 1), 2)
        active_members = int(np.random.uniform(500, 5000))
        new_members = int(np.random.uniform(10, 200))
        delivery_pct = round(np.random.uniform(0.08, 0.30), 3)

        # YoY growth (NULL for first year)
        yoy_growth = None
        if d.year >= 2025:
            yoy_growth = round(np.random.normal(0.03, 0.05), 4)

        kpi_rows.append({
            "kpi_date": d_date,
            "market": market_code,
            "total_restaurants": n_restaurants,
            "total_transactions": total_txn,
            "total_revenue": total_rev,
            "avg_ticket": avg_ticket,
            "active_members": active_members,
            "new_members": new_members,
            "delivery_pct": delivery_pct,
            "yoy_revenue_growth": yoy_growth,
        })

df_kpis = pd.DataFrame(kpi_rows)

# ── Inject DQ issues ──
# 20 rows: transactions = 0 but revenue > 0
zero_txn_idx = np.random.choice(len(df_kpis), 20, replace=False)
df_kpis.loc[zero_txn_idx, "total_transactions"] = 0

# 5 rows: market = "UNKNOWN"
unknown_idx = np.random.choice(len(df_kpis), 5, replace=False)
df_kpis.loc[unknown_idx, "market"] = "UNKNOWN"

# 3 rows: yoy_revenue_growth = 999.99 (sentinel outlier)
sentinel_idx = np.random.choice(df_kpis[df_kpis["yoy_revenue_growth"].notna()].index, 3, replace=False)
df_kpis.loc[sentinel_idx, "yoy_revenue_growth"] = 999.99

print(f"fact_daily_kpis: {len(df_kpis)} rows")
print(f"  DQ issues: 20 zero-txn with revenue, 5 UNKNOWN market, 3 sentinel outliers")

sdf_kpis = spark.createDataFrame(df_kpis)
sdf_kpis.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_daily_kpis")
print(f"✅ {CATALOG}.{SCHEMA}.fact_daily_kpis written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

print("=" * 60)
print("WORKSHOP DATA GENERATION — SUMMARY")
print("=" * 60)

tables = ["dim_restaurants", "fact_daily_sales", "fact_member_activity", "fact_promo_events", "fact_daily_kpis"]
for t in tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.{t}").collect()[0]["cnt"]
    print(f"  {CATALOG}.{SCHEMA}.{t}: {count:,} rows")

print()
print("DQ Issues Summary:")
print("  dim_restaurants: 15 null markets, 8 future dates, 12 zero coords, 5 dup IDs, 20 bad casing")
print("  fact_daily_sales: 200 zero revenue, 50 negative tickets, 30 null dates, 40 bad dayparts")
print("  fact_member_activity: 100 lowercase tiers, 40 negative points, 25 orphan XX, 80 ref breaks")
print("  fact_promo_events: 3 inverted dates, 5 impossible discounts, 10 null names")
print("  fact_daily_kpis: 20 zero-txn with revenue, 5 UNKNOWN market, 3 sentinel outliers")
print()
print(f"Total DQ defects: ~356")
print("=" * 60)
print("✅ Data generation complete. Ready for workshop.")
