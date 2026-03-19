import json
import os
import shutil
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output"))
HIVE_WAREHOUSE_DIR = os.getenv("HIVE_WAREHOUSE_DIR", str(OUTPUT_DIR / "spark-warehouse"))
HIVE_TABLE_NAME = os.getenv("HIVE_TABLE_NAME", "target")
CLEANED_CSV_NAME = "cleaned__target_data.csv"


def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    cleaned_df = df.copy()
    timestamp_columns = [
        "order_purchase_timestamp",
        "order_aproved_at",
        "order_delivered_customer_date",
    ]
    for column in timestamp_columns:
        cleaned_df[column] = pd.to_datetime(cleaned_df[column], errors="coerce")

    numeric_columns = [
        "Id",
        "order_products_value",
        "order_freight_value",
        "order_items_qty",
        "customer_zip_code_prefix",
        "review_score",
    ]
    for column in numeric_columns:
        cleaned_df[column] = pd.to_numeric(cleaned_df[column], errors="coerce")

    cleaned_df = cleaned_df.dropna()
    cleaned_df["total_order_value"] = (
        cleaned_df["order_products_value"] + cleaned_df["order_freight_value"]
    )
    cleaned_df["approval_delay_hours"] = (
        cleaned_df["order_aproved_at"] - cleaned_df["order_purchase_timestamp"]
    ).dt.total_seconds() / 3600
    cleaned_df["delivery_time_days"] = (
        cleaned_df["order_delivered_customer_date"] - cleaned_df["order_purchase_timestamp"]
    ).dt.total_seconds() / 86400
    cleaned_df["freight_ratio"] = (
        cleaned_df["order_freight_value"] / cleaned_df["total_order_value"]
    ).round(4)
    cleaned_df["customer_city"] = cleaned_df["customer_city"].str.title()
    cleaned_df["customer_state"] = cleaned_df["customer_state"].str.upper()
    return cleaned_df


def generate_insights(df: pd.DataFrame) -> dict:
    insights = {
        "row_count": int(len(df)),
        "order_status_distribution": df["order_status"].value_counts().to_dict(),
        "average_total_order_value": round(float(df["total_order_value"].mean()), 2),
        "average_freight_value": round(float(df["order_freight_value"].mean()), 2),
        "average_delivery_time_days": round(float(df["delivery_time_days"].mean()), 2),
        "average_approval_delay_hours": round(float(df["approval_delay_hours"].mean()), 2),
        "average_review_score": round(float(df["review_score"].mean()), 2),
        "sales_by_city": (
            df.groupby("customer_city")["total_order_value"]
            .sum()
            .round(2)
            .sort_values(ascending=False)
            .to_dict()
        ),
        "sales_by_state": (
            df.groupby("customer_state")["total_order_value"]
            .sum()
            .round(2)
            .sort_values(ascending=False)
            .to_dict()
        ),
        "review_to_delivery_correlation": round(
            float(df["review_score"].corr(df["delivery_time_days"])), 4
        ),
    }
    return insights


def split_new_cleaned_data(
    df: pd.DataFrame, cleaned_csv_path: Path
) -> tuple[pd.DataFrame, int, int, int]:
    incoming_unique_df = df.drop_duplicates(subset=["Id"]).copy()
    incoming_unique_df["Id"] = pd.to_numeric(incoming_unique_df["Id"], errors="coerce")
    incoming_unique_df = incoming_unique_df.dropna(subset=["Id"])
    incoming_unique_df["Id"] = incoming_unique_df["Id"].astype(int)

    duplicate_rows_within_batch = len(df) - len(incoming_unique_df)
    if not cleaned_csv_path.exists():
        return (
            incoming_unique_df.sort_values("Id").reset_index(drop=True),
            0,
            duplicate_rows_within_batch,
            len(incoming_unique_df),
        )

    existing_ids_df = pd.read_csv(cleaned_csv_path, usecols=["Id"])
    existing_ids = set(pd.to_numeric(existing_ids_df["Id"], errors="coerce").dropna().astype(int))
    new_rows_df = incoming_unique_df[~incoming_unique_df["Id"].isin(existing_ids)].copy()
    return (
        new_rows_df.sort_values("Id").reset_index(drop=True),
        len(existing_ids),
        duplicate_rows_within_batch,
        len(incoming_unique_df),
    )


def write_outputs(df: pd.DataFrame) -> dict:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    cleaned_csv_path = OUTPUT_DIR / CLEANED_CSV_NAME
    insights_path = OUTPUT_DIR / "insights.json"
    new_rows_df, existing_row_count, duplicate_rows_within_batch, incoming_unique_count = (
        split_new_cleaned_data(df, cleaned_csv_path)
    )

    if not new_rows_df.empty:
        new_rows_df.to_csv(
            cleaned_csv_path,
            mode="a",
            header=not cleaned_csv_path.exists(),
            index=False,
        )

    final_df = pd.read_csv(cleaned_csv_path)
    insights = generate_insights(final_df)

    insights_path.write_text(json.dumps(insights, indent=2), encoding="utf-8")
    print(f"[pipeline] incoming_cleaned_rows={len(df)}")
    print(f"[pipeline] incoming_unique_rows={incoming_unique_count}")
    print(f"[pipeline] duplicate_rows_within_batch={duplicate_rows_within_batch}")
    print(f"[pipeline] existing_cleaned_rows={existing_row_count}")
    print(f"[pipeline] newly_appended_rows={len(new_rows_df)}")
    print(f"[pipeline] total_cleaned_rows={len(final_df)}")

    spark = (
        SparkSession.builder.appName("target-data-pipeline")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", HIVE_WAREHOUSE_DIR)
        .enableHiveSupport()
        .getOrCreate()
    )

    table_path = Path(HIVE_WAREHOUSE_DIR) / HIVE_TABLE_NAME
    spark.sql(f"DROP TABLE IF EXISTS {HIVE_TABLE_NAME}")
    shutil.rmtree(table_path, ignore_errors=True)

    spark_df = spark.createDataFrame(final_df)
    spark_df.write.mode("overwrite").saveAsTable(HIVE_TABLE_NAME)

    summary_df = (
        spark.table(HIVE_TABLE_NAME)
        .groupBy("customer_city", "customer_state")
        .agg(
            F.round(F.sum("total_order_value"), 2).alias("total_sales"),
            F.round(F.avg("delivery_time_days"), 2).alias("avg_delivery_days"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        )
        .orderBy(F.desc("total_sales"))
    )

    summary_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(OUTPUT_DIR / "sales_by_city_report")
    )
    spark.stop()
    return insights


def run_pipeline(raw_records: list[dict]) -> dict:
    raw_df = pd.DataFrame(raw_records)
    cleaned_df = clean_sales_data(raw_df)
    return write_outputs(cleaned_df)
