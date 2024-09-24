import os
import sys

from pyspark.sql import SparkSession
import polars as pl
from datetime import datetime

ACTION_TYPES = ["CREATE", "READ", "UPDATE", "DELETE"]

# Создаём сессию Spark
spark = SparkSession.builder \
    .appName("Weekly CRUD Aggregation with Caching") \
    .getOrCreate()

INPUT_DIR = "./input"
OUTPUT_DIR = "./output"
CACHE_DIR = "./cache"  # Директория для промежуточного хранения


def load_or_cache_day_data(input_dir: str, cache_dir: str, dt: str) -> pl.DataFrame:
    cache_file = os.path.join(cache_dir, f"{dt}.parquet")
    if os.path.exists(cache_file):
        return pl.read_parquet(cache_file)
    else:
        file_name = f"{dt}.csv"
        file_path = os.path.join(input_dir, file_name)
        if not os.path.exists(file_path):
            return None
        df = pl.read_csv(file_path, has_header=False, new_columns=["email", "action", "timestamp"])
        df = df.filter(pl.col("action").is_in(ACTION_TYPES))
        df.write_parquet(cache_file)
        return df


def read_logs_with_cache(input_dir: str, cache_dir: str, dt: str) -> pl.DataFrame:
    dt = datetime.datetime.strptime(dt, '%Y-%m-%d').date()
    dfs = []
    for i in range(7):
        current_date = dt - datetime.timedelta(days=i)
        current_date_str = current_date.strftime('%Y-%m-%d')
        day_data = load_or_cache_day_data(input_dir, cache_dir, current_date_str)
        if day_data is not None:
            dfs.append(day_data)
    combined_df = pl.concat(dfs)
    aggregated_df = combined_df.group_by(["email", "action"]).count().pivot(
        values="count", index="email", columns="action").fill_nan(0)
    for action in ACTION_TYPES:
        if action not in aggregated_df.columns:
            aggregated_df = aggregated_df.with_column(pl.lit(0).alias(action))
    aggregated_df = aggregated_df.rename({
        "CREATE": "create_count",
        "READ": "read_count",
        "UPDATE": "update_count",
        "DELETE": "delete_count"
    })
    return aggregated_df


def save_aggregated_data(df: pl.DataFrame, output_dir: str, dt: str) -> None:
    output_file = os.path.join(output_dir, f"{dt}.csv")
    df.write_csv(output_file)


if __name__ == "__main__":
    target_date = sys.argv[3]
    aggregated_df = read_logs_with_cache(INPUT_DIR, CACHE_DIR, target_date)
    save_aggregated_data(aggregated_df, OUTPUT_DIR, target_date)
