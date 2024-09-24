"""
Module for aggregating and caching CRUD log data using Polars.

This script processes user CRUD action logs, aggregates actions (CREATE, READ,
UPDATE, DELETE) over the past 7 days, and caches the results to avoid reprocessing.
It handles large datasets efficiently using the Polars library and caches data
in Parquet format for intermediate storage.

Usage:
    python spark_job.py <input_dir> <output_dir> <target_date>

Arguments:
    input_dir: Directory containing daily log files in CSV format.
    output_dir: Directory to store the aggregated result CSV files.
    target_date: The target date (YYYY-MM-DD) for which to calculate the 7-day aggregation.
"""

import os
import sys
from datetime import datetime, timedelta

import polars as pl
from pyspark.sql import SparkSession

ACTION_TYPES = ["CREATE", "READ", "UPDATE", "DELETE"]

# Создаём сессию Spark
spark = SparkSession.builder \
    .appName("Weekly CRUD Aggregation with Caching") \
    .getOrCreate()

CACHE_DIR = "./cache"  # Директория для промежуточного хранения


def load_or_cache_day_data(input_dir: str, cache_dir: str, dt: str) -> pl.DataFrame | None:
    """
    Load data for a single day from cache or CSV, then store it in cache.

    Parameters
    ----------
    input_dir : str
        Path to the directory containing the CSV log files.
    cache_dir : str
        Path to the directory for storing intermediate results.
    dt : str
        The date to process in 'YYYY-MM-DD' format.

    Returns
    -------
    Optional[pl.DataFrame]
        A Polars DataFrame containing the loaded data, or None if the file is not found.
    """
    cache_file = os.path.join(cache_dir, f"{dt}.parquet")
    if os.path.exists(cache_file):
        return pl.read_parquet(cache_file)
    file_name = f"{dt}.csv"
    file_path = os.path.join(input_dir, file_name)
    if not os.path.exists(file_path):
        return None
    df = pl.read_csv(file_path, has_header=False, new_columns=["email", "action", "timestamp"])
    df = df.filter(pl.col("action").is_in(ACTION_TYPES))
    df.write_parquet(cache_file)
    return df


def read_logs_with_cache(input_dir: str, cache_dir: str, dt: str) -> pl.DataFrame:
    """
    Load logs for the past 7 days, using cache if available, and aggregate the data.

    Parameters
    ----------
    input_dir : str
        Path to the directory containing the CSV log files.
    cache_dir : str
        Path to the directory for storing intermediate results.
    dt : str
        The target date for aggregation in 'YYYY-MM-DD' format.

    Returns
    -------
    pl.DataFrame
        A Polars DataFrame containing aggregated counts of CRUD operations per user.
    """
    dt = datetime.strptime(dt, '%Y-%m-%d').date()
    dfs = []
    for i in range(7):
        current_date = dt - timedelta(days=i)
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
    """
    Save the aggregated data to a CSV file.

    Parameters
    ----------
    df : pl.DataFrame
        A Polars DataFrame containing the aggregated data.
    output_dir : str
        Path to the directory where the result CSV file will be saved.
    dt : str
        The target date for aggregation in 'YYYY-MM-DD' format.

    Returns
    -------
    None
    """
    output_file = os.path.join(output_dir, f"{dt}.csv")
    df.write_csv(output_file)


if __name__ == "__main__":
    target_date = sys.argv[3]
    agg_df = read_logs_with_cache(sys.argv[1], CACHE_DIR, target_date)
    save_aggregated_data(agg_df, sys.argv[2], target_date)
