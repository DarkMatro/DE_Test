"""
Optimized CRUD Log Aggregator using Polars for large datasets.

This module is designed to process and aggregate user CRUD action logs from large CSV files
with millions of users and billions of events. The script aggregates user actions (CREATE, READ,
UPDATE, DELETE)
over the past 7 days and stores the results in a new CSV file.

The solution uses Polars for high-performance and memory-efficient processing, handling large
datasets with multi-threading and optimized data access.

Usage:
    python script.py <input_dir> <output_dir> <target_date>

Arguments:
    input_dir: Directory containing daily log files in CSV format.
    output_dir: Directory to store the aggregated result CSV files.
    target_date: The target date (YYYY-MM-DD) for which to calculate the 7-day aggregation.
"""

import datetime
import os
import sys
from typing import Any

import polars as pl

# Action types for aggregation
ACTION_TYPES = ["CREATE", "READ", "UPDATE", "DELETE"]
INPUT_DIR = './input'
OUTPUT_DIR = './output'
CACHE_DIR = './cache'


def load_or_cache_day_data(input_dir: str, cache_dir: str, dt: str) -> Any | None:
    """
    Загружает данные за один день из кеша, если они уже обработаны,
    либо читает данные из CSV и сохраняет в кеш.

    Parameters
    ----------
    input_dir : str
        Путь до директории с исходными CSV логами.
    cache_dir : str
        Путь до директории для хранения промежуточных результатов.
    dt : str
        Дата для обработки (YYYY-MM-DD).

    Returns
    -------
    polars.DataFrame
        Полярный DataFrame с данными за указанный день.
    """
    cache_file = os.path.join(cache_dir, f"{dt}.parquet")

    if os.path.exists(cache_file):
        # Если кешированный файл существует, загружаем его
        print(f"Загрузка данных из кеша для {dt}")
        return pl.read_parquet(cache_file)

    # Если кеша нет, загружаем CSV и сохраняем данные в кеш
    file_name = f"{dt}.csv"
    file_path = os.path.join(input_dir, file_name)

    if not os.path.exists(file_path):
        print(f"Файл не найден: {file_path}")
        return None

    # Читаем CSV файл
    print(f"Загрузка данных из CSV для {dt}")
    df = pl.read_csv(file_path, has_header=False, new_columns=["email", "action", "timestamp"])

    # Фильтруем только нужные действия
    df = df.filter(pl.col("action").is_in(ACTION_TYPES))

    # Сохраняем данные в кеш
    df.write_parquet(cache_file)
    print(f"Сохранение данных в кеш для {dt}")

    return df


def read_logs_with_cache(input_dir: str, cache_dir: str, dt: str) -> pl.DataFrame:
    """
    Reads user logs from the past 7 days and aggregates them into a single DataFrame.

    Parameters
    ----------
    input_dir : str
        Path to the directory containing the CSV log files.
    cache_dir : str
        Путь до директории для хранения промежуточных результатов.
    dt : str
        The target date for the aggregation (YYYY-MM-DD). Logs from this date and the
        preceding 6 days will be processed.

    Returns
    -------
    polars.DataFrame
         A Polars DataFrame containing aggregated CRUD counts for each user.
    """
    dt = datetime.datetime.strptime(dt, '%Y-%m-%d').date()

    # Список для хранения DataFrame за каждый день
    dfs = []

    # Обрабатываем последние 7 дней
    for i in range(7):
        current_date = dt - datetime.timedelta(days=i)
        current_date_str = current_date.strftime('%Y-%m-%d')

        day_data = load_or_cache_day_data(input_dir, cache_dir, current_date_str)
        if day_data is not None:
            dfs.append(day_data)

    # Объединяем все DataFrame в один
    combined_df = pl.concat(dfs)

    # Группируем по email и action, затем считаем количество каждой операции
    aggregated_df = combined_df.group_by(["email", "action"]).count().pivot(
        values="count", index="email", columns="action"
    ).fill_nan(0)  # Заполняем отсутствующие значения 0

    # Убеждаемся, что все CRUD действия присутствуют в колонках
    for action in ACTION_TYPES:
        if action not in aggregated_df.columns:
            aggregated_df = aggregated_df.with_column(pl.lit(0).alias(action))

    # Переименовываем колонки в соответствии с требуемым форматом
    aggregated_df = aggregated_df.rename({
        "CREATE": "create_count",
        "READ": "read_count",
        "UPDATE": "update_count",
        "DELETE": "delete_count"
    })

    return aggregated_df


def save_aggregated_data(df: pl.DataFrame, output_dir: str, dt: str) -> None:
    """
    Saves the aggregated data to a CSV file in the output directory.

    Parameters
    ----------
    df : polars.DataFrame
        Aggregated DataFrame with user actions.
    output_dir : str
        Path to the directory where the output CSV file will be saved.
    dt : str
        The target date (YYYY-MM-DD) for which the aggregation was performed.

    Returns
    -------
    None
    """
    output_file = os.path.join(output_dir, f"{dt}.csv")
    df.write_csv(output_file)
    print(f"Агрегированные данные сохранены в {output_file}")


def aggregate_logs(input_dir: str, output_dir: str, dt: str) -> None:
    """
    Main function that reads logs, aggregates them, and saves the results.

    Parameters
    ----------
    input_dir : str
        Directory containing the input CSV files with logs.
    output_dir : str
        Directory where the aggregated result CSV file will be saved.
    dt : str
        The target date (YYYY-MM-DD) for which the aggregation will be performed.

    Returns
    -------
    None
    """
    # Шаг 1: Читаем логи за 7 дней с использованием кеша
    aggregated_df = read_logs_with_cache(input_dir, CACHE_DIR, dt)

    # Шаг 2: Сохраняем агрегированные данные в CSV
    save_aggregated_data(aggregated_df, output_dir, dt)


if __name__ == "__main__":
    # Command-line arguments
    in_dir = sys.argv[1]
    out_dir = sys.argv[2]
    target_date = sys.argv[3]  # Target date for aggregation (e.g., 2024-09-16)

    # Run the aggregation process
    aggregate_logs(in_dir, out_dir, target_date)
