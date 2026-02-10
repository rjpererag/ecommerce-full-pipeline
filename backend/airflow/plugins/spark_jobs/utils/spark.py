from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql import types as T
import re
import hashlib


def get_valid_cols(cols: list[str], df_cols: list[str]) -> list[str]:
    valid_cols = [col for col in cols if col in df_cols]
    return valid_cols


def get_parsed_cols(mapping: dict) -> list[Column]:

    parsed_cols = []
    for col_path, (alias, col_type) in mapping.items():
        col_expr = F.col(col_path)

        if col_type:
            col_expr = col_expr.cast(col_type)

        parsed_cols.append(col_expr.alias(alias))

    return parsed_cols


def create_table_name(
        run_id: str,
        table_name: str | None = None,
        prefix: str = "stg"
) -> str:
    """
    Transforms an Airflow run_id into a valid PostgreSQL table name.
    Example: 'scheduled__2024-01-01T00:00:00+00:00' -> 'scheduled_2024_01_01t00_00_00_00_00'
    """
    name = run_id.lower()
    name = re.sub(r'[^a-z0-9]', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')

    full_name = f"{prefix}_{name}"
    if table_name:
        full_name = f"{prefix}_{table_name}_{name}"

    if len(full_name) > 63:
        hash_suffix = hashlib.md5(run_id.encode()).hexdigest()[:8]
        full_name = f"{full_name[:54]}_{hash_suffix}"

    return full_name

def explode_df(
        df: DataFrame,
        col_to_explode: str,
        alias: str,
        cols_to_keep: list[str],
) -> DataFrame:
    cols_to_keep = [F.col(name) for name in cols_to_keep]
    exploded_df = df.select(
        *cols_to_keep,
        F.explode(col_to_explode).alias(alias),
    )
    return exploded_df

def change_to_timestamp(
        df: DataFrame,
        col_to_change: str,
        alias: str,
) -> DataFrame:

    cols_to_keep = [F.col(col) for col in df.columns]
    new_df = df.select(
        *cols_to_keep,
        F.to_timestamp(F.col(col_to_change)).alias(alias)
    )
    return new_df


def remove_nulls_from_cols(df: DataFrame, cols: list[str]) -> DataFrame:
    valid_cols = get_valid_cols(cols, df.columns)

    if not valid_cols:
        return df

    for col in valid_cols:
        df = df.filter(F.col(col).isNotNull())

    return df


def drop_duplicates_from_cols(df: DataFrame, cols: list[str]) -> DataFrame:
    cols_valid = get_valid_cols(cols, df.columns)
    return df.dropDuplicates(cols_valid)


def expand_json_to_col(
        df: DataFrame,
        target_col: str = "payload",
        new_col: str = "data",
        schema: T.StructType = None,
) -> DataFrame:
    return df.withColumn(
        new_col,
        F.from_json(F.col(target_col), schema)
    )