
import os
import polars as pl
from pathlib import Path

def get_env_var(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

ANALYTICS_DIR     = Path(get_env_var("ANALYTICS_PATH"))
DIM_PLATFORM_PATH = ANALYTICS_DIR / "dim_platform.parquet"

def test_schema():
    df = pl.read_parquet(DIM_PLATFORM_PATH)

    expected_cols = {
        "platform_id":   pl.UInt32,
        "platform_name": pl.Utf8,
    }

    for col, dtype in expected_cols.items():
        assert col in df.columns
        assert df.schema[col] == dtype

def test_primary_key_unique():
    df = pl.read_parquet(DIM_PLATFORM_PATH)
    assert df.height == df.select(pl.col("platform_id").n_unique()).item()

def test_platform_name_unique():
    df = pl.read_parquet(DIM_PLATFORM_PATH)
    assert df.height == df.select(pl.col("platform_name").n_unique()).item()

def test_platform_name_not_empty():
    df = pl.read_parquet(DIM_PLATFORM_PATH)
    assert df.filter(pl.col("platform_name").str.len_chars() == 0).height == 0
