import polars as pl
from pathlib import Path

STAGING_DIR = Path("data/processed/staging")
TABLE_PATH  = STAGING_DIR / "stg_game_identity.parquet"

def test_schema():
    df = pl.read_parquet(TABLE_PATH)

    expected_cols = {
        "game_id": pl.Int64,
        "slug":    pl.Utf8,
    }

    for col, dtype in expected_cols.items():
        assert col in df.columns
        assert df.schema[col] == dtype

def test_primary_key_not_null():
    df = pl.read_parquet(TABLE_PATH)
    assert df.select(pl.col("game_id").null_count()).item() == 0


def test_primary_key_unique():
    df = pl.read_parquet(TABLE_PATH)
    assert df.height == df.select(pl.col("game_id").n_unique()).item()


def test_slug_unique():
    df = pl.read_parquet(TABLE_PATH)
    assert df.height == df.select(pl.col("slug").n_unique()).item()
