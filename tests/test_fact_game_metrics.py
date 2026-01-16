import polars as pl
from pathlib import Path

ANALYTICS_DIR = Path("data/processed/analytics")
STAGING_DIR   = Path("data/processed/staging")

FACT_PATH     = ANALYTICS_DIR / "fact_game_metrics.parquet"
PLATFORM_PATH = ANALYTICS_DIR / "dim_platform.parquet"

def test_grain_game_platform_unique():
    df = pl.read_parquet(FACT_PATH)

    duplicates = (
        df.group_by(["game_id", "platform_id"])
        .len()
        .filter(pl.col("len") > 1)
    )

    assert duplicates.height == 0

def test_required_columns_not_null():
    df = pl.read_parquet(FACT_PATH)

    required = ["game_id", "platform_id"]
    for col in required:
        assert df.select(pl.col(col).null_count()).item() == 0

def test_platform_fk_integrity():
    fact = pl.read_parquet(FACT_PATH)
    platforms = pl.read_parquet(PLATFORM_PATH)

    invalid = fact.join(platforms, on="platform_id", how="anti")

    assert invalid.height == 0
