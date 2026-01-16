import polars as pl
from pathlib import Path

STAGING_DIR = Path("data/processed/staging")
TABLE_PATH  = STAGING_DIR / "stg_game_info.parquet"

def test_schema_contains_expected_columns():
    df = pl.read_parquet(TABLE_PATH)

    expected_columns = [
        "game_id",
        "name",
        "released_date",
        "updated_at",
        "rating",
        "metacritic_score",
        "ratings_count",
        "reviews_count",
        "platforms",
        "genres",
        "developers",
        "publishers",
    ]

    for col in expected_columns:
        assert col in df.columns

def test_primary_key_unique():
    df = pl.read_parquet(TABLE_PATH)
    assert df.height == df.select(pl.col("game_id").n_unique()).item()

def test_required_fields_not_null():
    df = pl.read_parquet(TABLE_PATH)

    required = ["game_id", "name"]
    for col in required:
        assert df.select(pl.col(col).null_count()).item() == 0

def test_array_columns_are_lists():
    df = pl.read_parquet(TABLE_PATH)

    array_cols = ["platforms", "genres", "developers", "publishers"]
    for col in array_cols:
        assert df.schema[col] == pl.List(pl.Utf8)

def test_no_empty_strings_in_arrays():
    df = pl.read_parquet(TABLE_PATH)

    for col in ["platforms", "genres", "developers", "publishers"]:
        bad = df.filter(
            pl.col(col)
    		.list.eval(pl.element().str.len_chars() == 0)
    		.list.any()
        )
        assert bad.height == 0
