
import os
import polars as pl
from pathlib import Path

GAME_ID_SCHEMA = {
    "id":   pl.Int64,
    "slug": pl.Utf8,
}

def get_env_var(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def run_quality_checks(df: pl.DataFrame) -> None:
    """
    Basic data quality checks.
    """
    
    # Null checks
    nulls = df.null_count()
    if nulls.select(pl.sum_horizontal(pl.all())).item() > 0:
        raise ValueError(f"Null values detected:\n{nulls}")
        
    # Uniqueness checks
    if df.select(pl.col("game_id").n_unique()).item() != df.height:
        raise ValueError("Duplicate game_id values detected")
        
    if df.select(pl.col("slug").n_unique()).item() != df.height:
        raise ValueError("Duplicate slug values detected")

def run_staging():
    raw_path     = Path(get_env_var("RAW_PATH"))
    staging_path = Path(get_env_var("STAGING_PATH"))

    input_file   = raw_path / "game_id.csv"
    output_file  = staging_path / "stg_game_identity.parquet"

    staging_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Read raw data
    # ------------------------------------------------------------------
    df = pl.read_csv(
        input_file,
        schema_overrides=GAME_ID_SCHEMA,
        infer_schema_length=0
    )

    # ------------------------------------------------------------------
    # Cleanup & canonicalization
    # ------------------------------------------------------------------
    df = (
        df.rename({"id": "game_id"})
        .select(["game_id", "slug"])
        .group_by("game_id")
        .agg(pl.col("slug").sort().first())
    )

    df = (
        df
        .filter(pl.col("game_id").is_not_null())
        .with_columns(
            pl.col("slug")
              .cast(pl.Utf8)
              .str.strip_chars()
              .str.to_lowercase()
        )
        # Deduplicate by game id (canonical row)
        .unique(subset=["game_id"], keep="first")
    )

    run_quality_checks(df)

    # ------------------------------------------------------------------
    # Write staged output
    # ------------------------------------------------------------------
    df.write_parquet(output_file)

    print(f"[SUCCESS] Wrote staged identity data to {output_file}")
    print(f"[INFO] Row count after deduplication: {df.height}")


def main():
    run_staging()


if __name__ == "__main__":
    main()
