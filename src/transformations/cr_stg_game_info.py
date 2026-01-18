
import os
import polars as pl
from pathlib import Path

def get_env_var(name: str) -> str:
    """
    Fetch required environment variable or fail fast.
    """
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def run_staging():
    # ------------------------------------------------------------------
    # Configuration (injected by environment)
    # ------------------------------------------------------------------
    raw_path     = Path(get_env_var("RAW_PATH"))
    staging_path = Path(get_env_var("STAGING_PATH"))

    input_file   = raw_path / "game_info.csv"
    output_file  = staging_path / "cr_stg_game_info.parquet"

    # Ensure output directory exists (idempotent)
    staging_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Read raw data
    # ------------------------------------------------------------------
    df = pl.read_csv(
        input_file,
        null_values=["", "null", "NULL"]
    )

    # ------------------------------------------------------------------
    # Transformations (existing logic preserved)
    # ------------------------------------------------------------------
    df = (
        df
        .with_columns([
            pl.col("released")
              .str.strptime(pl.Date, strict=False)
              .alias("released_date"),

            pl.col("platforms")
              .str.split("||"),

            pl.col("developers")
              .str.split("||"),

            pl.col("genres")
              .str.split("||"),

            pl.col("publishers")
              .str.split("||"),
        ])
        .drop("released")
    )

    # ------------------------------------------------------------------
    # Write staged output
    # ------------------------------------------------------------------
    df.write_parquet(output_file)

    print(f"[SUCCESS] Wrote staged data to {output_file}")
    print(f"[INFO] Row count: {df.height}")


def main():
    run_staging()


if __name__ == "__main__":
    main()
