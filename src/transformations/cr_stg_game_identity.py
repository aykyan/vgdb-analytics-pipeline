
import os
import polars as pl
from pathlib import Path

def get_env_var(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def run_staging():
    raw_path     = Path(get_env_var("RAW_PATH"))
    staging_path = Path(get_env_var("STAGING_PATH"))

    input_file   = raw_path / "game_id.csv"
    output_file  = staging_path / "cr_stg_game_identity.parquet"

    staging_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Read raw data
    # ------------------------------------------------------------------
    df = pl.read_csv(
        input_file,
        null_values=["", "null", "NULL"]
    )

    # ------------------------------------------------------------------
    # Cleanup & canonicalization
    # ------------------------------------------------------------------
    df = (
        df
        .filter(pl.col("id").is_not_null())
        .with_columns(
            pl.col("slug")
              .cast(pl.Utf8)
              .str.strip_chars()
              .str.to_lowercase()
        )
        # Deduplicate by game id (canonical row)
        .unique(subset=["id"], keep="first")
    )

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
