
import os
import polars as pl
from pathlib import Path

def get_env_var(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def build_dim_platform():
    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------
    staging_path   = Path(get_env_var("STAGING_PATH"))
    analytics_path = Path(get_env_var("ANALYTICS_PATH"))

    input_file   = staging_path / "stg_game_info.parquet"
    output_file  = analytics_path / "dim_platform.parquet"

    analytics_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Read staged data
    # ------------------------------------------------------------------
    df = pl.read_parquet(input_file)

    # ------------------------------------------------------------------
    # Extract and canonicalize platforms
    # ------------------------------------------------------------------
    platforms = (
        df
        .select(pl.col("platforms"))
        .explode("platforms")
        .filter(pl.col("platforms").is_not_null())
        .with_columns(
            pl.col("platforms")
              .str.strip_chars()
              .str.to_lowercase()
              .alias("platform_name")
        )
        .select("platform_name")
        .unique()
        .sort("platform_name")
    )

    # ------------------------------------------------------------------
    # Generate deterministic surrogate keys
    # ------------------------------------------------------------------
    platforms = platforms.with_row_index(
        name="platform_id",
        offset=1
    )

    # ------------------------------------------------------------------
    # Write dimension
    # ------------------------------------------------------------------
    platforms.write_parquet(output_file)

    print(f"[SUCCESS] Wrote dim_platform to {output_file}")
    print(f"[INFO] Platform count: {platforms.height}")


def main():
    build_dim_platform()


if __name__ == "__main__":
    main()
