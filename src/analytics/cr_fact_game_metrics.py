
import os
import polars as pl
from pathlib import Path

def get_env_var(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def build_fact_game_metrics():
    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------
    staging_path = Path(get_env_var("STAGING_PATH"))
    dim_path     = Path(get_env_var("DIM_PATH"))
    fact_path    = Path(get_env_var("FACT_PATH"))

    stg_file          = staging_path / "stg_game_info.parquet"
    dim_platform_file = dim_path / "cr_dim_platform.parquet"
    output_file       = fact_path / "cr_fact_game_metrics.parquet"

    fact_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Read inputs
    # ------------------------------------------------------------------
    games     = pl.read_parquet(stg_file)
    platforms = pl.read_parquet(dim_platform_file)

    # ------------------------------------------------------------------
    # Explode game-platform relationships
    # ------------------------------------------------------------------
    exploded = (
        games
        .select([
            "game_id",
            "released_date",
            "rating",
            "ratings_count",
            "reviews_count",
            "added_status_owned",
            "added_status_playing",
            "platforms",
        ])
        .explode("platforms")
        .filter(pl.col("platforms").is_not_null())
        .with_columns(
            pl.col("platforms")
              .str.strip_chars()
              .str.to_lowercase()
              .alias("platform_name")
        )
        .drop("platforms")
    )

    # ------------------------------------------------------------------
    # Join to platform dimension
    # ------------------------------------------------------------------
    fact = (
        exploded
        .join(
            platforms,
            on="platform_name",
            how="inner"
        )
        .select([
            "game_id",
            "platform_id",
            "released_date",
            "rating",
            "ratings_count",
            "reviews_count",
            "added_status_owned",
            "added_status_playing",
        ])
    )

    # ------------------------------------------------------------------
    # Write fact table
    # ------------------------------------------------------------------
    fact.write_parquet(output_file)

    print(f"[SUCCESS] Wrote fact_game_metrics to {output_file}")
    print(f"[INFO] Row count: {fact.height}")


def main():
    build_fact_game_metrics()


if __name__ == "__main__":
    main()
