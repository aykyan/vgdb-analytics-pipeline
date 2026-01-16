
# Show top N games by rating and platform

import polars as pl
from polars import col
from pathlib import Path

ANALYTICS_DIR = Path("data/processed/analytics")

FACT_GAME_METRICS_PATH = ANALYTICS_DIR / "fact_game_metrics.parquet"
DIM_PLATFORM_PATH      = ANALYTICS_DIR / "dim_platform.parquet"
M_TOP_RATED_GAMES_PATH = ANALYTICS_DIR / "models/m_top_rated_games.parquet"

fact         = pl.read_parquet(FACT_GAME_METRICS_PATH)
dim_platform = pl.read_parquet(DIM_PLATFORM_PATH)

model = (
    fact.join(dim_platform, left_on="platform_id", right_on="platform_id")
        .with_columns([
            pl.col("rating").rank("dense").over("platform_name").alias("rank")
        ])
        .filter(col("rank") <= 10)
)

model.write_parquet(M_TOP_RATED_GAMES_PATH)