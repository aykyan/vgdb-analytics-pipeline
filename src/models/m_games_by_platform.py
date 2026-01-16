
# Show total games, average rating, and total owners per platform

import polars as pl
from pathlib import Path

ANALYTICS_DIR = Path("data/processed/analytics")

FACT_GAME_METRICS_PATH   = ANALYTICS_DIR / "fact_game_metrics.parquet"
DIM_PLATFORM_PATH        = ANALYTICS_DIR / "dim_platform.parquet"

M_GAMES_BY_PLATFORM_PATH = ANALYTICS_DIR / "models/m_games_by_platform.parquet"

fact         = pl.read_parquet(FACT_GAME_METRICS_PATH)
dim_platform = pl.read_parquet(DIM_PLATFORM_PATH)

model = (
	fact.join(dim_platform, left_on="platform_id", right_on="platform_id")
	.group_by("platform_name")
	.agg([
		pl.count("game_id").alias("total_games"),
		pl.mean("rating").alias("avg_rating"),
		pl.sum("added_status_owned").alias("total_owners")
	])
)

model.write_parquet(M_GAMES_BY_PLATFORM_PATH)