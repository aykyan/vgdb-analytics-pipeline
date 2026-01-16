
# Aggregate games by genre (from exploded lists in stg_game_info)

import polars as pl
from pathlib import Path

STAGING_DIR   = Path("data/processed/staging")
ANALYTICS_DIR = Path("data/processed/analytics")

STG_GAME_INFO_PATH   = STAGING_DIR / "stg_game_info.parquet"
M_GENRE_SUMMARY_PATH = ANALYTICS_DIR / "models/m_games_by_platform.parquet"

model = (
	stg.explode("genres")
       .groupby("genres")
       .agg([
           pl.count("id").alias("total_games"),
           pl.mean("rating").alias("avg_rating")
       ])
)

model.write_parquet(M_GENRE_SUMMARY_PATH)