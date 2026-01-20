
# Aggregate games by genre (from exploded lists in stg_game_info)

import os
import polars as pl
from pathlib import Path

def get_env_var(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

STAGING_DIR   = Path(get_env_var("STAGING_PATH"))
ANALYTICS_DIR = Path(get_env_var("ANALYTICS_PATH"))

STG_GAME_INFO_PATH   = STAGING_DIR / "stg_game_info.parquet"
M_GENRE_SUMMARY_PATH = ANALYTICS_DIR / "models/m_genre_summary.parquet"

stg = pl.read_parquet(STG_GAME_INFO_PATH)

model = (
	stg.explode("genres")
       .group_by("genres")
       .agg([
           pl.count("game_id").alias("total_games"),
           pl.mean("rating").alias("avg_rating")
       ])
)

model.write_parquet(M_GENRE_SUMMARY_PATH)