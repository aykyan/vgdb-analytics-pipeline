import polars as pl
from pathlib import Path

STAGING_DIR   = Path("C:\\Users\\Jien\\Desktop\\dataverse\\data\\processed\\staging")
ANALYTICS_DIR = Path("C:\\Users\\Jien\\Desktop\\dataverse\\data\\processed\\analytics")

STG_GAME_INFO_PATH = Path("C:\\Users\\Jien\\Desktop\\dataverse\\data\\processed\\staging\\stg_game_info.parquet")
DIM_PLATFORM_PATH  = Path("C:\\Users\\Jien\\Desktop\\dataverse\\data\\processed\\analytics\\dim_platform.parquet")

FACT_GAME_METRICS_PATH = Path("C:\\Users\\Jien\\Desktop\\dataverse\\data\\processed\\analytics\\fact_game_metrics.parquet")

def build_fact_game_metrics() -> pl.DataFrame:
	"""
	Build fact table at grain: game x platform.
	"""
	
	games     = pl.read_parquet(STG_GAME_INFO_PATH)
	platforms = pl.read_parquet(DIM_PLATFORM_PATH)
	
	fact = (
		games
		.select([
			"game_id",
            "released_date",
            "rating",
            "metacritic_score",
            "ratings_count",
            "reviews_count",
            "added_status_owned",
            "added_status_playing",
            "platforms",
		])
		.explode("platforms")
		.rename({"platforms": "platform_name"})
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
            "metacritic_score",
            "ratings_count",
            "reviews_count",
            "added_status_owned",
            "added_status_playing",
		])
	)
	
	return fact
	
def main() -> None:
	ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
	
	df = build_fact_game_metrics()
	
	df.write_parquet(FACT_GAME_METRICS_PATH)
	
	print(f"fact_game_metrics written to {FACT_GAME_METRICS_PATH}")
	
	print(df.head(10))
	
	games = pl.read_parquet(STG_GAME_INFO_PATH).select(["game_id", "name"])
	
	platforms = pl.read_parquet(DIM_PLATFORM_PATH)
	
	pretty = (
		df
		.join(games,     on="game_id",     how="left")
		.join(platforms, on="platform_id", how="left")
		.select([
			"game_id",
			"name",
			"platform_name",
		])
		.head(20)
	)
	
	print(f"\nReadable sample (game x platform):\n{pretty}")
	
if __name__ == "__main__":
	main()
