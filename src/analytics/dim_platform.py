import polars as pl
from pathlib import Path

STAGING_DIR   = Path("C:\\Users\\Jien\\Desktop\\dataverse\\data\\processed\\staging")
ANALYTICS_DIR = Path("C:\\Users\\Jien\\Desktop\\dataverse\\data\\processed\\analytics")

STG_GAME_INFO_PATH = Path("C:\\Users\\Jien\\Desktop\\dataverse\\data\\processed\\staging\\stg_game_info.parquet")
DIM_PLATFORM_PATH  = Path("C:\\Users\\Jien\\Desktop\\dataverse\\data\\processed\\analytics\\dim_platform.parquet")

def build_dim_platform() -> pl.DataFrame:
	"""
	Build platform dimension table.
	
	Grain: one row per platform.
	"""
	
	df = pl.read_parquet(STG_GAME_INFO_PATH)
	
	platforms = (
		df.select(pl.col("platforms"))
		.explode("platforms")
		.filter(pl.col("platforms") != "")
		.unique()
		.sort("platforms")
		.with_row_index("platform_id", offset=1)
		.rename({"platforms": "platform_name"})
	)
	
	return platforms

def main() -> None:
	ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
	
	df = build_dim_platform()
	
	df.write_parquet(DIM_PLATFORM_PATH)
	
	print(f"dim_platform written to {DIM_PLATFORM_PATH}")
	print(df.head(10))
	
if __name__ == "__main__":
	main()
