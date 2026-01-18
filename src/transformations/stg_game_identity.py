import polars as pl
from pathlib import Path

STAGING_DIR      = Path("data/processed/staging")

RAW_GAME_ID_PATH = Path("data/warehouse/vgdb/game_id.csv")
STG_GAME_ID_PATH = STAGING_DIR / "stg_game_identity.parquet"

GAME_ID_SCHEMA = {
	"id":   pl.Int64,
	"slug": pl.Utf8,
}

def build_stg_game_identity() -> pl.DataFrame:
	"""
	Build staging table for game identity.
	
	Grain: one row per game.
	"""
	
	df = pl.read_csv(RAW_GAME_ID_PATH, schema_overrides=GAME_ID_SCHEMA, infer_schema_length=0)
	
	df = (
		df.rename({"id": "game_id"})
		.select(["game_id", "slug"])
		.group_by("game_id")
		.agg(pl.col("slug").sort().first())
	)
	
	print("Raw game_id rows:", df.height)
	
	return df
	
def run_quality_checks(df: pl.DataFrame) -> None:
	"""
	Basic data quality checks.
	"""
	
	# Null checks
	nulls = df.null_count()
	if nulls.select(pl.sum_horizontal(pl.all())).item() > 0:
		raise ValueError(f"Null values detected:\n{nulls}")
		
	# Uniqueness checks
	if df.select(pl.col("game_id").n_unique()).item() != df.height:
		raise ValueError("Duplicate game_id values detected")
		
	if df.select(pl.col("slug").n_unique()).item() != df.height:
		raise ValueError("Duplicate slug values detected")
		
def main() -> None:
	STAGING_DIR.mkdir(parents=True, exist_ok=True)
	
	df = build_stg_game_identity()
	
	run_quality_checks(df)
	
	df.write_parquet(STG_GAME_ID_PATH)
	
	print(f"stg_game_identity written to {STG_GAME_ID_PATH}\n")
	print(df.head(5))
	
if __name__ == "__main__":
	main()
