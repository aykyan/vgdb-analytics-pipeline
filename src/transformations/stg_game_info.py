import polars as pl
from pathlib import Path

STAGING_DIR = Path("C:\\Users\\Jien\\Desktop\\dataverse\\data\\processed\\staging")

RAW_GAME_INFO_PATH = Path("C:\\Users\\Jien\\Desktop\\dataverse\\data\\warehouse\\vgdb\\game_info.csv")
STG_GAME_INFO_PATH = Path("C:\\Users\\Jien\\Desktop\\dataverse\\data\\processed\\staging\\stg_game_info.parquet")

GAME_INFO_SCHEMA = {
    "id":                   pl.Int64,
    "slug":                 pl.Utf8,
    "name":                 pl.Utf8,
    "metacritic":           pl.Int64,
    "released":             pl.Utf8,
    "tba":                  pl.Boolean,
    "updated":              pl.Utf8,
    "website":              pl.Utf8,
    "rating":               pl.Float64,
    "rating_top":           pl.Int64,
    "playtime":             pl.Float64,
    "achievements_count":   pl.Int64,
    "ratings_count":        pl.Int64,
    "suggestions_count":    pl.Int64,
    "game_series_count":    pl.Int64,
    "reviews_count":        pl.Int64,
    "platforms":            pl.Utf8,
    "developers":           pl.Utf8,
    "genres":               pl.Utf8,
    "publishers":           pl.Utf8,
    "esrb_rating":          pl.Utf8,
    "added_status_yet":     pl.Int64,
    "added_status_owned":   pl.Int64,
    "added_status_beaten":  pl.Int64,
    "added_status_toplay":  pl.Int64,
    "added_status_dropped": pl.Int64,
    "added_status_playing": pl.Int64,
}

def parse_delimited(col: str, delimiter: str = "||") -> pl.Expr:
	"""
	Parse delimited string column into list[str].
	Handles nulls and empty strings safely.
	"""
	
	return (
		pl.when(pl.col(col).is_null() | (pl.col(col).str.strip_chars() == ""))
		.then(pl.lit([])).otherwise(
			pl.col(col).str.split(delimiter).list.eval(
				pl.element().str.strip_chars()
			)
		)
		.alias(col)
	)
	
def build_stg_game_info() -> pl.DataFrame:
	"""
	Build staging table for game information.
	
	Grain: one row per game.
	"""
	
	df = pl.read_csv(
		RAW_GAME_INFO_PATH,
		schema_overrides=GAME_INFO_SCHEMA,
		infer_schema_length=0,
	)
	
	df = df.rename({
		"id": "game_id",
		"metacritic": "metacritic_score",
		"released": "released_date",
		"updated": "updated_at",
		"playtime": "playtime_hours",
	})
	
	df = df.with_columns(
		# Dates / timestamps
		pl.col("released_date").str.strptime(pl.Date, strict=False),
		pl.col("updated_at").str.strptime(pl.Datetime, strict=False),
		
		# Parsed arrays
		parse_delimited("platforms"),
		parse_delimited("developers"),
		parse_delimited("genres"),
		parse_delimited("publishers"),
	)
	
	return df
	
def run_quality_checks(df: pl.DataFrame) -> None:
	"""
	Core data quality checks.
	"""
	
	# Primary key
	if df.select(pl.col("game_id").n_unique()).item() != df.height:
		raise ValueError("Duplicate game_id values detected in game_info")
		
	# Required fields
	required = ["game_id", "name"]
	nulls = df.select(required).null_count()
	if nulls.select(pl.sum_horizontal(pl.all())).item() > 0:
		raise ValueError(f"Nulls detected in required fields:\n{nulls}")
		
	# Non-negative metrics
	metric_cols = [
		"ratings_count",
		"reviews_count",
		"suggestions_count",
		"game_series_count",
	]
	
	for col in metric_cols:
		if df.filter(pl.col(col) < 0).height > 0:
			raise ValueError(f"Negative values detected in {col}")
			
def main() -> None:
	STAGING_DIR.mkdir(parents=True, exist_ok=True)
	
	df = build_stg_game_info()
	
	run_quality_checks(df)
	
	df.write_parquet(STG_GAME_INFO_PATH)
	
	print(f"stg_game_info written to {STG_GAME_INFO_PATH}")
	print(df.select("game_id", "name", "platforms", "genres").head(5))
	
if __name__ == "__main__":
	main()
