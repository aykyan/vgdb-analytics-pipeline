import polars as pl
from pathlib import Path

GAME_ID_PATH   = Path("C:\\Users\\Jien\\Desktop\\dataverse\\data\\warehouse\\vgdb\\game_id.csv")
GAME_INFO_PATH = Path("C:\\Users\\Jien\\Desktop\\dataverse\\data\\warehouse\\vgdb\\game_info.csv")

def profile_dataframe(df: pl.DataFrame, name: str) -> None:
	"""Print basic profiling information for a DataFrame."""
	print(
		"\n"
		f"{'=' * 80}"
		f"\nProfiling: {name}\n"
		f"{'=' * 80}"
		"\n"
		f"Shape:       {df.shape}\n"
		f"Null count:  {df.null_count()}\n"
		f"Sample rows: {df.head(5)}\n"
	)
	
def main() -> None:
	print("Loading VGDB datasets...\n")
	
	game_id_schema = {
		"id":   pl.UInt64,
		"slug": pl.Utf8,
	}
	
	game_id_df   = pl.read_csv(GAME_ID_PATH, schema=game_id_schema, infer_schema_length=0)
	game_info_df = pl.read_csv(GAME_INFO_PATH)
	
	profile_dataframe(game_id_df,   "game_id.csv")
	profile_dataframe(game_info_df, "game_info.csv")
	
	print(
		"\n"
		f"{'=' * 80}"
		"\nID Integrity Checks\n"
		f"{'=' * 80}"
		"\n"
		"Duplicate IDs in game_id.csv:"
		"\n"
		f"{game_id_df.group_by("id").len().filter(pl.col("len") > 1)}"
		"\n"
		"Duplicate IDs in game_info.csv:"
		"\n"
		f"{game_info_df.group_by("id").len().filter(pl.col("len") > 1)}"
		"\n"
	)
	
	ids_in_info_not_in_id = (
		game_info_df.select("id").unique().join(
			game_id_df.select("id").unique(), on="id", how="anti"
		)
	)
	
	print(
		"\n"
		"IDs in game_info but missing in game_id:"
		"\n"
		f"{ids_in_info_not_in_id.head(10)}"
		"\n"
	)
	
	ids_in_id_not_in_info = (
		game_id_df.select("id").unique().join(
			game_info_df.select("id").unique(), on="id", how="anti"
		)
	)
	
	print(
		"\n"
		"IDs in game_id but missing in game_info:"
		"\n"
		f"{ids_in_id_not_in_info.head(10)}"
		"\n"
	)
	
if __name__ == "__main__":
	main()
	
	