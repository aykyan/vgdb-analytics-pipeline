@echo off

echo 

REM src\ingestion

echo Running profile_vgdb.py ...
python src\ingestion\profile_vgdb.py

REM src\analytics

echo Running dim_platform.py ...
python src\analytics\dim_platform.py

echo Running fact_game_metrics.py ...
python src\analytics\fact_game_metrics.py

REM src\transformations

echo Running stg_game_info.py ...
python src\transformations\stg_game_info.py

echo Running stg_game_identity.py ...
python src\transformations\stg_game_identity.py

REM src\models

echo Running m_games_by_platform.py ...
python src\models\m_games_by_platform.py

echo Running m_genre_summary.py ...
python src\models\m_genre_summary.py

echo Running m_top_rated_games.py ...
python src\models\m_top_rated_games.py

REM tests

echo Running tests ...
pytest tests

echo Done.
pause
