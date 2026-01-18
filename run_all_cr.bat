@echo off

echo 

set RAW_PATH=data/warehouse/vgdb
set STAGING_PATH=data/processed/staging
set DIM_PATH=data/processed/analytics
set FACT_PATH=data/processed/analytics

REM src\analytics

echo Running cr_dim_platform.py ...
python src\analytics\cr_dim_platform.py

echo Running cr_fact_game_metrics.py ...
python src\analytics\cr_fact_game_metrics.py

REM src\transformations

echo Running cr_stg_game_identity.py ...
python src\transformations\cr_stg_game_identity.py

echo Running cr_stg_game_info.py ...
python src\transformations\cr_stg_game_info.py

echo Done.
pause
