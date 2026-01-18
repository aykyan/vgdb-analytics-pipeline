@echo off

echo 

REM src\transformations

set RAW_PATH=data/warehouse/vgdb
set STAGING_PATH=data/processed/staging

echo Running cr_stg_game_identity.py ...
python src\transformations\cr_stg_game_identity.py

echo Running cr_stg_game_info.py ...
python src\transformations\cr_stg_game_info.py

echo Done.
pause
