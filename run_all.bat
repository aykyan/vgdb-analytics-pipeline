@echo off

echo 

echo Running profile_vgdb.py ...
python src\ingestion\profile_vgdb.py

echo Running dim_platform.py ...
python src\analytics\dim_platform.py

echo Running fact_game_metrics.py ...
python src\analytics\fact_game_metrics.py

echo Running stg_game_info.py ...
python src\transformations\stg_game_info.py

echo Running stg_game_identity.py ...
python src\transformations\stg_game_identity.py

echo Running tests ...
pytest tests

echo Done.
pause
