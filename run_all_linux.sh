#!/bin/bash

# src/ingestion

echo "Running profile_vgdb.py ..."
python3 src/ingestion/profile_vgdb.py

# src/analytics

echo "Running dim_platform.py ..."
python3 src/analytics/dim_platform.py

echo "Running fact_game_metrics.py ..."
python3 src/analytics/fact_game_metrics.py

# src/transformations

echo "Running stg_game_info.py ..."
python3 src/transformations/stg_game_info.py

echo "Running stg_game_identity.py ..."
python3 src/transformations/stg_game_identity.py

# src/models

echo "Running m_games_by_platform.py ..."
python3 src/models/m_games_by_platform.py

echo "Running m_genre_summary.py ..."
python3 src/models/m_genre_summary.py

echo "Running m_top_rated_games.py ..."
python3 src/models/m_top_rated_games.py

# tests

echo "Running tests ..."
pytest tests

echo "Done."
