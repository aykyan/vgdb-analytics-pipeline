#!/bin/bash

echo

export RAW_PATH=data/warehouse/vgdb
export STAGING_PATH=data/processed/staging
export DIM_PATH=data/processed/analytics
export FACT_PATH=data/processed/analytics

# src/analytics

echo "Running cr_dim_platform.py ..."
python3 src/analytics/cr_dim_platform.py

echo "Running cr_fact_game_metrics.py ..."
python3 src/analytics/cr_fact_game_metrics.py

# src/transformations

echo "Running cr_stg_game_identity.py ..."
python3 src/transformations/cr_stg_game_identity.py

echo "Running cr_stg_game_info.py ..."
python3 src/transformations/cr_stg_game_info.py

echo "Done."
read -p "Press Enter to continue..."
