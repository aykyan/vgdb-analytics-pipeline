# VGDB Analytics Pipeline

#### End-to-end data pipeline transforming raw video game metadata into analytics-ready facts & dimensions.

This project takes a video game dataset and builds a clean, cloud-ready analytics warehouse using modern Python tooling and data engineering best practices.

The pipeline ingests raw CSV data from a video game database (VGDB), performs staging-level cleanup and canonicalization, builds dimensional models, and produces a fact table suitable for BI, analytics, and downstream machine-learning workloads.

![diagram](./docs/gallery/diagram.png)

### Dataset

The project uses a [public video game dataset](https://www.kaggle.com/datasets/jummyegg/rawg-game-dataset) containing:

- ~665k raw game identity records
- ~474k game metadata records
- Platform information stored as delimited strings
- Missing values and duplicate identifiers
- Heterogeneous categorical fields (platforms, genres, developers)

The datasets are excluded from this repository, but you can download them and place them in the `data/warehouse/vgdb` directory. Here's the structure of the additional directory that is missing in this repository:

```
    root
    \data
        \processed          -- Contains .parquet files
            \analytics
                \models
            \staging
        \warehouse
            \vgdb           -- Contains raw .csv datasets
```

### Overview

| Layer      | Artifact                 | Description                                      |
|------------|--------------------------|--------------------------------------------------|
| Raw        | game_id.csv              | Raw game identifiers and slugs                   |
| Raw        | game_info.csv            | Raw game metadata with nested platform fields    |
| Staging    | stg_game_identity        | Deduplicated, canonical game identifiers         |
| Staging    | stg_game_info            | Cleaned and typed game metadata                  |
| Dimension  | dim_platform             | Unique platforms with surrogate keys             |
| Fact       | fact_game_metrics        | Game metrics at (game, platform) grain           |

### Goals

The main purpose of this project is to learn everything related to data engineering and analytics by using raw real-world data and making it production-ready. The learning steps are documented in the `docs/overview.txt` file, including project's structure, tools used and basic definitions. Additionally, we would want to have:

- Basic (or, perhaps, advanced) analysis of the processed data
- Visualizations (diagrams, charts, tables)
- Expanding the warehouse with more/heavier datasets to work with Big Data
- Utilizing Big Data tools (Apache Hadoop/Spark/Kafka, MS Azure)
- ...or, at least, documenting them
- Utilizing Machine Learning Python libraries