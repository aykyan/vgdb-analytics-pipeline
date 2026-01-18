# VGDB Analytics Pipeline

##### End-to-end data pipeline transforming raw video game metadata into analytics-ready facts & dimensions.

This project takes a video game dataset and builds a clean, cloud-ready analytics warehouse using modern Python tooling and data engineering best practices.

The pipeline ingests raw CSV data from a video game database (VGDB), performs staging-level cleanup and canonicalization, builds dimensional models, and produces a fact table suitable for BI, analytics, and downstream machine-learning workloads.

![diagram](./docs/diagram.png)

### Dataset

The project uses a [public video game dataset](https://www.kaggle.com/datasets/jummyegg/rawg-game-dataset) containing:

- ~665k raw game identity records
- ~474k game metadata records
- Platform information stored as delimited strings
- Missing values and duplicate identifiers
- Heterogeneous categorical fields (platforms, genres, developers)

Includes:

- Staging models
- Dimensions and facts
- Analytical visualizations
