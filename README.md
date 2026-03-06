# 1TD169_Project_Group23

## Setup Instructions

### Prerequisites

- Python 3.8+
- Apache Spark 3.x
- Java 8 or 11 (required by Spark)
- A Hadoop/Spark cluster or a local multi-node setup

### 1. Clone the repository

```bash
git clone https://github.com/klhma/1TD169_Project_Group23.git
cd 1TD169_Project_Group23
```

### 2. Configure paths and constants

Edit `src/config.py` to match your cluster's file paths and settings.

### 3. Provision infrastructure (optional)

If you need to set up VMs, see the scripts in `infrastructure/`.

### 4. Deploy code to the cluster

```bash
bash scripts/deploy.sh
```

### 5. Run the ETL job

```bash
spark-submit src/etl_job.py
```

### 6. Run the analysis job

```bash
spark-submit src/analysis_job.py
```

### 7. Run scaling benchmarks

```bash
bash scripts/run_benchmark.sh
```

---

## Architecture Diagram

```
+------------------+       +------------------+       +-------------------+
|   Raw Data (S3/  |       |   ETL Job        |       |   Analysis Job    |
|   HDFS / local)  +------>+  (etl_job.py)    +------>+ (analysis_job.py) |
+------------------+       +------------------+       +-------------------+
                                    |                           |
                              Cleaned Data               Results / Output
                            (Parquet / CSV)           (Parquet / CSV / stdout)
```

---

## Directory Structure

```
project-repo/
|-- README.md                  # Setup instructions & Architecture diagram
|-- data/                      # Sample data (Do NOT commit large files!)
|-- infrastructure/            # Scripts to provision VMs (optional)
|-- src/
|   |-- etl_job.py             # Cleaning and ingestion logic
|   |-- analysis_job.py        # Main Spark/MapReduce logic
|   +-- config.py              # Configuration (paths, constants)
|-- scripts/
|   |-- deploy.sh              # Helper to copy code to the cluster
|   +-- run_benchmark.sh       # Script to run the 3 scaling tests automatically
|-- notebooks/                 # Jupyter notebooks for initial data exploration
+-- docs/                      # Contribution statements and design notes
```

---

## Contributing

See `docs/` for contribution statements and design notes.
