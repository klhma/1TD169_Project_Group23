# Design Notes

## 1TD169 Project — Group 23

### Overview

_Briefly describe the problem you are solving and the dataset you are using._

---

### Architecture

_Describe the high-level architecture of your solution here. Reference the diagram in README.md._

---

### ETL Design (`src/etl_job.py`)

- **Input:** Raw data files stored in `data/raw/`
- **Transformations:** _(list the cleaning steps you apply)_
- **Output:** Cleaned data written to `data/cleaned/`

---

### Analysis Design (`src/analysis_job.py`)

- **Input:** Cleaned data from `data/cleaned/`
- **Computation:** _(describe the main analytical computation, e.g. word count, aggregation, join)_
- **Output:** Results written to `data/output/`

---

### Scaling Strategy

We run three benchmarks with dataset sizes of 100 MB, 500 MB, and 1 000 MB.

| Test | Dataset Size | Notes |
|------|-------------|-------|
| 1    | 100 MB      |       |
| 2    | 500 MB      |       |
| 3    | 1 000 MB    |       |

_Add observations and conclusions after running `scripts/run_benchmark.sh`._

---

### Known Limitations / Future Work

- _List any limitations or potential improvements here._
