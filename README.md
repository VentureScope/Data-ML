# VentureScope Data-ML

Data collection, preprocessing, and forecasting stack for African job-market intelligence.

## What's Inside

| Component | Purpose |
|---|---|
| **Scraper** (`afriwork_hahu_scraper.ipynb`) | Pulls job postings from Afriwork and Hahu Jobs APIs |
| **Preprocessing Pipeline** (`Job_pipeline/`) | Transforms raw JSON/CSV job rows into structured ML-ready features |
| **Time Series Models** (`model/timeseries/`) | Prophet & ARIMA forecasting for per-role job demand trends |

## Repository Structure

```
Data-ML/
├── afriwork_hahu_scraper.ipynb          # Scraper notebook (Afriwork + Hahu)
├── requirements.txt                     # Python dependencies
├── Job_pipeline/
│   ├── run_preprocessing_pipeline.py    # One-command batch runner
│   ├── data/
│   │   ├── raw/                         # Input JSON/CSV files
│   │   │   ├── jobs_has_link.json       # Afriwork scraped jobs (structured)
│   │   │   └── no_link_jobs.json        # Telegram channel jobs (semi-structured)
│   │   └── processed/                   # Output CSVs (ML-ready features)
│   │       ├── jobs_has_link.csv
│   │       └── no_link_jobs.csv
│   ├── preprocessing/
│   │   ├── unified_preprocessor.py      # Orchestrator (Step 1-10)
│   │   ├── tech_job_validation.py       # Step 0: Tech job filter
│   │   ├── clean_text.py                # Step 1: Text cleaning
│   │   ├── job_id.py                    # Step 2: Deterministic ID
│   │   ├── date_features.py             # Step 3: Date features
│   │   ├── title_normalization.py       # Step 4: Title → canonical role
│   │   ├── description_embedding.py     # Step 5: Dense vector
│   │   ├── location_extraction.py       # Step 6: City/region/country
│   │   ├── remote_detection.py          # Step 7: Remote/hybrid/onsite
│   │   ├── job_type_extraction.py       # Step 8: Employment type
│   │   ├── education_extraction.py      # Step 9: Education level
│   │   ├── skills_extraction.py         # Step 10: Skills list
│   │   ├── semantic_utils.py            # Shared embedding encoder
│   │   └── gemini_key_selector.py       # Gemini API key rotation
│   ├── taxonomy/
│   │   ├── roles.json                   # Canonical role definitions
│   │   └── skills.json                  # Canonical skill definitions
│   └── tests/
│       ├── test_step1_clean_text.py ... test_step10_skills_extraction.py
│       ├── test_pipeline_target_features.py
│       ├── test_json_source_compat.py   # Multi-source schema tests
│       └── test_run_preprocessing_pipeline.py
└── model/
    └── timeseries/
        ├── prophet_pipeline.ipynb       # Prophet forecasting
        ├── arima_pipeline.ipynb         # ARIMA forecasting
        └── artifacts/                   # Saved models, forecasts, metrics
```

---

## 1) Data Collection

### Notebook Scraper

Use `afriwork_hahu_scraper.ipynb` to pull fresh data from job sites.

| Source | Endpoint | Auth |
|--------|----------|------|
| [Afriwork](https://afriworket.com) — all jobs | `api.afriworket.com/v1/graphql` | Anonymous |
| [Afriwork](https://afriworket.com) — tech jobs | `api.afriworket.com/v1/graphql` | Optional Bearer |
| [Hahu Jobs](https://www.hahu.jobs) — tech sector | `graph.aggregator.hahu.jobs/v1/graphql` | Anonymous |

### Telegram Channel Scraper

The `no_link_jobs.json` data comes from a separate Puppeteer-based scraper that extracts job postings from Ethiopian Telegram job channels. This scraper runs in GitHub Codespaces and produces semi-structured JSON with `raw_text`, `job_title`, `company`, `work_location`, and `job_type` fields.

---

## 2) Preprocessing Pipeline

The preprocessing pipeline transforms raw data into a uniform 16-column feature schema, regardless of input source.

### Supported Input Formats

| Source File | Format | Key Differences |
|---|---|---|
| `jobs_has_link.json` | Afriwork structured JSON | Clean fields: `company_name`, `job_type`, `job_site`, `skills[]`, `education_qualification`, `location` |
| `no_link_jobs.json` | Telegram semi-structured JSON | Has `raw_text` (full) + truncated `description`, compound `job_type` strings like `"On-site - Permanent (Full-time)"`, `work_location` |
| `*.csv` (legacy) | Afriwork/Hahu CSV exports | Fields: `title`, `description`, `entity_name`, `city`, `country`, `created_at` |

### Multi-Source Intelligence

The pipeline automatically handles schema differences across sources:

- **Field alias resolution** — `job_title`→`title`, `company_name`/`company`→`entity_name`, `posted_date`/`date`→`created_at`, `work_location`→`location`
- **Smart description selection** — When `raw_text` (full text) is longer than a truncated `description`, the pipeline uses `raw_text`
- **Structured hint consumption** — Pre-parsed fields like `job_type="FULL_TIME"`, `job_site="REMOTE"`, `skills=["Python","SQL"]`, and `education_qualification="BACHELORS_DEGREE"` are used as high-confidence hints before rule/NLP extraction
- **Non-job row filtering** — Telegram polls, announcements, and all-null rows are skipped automatically

### Pipeline Steps

| Step | Module | Technique | Outputs |
|---|---|---|---|
| 0 | `tech_job_validation.py` | Taxonomy keyword matching | Filter: keeps only tech-related jobs |
| 1 | `clean_text.py` | Unicode/HTML/emoji normalization | `clean_title`, `clean_description` |
| 2 | `job_id.py` | SHA-256 hash of title\|company\|date\|source | `job_id` |
| 3 | `date_features.py` | Multi-format date parsing + holidays | `timestamp`, `year_month`, `month`, `holiday_flag` |
| 4 | `title_normalization.py` | Semantic similarity against role taxonomy | `normalized_title` |
| 5 | `description_embedding.py` | `sentence-transformers` dense vector | `DescriptionVec` |
| 6 | `location_extraction.py` | Rules + spaCy NER + structured hints | `city`, `region`, `country` |
| 7 | `remote_detection.py` | Keywords + structured hints (`job_site`) | `is_remote` |
| 8 | `job_type_extraction.py` | Keywords + structured hints (`job_type`) | `job_type` |
| 9 | `education_extraction.py` | Regex + structured hints (`education_qualification`) | `education_level` |
| 10 | `skills_extraction.py` | Semantic embeddings + hint union merge | `skills` |

All extraction steps support optional **Gemini LLM fallback** when rule-based confidence is low (disabled by default for batch runs).

### Output Feature Schema (16 columns)

| Column | Type | Example |
|---|---|---|
| `year_month` | string | `2026-04` |
| `timestamp` | ISO string | `2026-04-27T14:37:20+00:00` |
| `month` | int | `4` |
| `holiday_flag` | bool | `false` |
| `job_id` | string | `e418e80840ef2767` |
| `company_name` | string | `Minaye PLC` |
| `job_title` | string | `Junior Data Analyst` |
| `normalized_title` | string | `Data Analyst` |
| `DescriptionVec` | float[] | `[-0.028, -0.000, ...]` |
| `city` | string | `Addis Ababa` |
| `region` | string | `Addis Ababa` |
| `country` | string | `Ethiopia` |
| `is_remote` | bool | `false` |
| `job_type` | string | `full_time` |
| `education_level` | string | `Bachelors` |
| `skills` | string[] | `["Python", "SQL", "Power BI"]` |

All output CSVs share this identical schema and can be safely concatenated:

```python
import pandas as pd
df = pd.concat([
    pd.read_csv("Job_pipeline/data/processed/jobs_has_link.csv"),
    pd.read_csv("Job_pipeline/data/processed/no_link_jobs.csv"),
], ignore_index=True)
```

---

## 3) Time Series Forecasting

Located in `model/timeseries/`. Forecasts per-role job demand using processed pipeline output.

| Notebook | Model | Output |
|---|---|---|
| `prophet_pipeline.ipynb` | Facebook Prophet | 6-month role demand forecast |
| `arima_pipeline.ipynb` | ARIMA | 6-month role demand forecast |

Both notebooks auto-detect the project root and read from `Job_pipeline/data/processed/`. Each saves models, forecasts, and cross-validation metrics to `artifacts/`.

---

## Installation

```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

### Dependencies

| Package | Purpose |
|---|---|
| `sentence-transformers` | Semantic embeddings for title/skill matching |
| `spacy` | NER-based location extraction |
| `holidays` | Ethiopian holiday calendar |
| `rapidfuzz` | Fuzzy string matching for normalization |
| `google-genai` | Gemini LLM fallback (optional) |
| `requests`, `pydantic` | HTTP + data validation |

---

## Running the Pipeline

```bash
# Process all files in data/raw/ → data/processed/
python Job_pipeline/run_preprocessing_pipeline.py

# With options
python Job_pipeline/run_preprocessing_pipeline.py --max-rows 100
python Job_pipeline/run_preprocessing_pipeline.py --enable-gemini-fallback
python Job_pipeline/run_preprocessing_pipeline.py --raw-dir Job_pipeline/data/raw --processed-dir Job_pipeline/data/processed
```

**Behavior:**
- Reads all `.json` and `.csv` files from `data/raw/`
- Writes processed CSVs to `data/processed/` with matching filenames
- Gemini fallback is **disabled by default** for stable batch throughput
- Non-tech jobs are filtered out via taxonomy matching

---

## Running Tests

```bash
python -m unittest discover -s Job_pipeline/tests -v
```

Test coverage includes:
- Per-step module output tests (Steps 1–10)
- End-to-end target feature contract test
- Multi-source JSON schema compatibility tests (field aliases, structured hints, `raw_text` fallback)
- Pipeline runner integration tests

---

## Environment Variables (Optional)

For Gemini LLM fallback, set API keys via `.env` or environment:

```bash
GEMINI_API_KEY=your_key_here
# or multiple keys for rotation:
GEMINI_API_KEY_1=key1
GEMINI_API_KEY_2=key2
```