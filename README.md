# VentureScope Data-ML

Data collection and preprocessing stack for African job-market intelligence.

This repository includes:
- A scraper notebook that pulls job postings from Afriwork and Hahu Jobs.
- A production preprocessing pipeline that transforms raw CSV job rows into structured ML-ready features.

## Repository Structure

```
Data-ML/
	afriwork_hahu_scraper.ipynb
	requirements.txt
	Job_pipeline/
		README.md
		run_preprocessing_pipeline.py
		data/
			raw/
			processed/
			aggregated/
		preprocessing/
			clean_text.py
			job_id.py
			date_features.py
			title_normalization.py
			description_embedding.py
			location_extraction.py
			remote_detection.py
			job_type_extraction.py
			education_extraction.py
			skills_extraction.py
			semantic_utils.py
			gemini_key_selector.py
			unified_preprocessor.py
		taxonomy/
			roles.json
			skills.json
		tests/
			test_step1_clean_text.py
			...
			test_step10_skills_extraction.py
			test_pipeline_target_features.py
```

## 1) Data Collection (Notebook)

Use `afriwork_hahu_scraper.ipynb` to pull fresh source data.

### Sources

| Source | Endpoint | Auth |
|--------|----------|------|
| [Afri-work](https://afriworket.com) - all jobs | `https://api.afriworket.com/v1/graphql` | None (anonymous) |
| [Afri-work](https://afriworket.com) - software/tech jobs | `https://api.afriworket.com/v1/graphql` | Optional Bearer token |
| [Hahu Jobs](https://www.hahu.jobs) - tech sector | `https://graph.aggregator.hahu.jobs/v1/graphql` | None |

### Quick Start (Colab)

1. Open [Google Colab](https://colab.research.google.com/) and load `afriwork_hahu_scraper.ipynb`.
2. Run all notebook cells in sequence.
3. Save CSV outputs to Drive or local Colab storage.

Expected raw outputs:
- `afriwork_all_jobs_YYYYMMDD_HHMMSS.csv`
- `afriwork_tech_jobs_YYYYMMDD_HHMMSS.csv`
- `hahu_tech_jobs_YYYYMMDD_HHMMSS.csv`

## 2) Preprocessing Pipeline (Production)

The preprocessing system is implemented under `Job_pipeline/` and runs Step 1 to Step 10 end-to-end.

### Core Techniques

- Deterministic text cleaning, ID generation, and date feature engineering.
- Semantic embeddings (`sentence-transformers`) for title/skill matching and description vectors.
- Rule/regex extraction for location, remote mode, job type, and education.
- Optional Gemini fallbacks when confidence is low.
- Time-seeded random Gemini key selection through environment configuration.

### Target Processed Features

Each processed row includes:
- `year_month`
- `timestamp`
- `month`
- `holiday_flag`
- `job_id`
- `job_title`
- `normalized_title`
- `DescriptionVec`
- `city`
- `region`
- `country`
- `is_remote`
- `job_type`
- `education_level`
- `skills`

## Installation

From repository root:

```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

Main dependencies:
- `requests`
- `pydantic`
- `google-genai`
- `sentence-transformers`
- `spacy`
- `holidays`
- `rapidfuzz`

## Run the Full Preprocessing Pipeline

From repository root:

```bash
python Job_pipeline/run_preprocessing_pipeline.py
```

Optional flags:

```bash
python Job_pipeline/run_preprocessing_pipeline.py --max-rows 100
python Job_pipeline/run_preprocessing_pipeline.py --enable-gemini-fallback
python Job_pipeline/run_preprocessing_pipeline.py --raw-dir Job_pipeline/data/raw --processed-dir Job_pipeline/data/processed
```

Behavior notes:
- Input files are read from `Job_pipeline/data/raw/`.
- Output files are written to `Job_pipeline/data/processed/` with the same filenames.
- Gemini fallback is disabled by default for stable high-throughput batch runs.

## Running Tests

```bash
python -m unittest discover -s Job_pipeline/tests -v
```

## Additional Documentation

For full pipeline module-level documentation, see `Job_pipeline/README.md`.