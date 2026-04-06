# Job Pipeline

End-to-end preprocessing pipeline for job-post data, from raw CSV rows to structured feature CSVs.

This folder contains:
- Modular preprocessing components (Step 1 to Step 10)
- A unified row-level orchestrator
- A one-command batch runner for all raw files
- Taxonomy files for roles and skills
- Test suite for module and contract validation

## Folder Structure

```
Job_pipeline/
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
    test_step2_job_id.py
    test_step3_date_features.py
    test_step4_title_normalization.py
    test_step5_description_embedding.py
    test_step6_location_extraction.py
    test_step7_remote_detection.py
    test_step8_job_type_extraction.py
    test_step9_education_extraction.py
    test_step10_skills_extraction.py
    test_pipeline_target_features.py
    test_utils.py
  run_preprocessing_pipeline.py
```

## Data Directories

- `data/raw/`
  - Input CSV files to preprocess.
- `data/processed/`
  - Output CSV files from the preprocessing run.
  - Output files keep the same filename as input files.
- `data/aggregated/`
  - Reserved for downstream aggregation features (later steps).

## Taxonomy Files

- `taxonomy/roles.json`
  - Canonical role definitions and aliases for title normalization.
- `taxonomy/skills.json`
  - Canonical skill definitions, related skills, and metadata for skill extraction.

## Preprocessing Modules (Step 1 to Step 10)

### 1) `preprocessing/clean_text.py`
- Purpose: Clean title and description text.
- Technique:
  - Unicode normalization
  - HTML removal
  - Emoji removal
  - punctuation/whitespace normalization
  - boilerplate phrase removal
- Output fields:
  - `clean_title`
  - `clean_description`
- Fallback: none (deterministic module)

### 2) `preprocessing/job_id.py`
- Purpose: Generate deterministic job ID.
- Technique:
  - Normalize text fields
  - Convert date to month granularity (`YYYY-MM` by default)
  - Hash signature (`title|company|date|source`) via SHA256
- Output fields:
  - `job_id`
- Fallback: none (deterministic module)

### 3) `preprocessing/date_features.py`
- Purpose: Parse dates and derive time features.
- Technique:
  - Multi-format datetime parsing
  - `year`, `month`, `year_month`, `week`, `quarter`, `timestamp`
  - Holiday detection using `holidays` package
- Output fields:
  - `timestamp`, `year`, `month`, `year_month`, `week`, `quarter`, `holiday_flag`
- Fallback: no LLM fallback; configurable holiday behavior

### 4) `preprocessing/title_normalization.py`
- Purpose: Map noisy titles to canonical role names.
- Technique:
  - Semantic similarity with `sentence-transformers`
  - Cosine scoring against role taxonomy embeddings
  - Title alignment boosts
  - `rapidfuzz` for fuzzy normalization in fallback parsing
- Output fields:
  - `normalized_title`, `confidence_score`, `method_used`
- Fallback:
  - Gemini prompt extraction when score is below threshold
  - If unavailable: `method_used=embedding_fallback_unavailable`

### 5) `preprocessing/description_embedding.py`
- Purpose: Produce dense vector for job descriptions.
- Technique:
  - `sentence-transformers` semantic embedding
- Output fields:
  - `DescriptionVec`, `embedding_dim`, `embedding_model`
- Fallback: none (no LLM fallback by design)

### 6) `preprocessing/location_extraction.py`
- Purpose: Extract `city`, `region`, `country`.
- Technique:
  - Rule/regex parsing
  - spaCy NER (`en_core_web_sm`) for GPE/LOC entities
- Output fields:
  - `city`, `region`, `country`, `confidence_score`, `method_used`
- Fallback:
  - Gemini JSON extraction fallback when rule/NER is inconclusive

### 7) `preprocessing/remote_detection.py`
- Purpose: Detect remote/hybrid/onsite mode and boolean remote flag.
- Technique:
  - Keyword scoring rules
- Output fields:
  - `is_remote`, `remote_mode`, `confidence_score`, `method_used`
- Fallback:
  - Gemini single-label fallback when no keyword evidence exists

### 8) `preprocessing/job_type_extraction.py`
- Purpose: Extract employment type.
- Technique:
  - Keyword/regex classification
  - Supported labels:
    - `full_time`, `part_time`, `internship`, `contractual`, `temporary`, `freelance`
- Output fields:
  - `job_type`, `confidence_score`, `method_used`
- Fallback:
  - Gemini one-label fallback when no confident rule match exists

### 9) `preprocessing/education_extraction.py`
- Purpose: Extract minimum education requirement.
- Technique:
  - Regex-first level detection (`PhD`, `Masters`, `Bachelors`, `Diploma`)
- Output fields:
  - `education_level`, `confidence_score`, `method_used`
- Fallback:
  - Gemini fallback when regex rules find no signal

### 10) `preprocessing/skills_extraction.py`
- Purpose: Extract high-signal skill list.
- Technique:
  - Semantic similarity via `sentence-transformers`
  - Mention-aware precision gating
  - Canonical normalization with `rapidfuzz`
- Output fields:
  - `skills`, `skills_count`, `confidence_score`, `method_used`
- Fallback:
  - Gemini fallback when detected skills are below threshold

## Shared Utilities

### `preprocessing/semantic_utils.py`
- Shared semantic encoder wrapper.
- Centralizes model loading and cosine similarity for embedding-based modules.

### `preprocessing/gemini_key_selector.py`
- Loads Gemini API keys from env and `.env`.
- Supports:
  - `GEMINI_API_KEYS`
  - `GEMINI_API_KEY_N`
  - `GEMINI_API_KEY`
- Uses current time (`time.time_ns()`) as random seed to choose a key index.

### `preprocessing/unified_preprocessor.py`
- Orchestrates Step 1 through Step 10 for one row.
- Returns target feature contract only.
- Includes optional config to enable/disable Gemini fallbacks for batch runs.

## Batch Runner

### `run_preprocessing_pipeline.py`
- Iterates over all CSV files in `data/raw/`.
- Applies unified preprocessing row by row.
- Writes outputs to `data/processed/` with same input filename.

## Target Output Features

The processed CSVs are written with this feature schema:
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

## Dependencies

Install from project root:

```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

Core dependencies used by this pipeline:
- `sentence-transformers`
- `spacy`
- `holidays`
- `rapidfuzz`
- `google-genai` (Gemini fallback)
- `requests`, `pydantic`

## How To Run Entire Pipeline

From project root:

```bash
python Job_pipeline/run_preprocessing_pipeline.py
```

Optional flags:

```bash
python Job_pipeline/run_preprocessing_pipeline.py --raw-dir Job_pipeline/data/raw --processed-dir Job_pipeline/data/processed
python Job_pipeline/run_preprocessing_pipeline.py --max-rows 100
python Job_pipeline/run_preprocessing_pipeline.py --enable-gemini-fallback
```

Notes:
- Default batch mode is non-blocking and does not force live Gemini calls.
- Use `--enable-gemini-fallback` if you want network-based fallback enrichment.

## Tests

Run all preprocessing tests:

```bash
python -m unittest discover -s Job_pipeline/tests -v
```

Coverage includes:
- Step-by-step module output tests
- End-to-end target feature contract test
