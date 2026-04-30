# Job Pipeline

End-to-end preprocessing pipeline for job-post data, from raw JSON/CSV rows to structured feature CSVs.

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
    raw/                              # Input JSON/CSV files
      jobs_has_link.json              # Afriwork structured jobs
      no_link_jobs.json               # Telegram channel jobs
    processed/                        # Output CSVs (ML-ready)
  preprocessing/
    unified_preprocessor.py           # Orchestrator (Steps 0-10)
    tech_job_validation.py            # Step 0: Tech job filter
    clean_text.py                     # Step 1
    job_id.py                         # Step 2
    date_features.py                  # Step 3
    title_normalization.py            # Step 4
    description_embedding.py          # Step 5
    location_extraction.py            # Step 6
    remote_detection.py               # Step 7
    job_type_extraction.py            # Step 8
    education_extraction.py           # Step 9
    skills_extraction.py              # Step 10
    semantic_utils.py
    gemini_key_selector.py
  taxonomy/
    roles.json
    skills.json
  tests/
    test_step1_clean_text.py ... test_step10_skills_extraction.py
    test_pipeline_target_features.py
    test_json_source_compat.py        # Multi-source schema tests
    test_run_preprocessing_pipeline.py
    test_utils.py
  run_preprocessing_pipeline.py
```

## Data Directories

- `data/raw/`
  - Input JSON and CSV files to preprocess.
  - Supports multiple schemas: Afriwork structured JSON (`jobs_has_link.json`), Telegram semi-structured JSON (`no_link_jobs.json`), and legacy Afriwork/Hahu CSV exports.
- `data/processed/`
  - Output CSV files from the preprocessing run.
  - All outputs share the same 16-column schema regardless of input format.
  - Output files keep the same base filename as input files.

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
  - Structured hint resolution (`job_site="REMOTE"`, compound strings like `"On-site - Permanent"`)
  - Keyword scoring rules
- Output fields:
  - `is_remote`, `remote_mode`, `confidence_score`, `method_used`
- Fallback:
  - Gemini single-label fallback when no keyword evidence exists

### 8) `preprocessing/job_type_extraction.py`
- Purpose: Extract employment type.
- Technique:
  - Structured hint resolution (`job_type="FULL_TIME"`, compound strings like `"Remote - Contractual"`)
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
  - Structured hint resolution (`education_qualification="BACHELORS_DEGREE"`, `TVET`, `DIPLOMA`, etc.)
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
  - Union-merge with pre-parsed `skills` arrays from structured JSON sources
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
- Orchestrates Steps 0 through 10 for one row.
- Handles multi-source schema differences via field alias resolution.
- Prefers `raw_text` over truncated `description` when available.
- Passes structured hints (`job_type`, `job_site`, `skills`, `education_qualification`) to downstream modules.
- Filters non-job rows (polls, announcements) with all-null title/description.
- Returns target feature contract only.
- Includes optional config to enable/disable Gemini fallbacks for batch runs.

## Batch Runner

### `run_preprocessing_pipeline.py`
- Iterates over all JSON and CSV files in `data/raw/`.
- Applies unified preprocessing row by row.
- Writes outputs to `data/processed/` with same base filename (`.csv` extension).

## Target Output Features

The processed CSVs are written with this feature schema:
- `year_month`
- `timestamp`
- `month`
- `holiday_flag`
- `job_id`
- `company_name`
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

All output CSVs share this identical 16-column schema and can be safely concatenated.

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
- Multi-source JSON schema compatibility tests (field aliases, structured hints, `raw_text` fallback)
- Pipeline runner integration tests
