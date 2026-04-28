# Run the Hahu extraction script

This folder contains `script.py` which extracts `date`, `id` and up to five `url` values found under `button_links` from the JSON files in this folder and writes two CSVs into the processed data folder.

Outputs (written to this folder):

- Job_pipeline/data/raw/hahu/hahu_with_button_links.csv
- Job_pipeline/data/raw/hahu/hahu_no_button_links.csv

Prerequisites
- Python 3.8+ installed
- (optional) an activated virtual environment with dependencies available

Recommended (run from repository root)

1. Activate your virtual environment (example PowerShell):

```powershell
& .\venv\Scripts\Activate.ps1
```

2. Run the script from the project root (recommended):

```powershell
python Job_pipeline/data/raw/hahu/script.py
```

Or on macOS / Linux with a venv:

```bash
source venv/bin/activate
python Job_pipeline/data/raw/hahu/script.py
```

Notes
- The script expects the three JSON files to live in this directory: `telegram_scrape_*.json`.
-- The script will write the two CSV outputs into this folder (`Job_pipeline/data/raw/hahu/`).
-- The script currently uses a fixed input glob by default (all `*.json` in this folder); run it from the repository root so paths resolve correctly.
-- Re-run the same command to regenerate the CSV outputs.

If you want the script to accept a custom input glob from the command line, I can update it to add a simple CLI flag.
