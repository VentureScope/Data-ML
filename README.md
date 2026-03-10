# venterscope-data-scraper

Jupyter notebook that scrapes job listings from African job boards and saves the results as CSV files. Designed to run on **Google Colab** with no extra dependencies.

## Sources

| Source | Endpoint | Auth |
|--------|----------|------|
| [Afri-work](https://afriworket.com) – all jobs | `https://api.afriworket.com/v1/graphql` | None (anonymous) |
| [Afri-work](https://afriworket.com) – software/tech jobs | `https://api.afriworket.com/v1/graphql` | None (optional Bearer token) |
| [Hahu Jobs](https://www.hahu.jobs) – tech sector | `https://graph.aggregator.hahu.jobs/v1/graphql` | None |

## Quick start (Google Colab)

1. Open [Google Colab](https://colab.research.google.com/) and upload **`afriwork_hahu_scraper.ipynb`**, or use *File → Open notebook → GitHub* and paste this repo URL.
2. Run all cells in order (`Runtime → Run all`).
3. When prompted, allow Google Drive access so the CSV files are saved to `My Drive/job_data/`.

> **No Drive?** Set `USE_GOOGLE_DRIVE = False` in Cell 2 – files will be saved to `/content/job_data/` inside the Colab session instead.

## Output files

Each run produces three timestamped CSV files:

| File | Contents |
|------|----------|
| `afriwork_all_jobs_YYYYMMDD_HHMMSS.csv` | All active Afri-work listings |
| `afriwork_tech_jobs_YYYYMMDD_HHMMSS.csv` | Afri-work listings filtered to *Software design and Development* |
| `hahu_tech_jobs_YYYYMMDD_HHMMSS.csv` | Hahu Jobs listings in the IT/Tech sector |

## Optional: authenticated Afri-work requests

The software-jobs query works anonymously by default. If you have a personal
Afri-work account you can paste your Bearer token into the `AFRIWORK_BEARER_TOKEN`
variable in Cell 5 to use the `job_seeker` role.

## Running locally

```bash
pip install requests pandas notebook
jupyter notebook afriwork_hahu_scraper.ipynb
```

Set `USE_GOOGLE_DRIVE = False` when running outside Colab.