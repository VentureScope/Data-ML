# Time Series Role Forecasting

This folder contains split notebooks and generated artifacts for per-role job demand forecasting.

## Notebooks
- `prophet_pipeline.ipynb`: Full Prophet pipeline (load, preprocessing, EDA, feature engineering, CV, evaluation, forecasting, artifact saving).
- `arima_pipeline.ipynb`: Full ARIMA pipeline (load, preprocessing, EDA, feature engineering, CV with order search, evaluation, forecasting, artifact saving).
- `ts.ipynb`: Legacy combined notebook.

## Comparison Workflow
1. Run `prophet_pipeline.ipynb`.
2. Run `arima_pipeline.ipynb`.
3. Each notebook will read the other model's metric file (if available) and print side-by-side comparison by mean CV RMSE.

## Final Prediction Output
- Prophet notebook prints and stores the top predicted role (highest 6-month forecast sum) for Prophet.
- ARIMA notebook prints and stores the top predicted role (highest 6-month forecast sum) for ARIMA.

## Artifact Structure
- `artifacts/models/prophet/`: Saved Prophet models (`prophet_<role>.pkl`).
- `artifacts/models/arima/`: Saved ARIMA models (`arima_<role>.pkl`).
- `artifacts/forecasts/`: Forecast exports for Prophet and ARIMA.
- `artifacts/metrics/`: Cross-validation metrics and summary JSON files.
- `artifacts/modeling_dataset_month_role_job_count.csv`: Modeling input table in required format (`month | role | job_count`).

## Input Dataset
Both notebooks read:
- `Job_pipeline/data/processed/afriwork_all_jobs_20260310_124628.csv`

The notebooks auto-detect the project root by searching for the `Job_pipeline` directory, so they are resilient to working-directory changes.
