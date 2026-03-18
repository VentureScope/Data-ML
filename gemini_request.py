from gemini_role_classifier import classify_csv


# Keys are read from environment/.env by classify_csv().


if __name__ == "__main__":
    # Writes: unclean_jobs_labeled.csv
    classify_csv(
        "hahu_tech_jobs_20260310_124735.csv",
        batch_size=30,
        sleep=0.0,
    )