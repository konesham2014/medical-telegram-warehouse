"""
TASK 5: Dagster Pipeline Orchestration
Automates the entire ELT pipeline with scheduling and monitoring.

File: scripts/pipeline.py
"""
import os
import subprocess
from datetime import datetime

from dagster import (
    job, op, graph, schedule, Definitions,
    RunRequest, ScheduleEvaluationContext,
    Failure, RetryPolicy, Backoff
)
from dagster import get_dagster_logger

logger = get_dagster_logger()


# ============================================================
# OPS (Individual Pipeline Steps)
# ============================================================

@op(
    description="Scrape Telegram channels for medical product data",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=60,  # 1 minute initial delay
        backoff=Backoff.EXPONENTIAL
    )
)
def scrape_telegram_data():
    """
    Step 1: Run the Telegram scraper to extract messages and images.
    Handles rate limiting with exponential backoff retries.
    """
    logger.info("Starting Telegram data scraping...")

    result = subprocess.run(
        ["python", "src/scraper.py"],
        capture_output=True,
        text=True,
        cwd="/app"  # Adjust based on your Docker setup
    )

    if result.returncode != 0:
        logger.error(f"Scraper failed: {result.stderr}")
        raise Failure(f"Telegram scraper failed: {result.stderr}")

    logger.info("Telegram scraping completed successfully")
    return "scraping_complete"


@op(
    description="Load raw JSON data into PostgreSQL raw schema",
    retry_policy=RetryPolicy(max_retries=2, delay=30)
)
def load_raw_to_postgres(scraping_status: str):
    """
    Step 2: Load scraped JSON files from data lake into PostgreSQL.
    """
    logger.info("Loading raw data to PostgreSQL...")

    result = subprocess.run(
        ["python", "src/load_raw_to_postgres.py"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        logger.error(f"Data loading failed: {result.stderr}")
        raise Failure(f"Raw data loading failed: {result.stderr}")

    logger.info("Raw data loaded to PostgreSQL successfully")
    return "raw_loaded"


@op(
    description="Run dbt transformations (staging and mart models)",
    retry_policy=RetryPolicy(max_retries=2, delay=30)
)
def run_dbt_transformations(raw_status: str):
    """
    Step 3: Execute dbt models to transform raw data into star schema.
    Runs staging models, then mart models, then tests.
    """
    logger.info("Running dbt transformations...")

    # Run dbt models
    result = subprocess.run(
        ["dbt", "run", "--project-dir", "medical_warehouse"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        logger.error(f"dbt run failed: {result.stderr}")
        raise Failure(f"dbt run failed: {result.stderr}")

    # Run dbt tests
    test_result = subprocess.run(
        ["dbt", "test", "--project-dir", "medical_warehouse"],
        capture_output=True,
        text=True
    )

    if test_result.returncode != 0:
        logger.error(f"dbt tests failed: {test_result.stderr}")
        raise Failure(f"dbt tests failed: {test_result.stderr}")

    logger.info("dbt transformations and tests completed successfully")
    return "dbt_complete"


@op(
    description="Run YOLOv8 object detection on downloaded images",
    retry_policy=RetryPolicy(max_retries=2, delay=30)
)
def run_yolo_enrichment(dbt_status: str):
    """
    Step 4: Enrich data with YOLO object detection results.
    """
    logger.info("Running YOLO object detection...")

    result = subprocess.run(
        ["python", "src/yolo_detect.py"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        logger.error(f"YOLO detection failed: {result.stderr}")
        raise Failure(f"YOLO detection failed: {result.stderr}")

    logger.info("YOLO enrichment completed successfully")
    return "yolo_complete"


@op(
    description="Run dbt for image detection mart models"
)
def run_dbt_image_marts(yolo_status: str):
    """
    Step 5: Run dbt models for image detection facts.
    """
    logger.info("Running dbt image detection mart models...")

    result = subprocess.run(
        ["dbt", "run", "--select", "fct_image_detections", "--project-dir", "medical_warehouse"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        logger.error(f"dbt image marts failed: {result.stderr}")
        raise Failure(f"dbt image marts failed: {result.stderr}")

    logger.info("Image detection mart models completed")
    return "pipeline_complete"


# ============================================================
# JOB DEFINITION
# ============================================================

@job(
    description="Complete Medical Telegram Data Pipeline",
    tags={"pipeline": "medical_telegram_warehouse"}
)
def medical_telegram_pipeline():
    """
    Complete ELT pipeline job:
    1. Scrape Telegram data
    2. Load raw data to PostgreSQL
    3. Run dbt transformations
    4. Run YOLO enrichment
    5. Run dbt image detection marts
    """
    scraping = scrape_telegram_data()
    raw_loaded = load_raw_to_postgres(scraping)
    dbt_done = run_dbt_transformations(raw_loaded)
    yolo_done = run_yolo_enrichment(dbt_done)
    run_dbt_image_marts(yolo_done)


# ============================================================
# SCHEDULE: Daily at 2:00 AM UTC
# ============================================================

@schedule(
    job=medical_telegram_pipeline,
    cron_schedule="0 2 * * *",  # Daily at 2:00 AM UTC
    execution_timezone="UTC",
    name="daily_medical_pipeline_schedule"
)
def daily_medical_pipeline_schedule(context: ScheduleEvaluationContext):
    """
    Daily schedule for the medical telegram pipeline.
    Runs at 2:00 AM UTC to process the previous day's data.
    """
    scheduled_time = context.scheduled_execution_time
    run_key = f"daily_run_{scheduled_time.strftime('%Y%m%d_%H%M%S')}"

    return RunRequest(
        run_key=run_key,
        run_config={},
        tags={"date": scheduled_time.strftime("%Y-%m-%d")}
    )


# ============================================================
# DEFINITIONS (Register assets with Dagster)
# ============================================================

defs = Definitions(
    jobs=[medical_telegram_pipeline],
    schedules=[daily_medical_pipeline_schedule]
)


# ============================================================
# Manual execution for testing
# ============================================================
if __name__ == "__main__":
    from dagster import execute_job
    from dagster import DagsterInstance

    instance = DagsterInstance.get()
    result = execute_job(
        recon_job=medical_telegram_pipeline,
        instance=instance
    )

    if result.success:
        print("Pipeline executed successfully!")
    else:
        print("Pipeline failed. Check Dagster logs for details.")
