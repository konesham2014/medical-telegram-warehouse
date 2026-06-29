
"""
Task 5: Dagster Pipeline Orchestration

Defines the complete ELT pipeline as a Dagster job with the following ops:
1. scrape_telegram_data - Extract messages from Telegram
2. load_raw_to_postgres - Load JSON to raw schema
3. run_dbt_transformations - Execute dbt models
4. run_yolo_enrichment - Run object detection on images

Usage:
    dagster dev -f pipeline.py
    Access UI at http://localhost:3000
"""

import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime

from dagster import (
    job, op, Out, In, Nothing,
    ScheduleDefinition,
    DefaultScheduleStatus,
    Definitions,
    get_dagster_logger,
    AssetMaterialization,
    MetadataValue
)
from dagster._utils import file_relative_path

# Logger
logger = get_dagster_logger()

# Base directory
BASE_DIR = Path(__file__).parent


# ============== OP 1: SCRAPE TELEGRAM DATA ==============

@op(
    out=Out(str, description="Path to scraped data directory"),
    tags={"stage": "extract"}
)
def scrape_telegram_data():
    """
    Op 1: Scrape messages and images from Telegram channels.

    Runs the Telethon-based scraper to extract data from configured
    Ethiopian medical Telegram channels and store in data lake.
    """
    logger.info("Starting Telegram data scraping...")

    scraper_script = BASE_DIR / "src" / "scraper.py"

    if not scraper_script.exists():
        logger.error(f"Scraper script not found: {scraper_script}")
        raise FileNotFoundError(f"Scraper script not found: {scraper_script}")

    try:
        result = subprocess.run(
            [sys.executable, str(scraper_script)],
            capture_output=True,
            text=True,
            cwd=str(BASE_DIR),
            timeout=3600  # 1 hour timeout
        )

        if result.returncode != 0:
            logger.error(f"Scraper failed: {result.stderr}")
            raise RuntimeError(f"Scraper failed: {result.stderr}")

        logger.info("Telegram scraping completed successfully")
        logger.info(result.stdout)

        # Yield materialization event
        yield AssetMaterialization(
            asset_key="telegram_raw_data",
            description="Raw Telegram messages scraped",
            metadata={
                "scraped_at": MetadataValue.text(datetime.now().isoformat()),
                "output_dir": MetadataValue.text(str(BASE_DIR / "data" / "raw"))
            }
        )

        yield str(BASE_DIR / "data" / "raw")

    except subprocess.TimeoutExpired:
        logger.error("Scraper timed out after 1 hour")
        raise RuntimeError("Scraper timed out")


# ============== OP 2: LOAD RAW TO POSTGRES ==============

@op(
    ins={"raw_data_path": In(str)},
    out=Out(str, description="Database connection status"),
    tags={"stage": "load"}
)
def load_raw_to_postgres(raw_data_path: str):
    """
    Op 2: Load raw JSON data into PostgreSQL raw schema.

    Reads JSON files from data lake and inserts them into
    raw.telegram_messages table.
    """
    logger.info("Loading raw data to PostgreSQL...")

    loader_script = BASE_DIR / "src" / "load_raw_to_postgres.py"

    if not loader_script.exists():
        logger.error(f"Loader script not found: {loader_script}")
        raise FileNotFoundError(f"Loader script not found: {loader_script}")

    try:
        result = subprocess.run(
            [sys.executable, str(loader_script)],
            capture_output=True,
            text=True,
            cwd=str(BASE_DIR),
            timeout=1800  # 30 minutes timeout
        )

        if result.returncode != 0:
            logger.error(f"Loader failed: {result.stderr}")
            raise RuntimeError(f"Loader failed: {result.stderr}")

        logger.info("Raw data loaded to PostgreSQL successfully")

        yield AssetMaterialization(
            asset_key="raw_postgres_data",
            description="Raw data loaded to PostgreSQL",
            metadata={
                "loaded_at": MetadataValue.text(datetime.now().isoformat()),
                "source_path": MetadataValue.text(raw_data_path)
            }
        )

        yield "success"

    except subprocess.TimeoutExpired:
        logger.error("Loader timed out after 30 minutes")
        raise RuntimeError("Loader timed out")


# ============== OP 3: RUN DBT TRANSFORMATIONS ==============

@op(
    ins={"load_status": In(str)},
    out=Out(str, description="dbt execution status"),
    tags={"stage": "transform"}
)
def run_dbt_transformations(load_status: str):
    """
    Op 3: Execute dbt models to transform raw data into star schema.

    Runs dbt build which includes:
    - Staging models (cleaning and standardization)
    - Mart models (dimensional star schema)
    - Tests (data quality validation)
    """
    logger.info("Running dbt transformations...")

    dbt_project_dir = BASE_DIR / "medical_warehouse"

    if not dbt_project_dir.exists():
        logger.error(f"dbt project not found: {dbt_project_dir}")
        raise FileNotFoundError(f"dbt project not found: {dbt_project_dir}")

    try:
        # Run dbt deps first
        deps_result = subprocess.run(
            ["dbt", "deps"],
            capture_output=True,
            text=True,
            cwd=str(dbt_project_dir),
            timeout=300
        )

        if deps_result.returncode != 0:
            logger.warning(f"dbt deps warning: {deps_result.stderr}")

        # Run dbt build (models + tests)
        build_result = subprocess.run(
            ["dbt", "build"],
            capture_output=True,
            text=True,
            cwd=str(dbt_project_dir),
            timeout=1800
        )

        if build_result.returncode != 0:
            logger.error(f"dbt build failed: {build_result.stderr}")
            raise RuntimeError(f"dbt build failed: {build_result.stderr}")

        logger.info("dbt transformations completed successfully")
        logger.info(build_result.stdout)

        # Generate docs
        docs_result = subprocess.run(
            ["dbt", "docs", "generate"],
            capture_output=True,
            text=True,
            cwd=str(dbt_project_dir),
            timeout=300
        )

        if docs_result.returncode == 0:
            logger.info("dbt documentation generated")

        yield AssetMaterialization(
            asset_key="dbt_transformed_data",
            description="Data transformed via dbt into star schema",
            metadata={
                "transformed_at": MetadataValue.text(datetime.now().isoformat()),
                "project_dir": MetadataValue.text(str(dbt_project_dir))
            }
        )

        yield "success"

    except subprocess.TimeoutExpired:
        logger.error("dbt timed out after 30 minutes")
        raise RuntimeError("dbt timed out")


# ============== OP 4: RUN YOLO ENRICHMENT ==============

@op(
    ins={"dbt_status": In(str)},
    out=Out(str, description="YOLO enrichment status"),
    tags={"stage": "enrich"}
)
def run_yolo_enrichment(dbt_status: str):
    """
    Op 4: Run YOLO object detection on downloaded images.

    Processes all images in data/raw/images/ and loads
    detection results into the data warehouse.
    """
    logger.info("Starting YOLO object detection enrichment...")

    yolo_script = BASE_DIR / "src" / "yolo_detect.py"

    if not yolo_script.exists():
        logger.error(f"YOLO script not found: {yolo_script}")
        raise FileNotFoundError(f"YOLO script not found: {yolo_script}")

    try:
        result = subprocess.run(
            [sys.executable, str(yolo_script)],
            capture_output=True,
            text=True,
            cwd=str(BASE_DIR),
            timeout=3600  # 1 hour timeout for image processing
        )

        if result.returncode != 0:
            logger.error(f"YOLO enrichment failed: {result.stderr}")
            raise RuntimeError(f"YOLO enrichment failed: {result.stderr}")

        logger.info("YOLO enrichment completed successfully")
        logger.info(result.stdout)

        yield AssetMaterialization(
            asset_key="yolo_enriched_data",
            description="Images analyzed with YOLO object detection",
            metadata={
                "processed_at": MetadataValue.text(datetime.now().isoformat()),
                "model": MetadataValue.text("yolov8n")
            }
        )

        yield "success"

    except subprocess.TimeoutExpired:
        logger.error("YOLO enrichment timed out after 1 hour")
        raise RuntimeError("YOLO enrichment timed out")


# ============== JOB DEFINITION ==============

@job(
    description="""
    Complete ELT pipeline for Medical Telegram Data Warehouse.

    Pipeline stages:
    1. Extract: Scrape Telegram channels for messages and images
    2. Load: Load raw JSON data into PostgreSQL
    3. Transform: Run dbt models to create star schema
    4. Enrich: Run YOLO object detection on images
    """,
    tags={"project": "medical-telegram-warehouse"}
)
def medical_telegram_pipeline():
    """
    Complete ELT pipeline job.

    Defines the dependency graph:
    scrape -> load -> dbt_transform -> yolo_enrich
    """
    raw_data = scrape_telegram_data()
    load_status = load_raw_to_postgres(raw_data)
    dbt_status = run_dbt_transformations(load_status)
    yolo_status = run_yolo_enrichment(dbt_status)


# ============== SCHEDULE ==============

# Daily schedule at 2:00 AM UTC
daily_schedule = ScheduleDefinition(
    job=medical_telegram_pipeline,
    cron_schedule="0 2 * * *",  # 2:00 AM daily
    name="daily_medical_pipeline",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Run the complete pipeline daily at 2:00 AM UTC"
)


# ============== DEFINITIONS ==============

defs = Definitions(
    jobs=[medical_telegram_pipeline],
    schedules=[daily_schedule]
)
