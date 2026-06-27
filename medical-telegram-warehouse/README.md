# Medical Telegram Data Warehouse

An end-to-end data pipeline for Telegram, leveraging dbt for transformation, Dagster for orchestration, and YOLOv8 for data enrichment.

## Overview

This project builds a robust data platform that generates actionable insights about Ethiopian medical businesses using data scraped from public Telegram channels.

### Business Questions Answered

- What are the top 10 most frequently mentioned medical products or drugs across all channels?
- How does the price or availability of a specific product vary across different channels?
- Which channels have the most visual content (e.g., images of pills vs. creams)?
- What are the daily and weekly trends in posting volume for health-related topics?

## Architecture

```
Raw Data Lake (JSON + Images)
    |
    v
PostgreSQL Raw Schema
    |
    v
dbt Staging Models (Cleaning)
    |
    v
dbt Mart Models (Star Schema)
    |
    v
YOLO Enrichment + FastAPI
```

## Project Structure

```
medical-telegram-warehouse/
├── .vscode/           # VS Code settings
├── .github/           # GitHub Actions workflows
├── .env               # Environment variables (DO NOT COMMIT)
├── .gitignore         # Git ignore rules
├── docker-compose.yml # Container orchestration
├── Dockerfile         # Python environment
├── requirements.txt   # Python dependencies
├── README.md          # This file
├── data/              # Data lake
│   ├── raw/           # Raw scraped data
│   └── processed/     # Processed outputs
├── medical_warehouse/ # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/   # Staging models
│   │   └── marts/     # Dimensional models
│   └── tests/         # Custom tests
├── src/               # Source code
│   ├── scraper.py     # Telegram scraper
│   ├── load_raw_to_postgres.py
│   └── yolo_detect.py # Object detection
├── api/               # FastAPI application
│   ├── main.py        # API endpoints
│   ├── database.py    # DB connection
│   └── schemas.py     # Pydantic models
├── notebooks/         # Jupyter notebooks
├── tests/             # Unit tests
└── scripts/           # Utility scripts
```

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL 16+
- Docker & Docker Compose (optional)

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd medical-telegram-warehouse
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your credentials
```

### Telegram API Setup

1. Visit [my.telegram.org](https://my.telegram.org)
2. Register your application
3. Copy API_ID and API_HASH to your `.env` file

### Database Setup

Using Docker:
```bash
docker-compose up -d postgres
```

Or manually:
```bash
createdb medical_warehouse
```

### Running the Pipeline

#### Option 1: Manual Execution

1. Scrape Telegram data:
```bash
python src/scraper.py
```

2. Load to PostgreSQL:
```bash
python src/load_raw_to_postgres.py
```

3. Run dbt transformations:
```bash
cd medical_warehouse
dbt deps
dbt build
dbt docs generate
```

4. Run YOLO enrichment:
```bash
python src/yolo_detect.py
```

5. Start API:
```bash
uvicorn api.main:app --reload
```

#### Option 2: Docker Compose (Recommended)

```bash
docker-compose up -d
```

This starts:
- PostgreSQL on port 5432
- FastAPI on port 8000
- Dagster on port 3000

#### Option 3: Dagster Orchestration

```bash
dagster dev -f pipeline.py
```

Access Dagster UI at http://localhost:3000

## API Documentation

Once the API is running, access interactive documentation at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Health check |
| `/api/reports/top-products` | GET | Top mentioned products |
| `/api/channels/{name}/activity` | GET | Channel activity & trends |
| `/api/search/messages` | GET | Search messages by keyword |
| `/api/reports/visual-content` | GET | Visual content statistics |
| `/api/channels` | GET | List all channels |
| `/api/reports/daily-trends` | GET | Daily posting trends |

## Data Model

### Star Schema

**Dimension Tables:**
- `dim_channels` - Channel attributes and statistics
- `dim_dates` - Date dimension for time-based analysis

**Fact Tables:**
- `fct_messages` - Message metrics and foreign keys
- `fct_image_detections` - YOLO detection results

## Testing

Run dbt tests:
```bash
cd medical_warehouse
dbt test
```

Run Python tests:
```bash
pytest tests/
```

## Contributing

1. Create a feature branch
2. Make your changes
3. Run tests
4. Submit a pull request

## License

This project is for educational purposes at Kara Solutions.

## Contact

For questions or support, contact the Data Engineering team.
