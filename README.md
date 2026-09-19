# OptiRide PySpark Pipeline for Bike Sharing Data

A PySpark-based ETL pipeline for processing bike-sharing network data, transforming it into warehouse-ready tables, and loading it into a relational database for downstream analysis.

This project demonstrates a practical end-to-end data engineering workflow covering extraction, transformation, loading, and data validation using Python, Spark, SQL, and cloud-ready deployment patterns.

## Overview

OptiRide collects and processes bike-sharing data from public station/network datasets and prepares it for analytical use. The project is structured around a multi-stage pipeline:

1. Extract bike-sharing data from source files or APIs
2. Transform raw records into cleaned, modeled datasets
3. Load the transformed data into SQL tables
4. Store the results in a structured, query-friendly format

The repository is organized to support both experimentation and deployment in a data engineering context.

## Project Architecture

```text
.
├── azure/
│   └── tables.sql              # SQL schema for network and station tables
├── data/                       # Source or processed datasets
├── pipeline/
│   ├── extract.py              # Data extraction logic
│   ├── transform.py            # Data cleaning and transformation logic
│   ├── load.py                 # Load operations into database destination
│   ├── main.py                 # Entry point for running the pipeline
│   ├── logger.py               # Logging utility
│   ├── Dockerfile              # Container configuration for pipeline execution
│   └── requirements.txt        # Python dependencies for pipeline execution
├── utils/
│   └── logger.py               # Shared logging helper
├── create_tables.sql           # SQL table creation script
├── demo.ipynb                 # Notebook-based demonstration
├── Dockerfile                  # Root-level container configuration
├── requirements.txt            # Project dependencies
├── dockerignore                # Docker ignore file
├── .gitignore                  # Git ignore file
├── LICENSE                     # MIT license
├── README.md                   # Project documentation
└── ...
```

## Key Components

### Pipeline stages

- `pipeline/extract.py` - handles data extraction from source systems
- `pipeline/transform.py` - performs validation, schema mapping, and transformations
- `pipeline/load.py` - inserts cleaned data into destination tables
- `pipeline/main.py` - orchestrates the full workflow

### Database model

The repository includes SQL definitions for a star-schema style warehouse structure:

- `dim_network` - bike-sharing network metadata
- `dim_station` - station-level attributes
- `fact_station_status` - real-time station status data

These tables are defined in `azure/tables.sql` and align with bike-sharing analytics use cases such as station availability, city comparisons, and network trends.

## Tech Stack

- Python
- PySpark
- SQL
- Pandas
- PostgreSQL / SQL Server-compatible schema patterns
- Docker
- Jupyter Notebook

## Prerequisites

Before running the project, ensure you have:

- Python 3.9+
- Java (required for Spark)
- pip
- A configured database destination (for example PostgreSQL or SQL Server)
- Docker (optional, for containerized execution)

## Installation

Clone the repository:

```bash
git clone https://github.com/kinematician30/OptiRide-PySpark-Pipeline4Bike-Sharing-Data.git
cd OptiRide-PySpark-Pipeline4Bike-Sharing-Data
```

Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

On Windows:

```bash
.venv\Scripts\activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

If you are using the pipeline directory dependencies separately:

```bash
pip install -r pipeline/requirements.txt
```

## Running the Pipeline

Run the full workflow from the pipeline entry point:

```bash
python pipeline/main.py
```

This executes the end-to-end process:

```text
Extract -> Transform -> Load
```

## Database Setup

The repository includes SQL scripts for table creation. You can initialize the schema using:

```bash
psql -f create_tables.sql
```

or use the Azure schema definitions in:

```bash
azure/tables.sql
```

## Notebook Demo

A notebook is included for exploratory analysis and demonstration:

```bash
jupyter notebook demo.ipynb
```

## Docker

The repository includes Docker configuration for containerized setup.

Build the image:

```bash
docker build -t optiride-pyspark .
```

Run it:

```bash
docker run -it --rm optiride-pyspark
```

## Data Flow Example

A typical data workflow in this project looks like this:

```text
Source bike-share data
        ↓
Extract and validate data
        ↓
Clean and enrich fields
        ↓
Transform into analytics-ready tables
        ↓
Load into SQL destination
```

## Use Cases

This project is useful for:

- analyzing station availability and occupancy
- comparing urban bike-sharing networks
- tracking real-time and historical station patterns
- building ETL workflows using PySpark and SQL
- experimenting with cloud-based data engineering pipelines

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contributing

Contributions are welcome. If you want to improve the project:

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Open a pull request

## Contact

For project questions or collaboration opportunities, use the repository's GitHub contact and discussion options.

---

This project is a clean example of a PySpark data pipeline for bike-sharing analytics and can be extended with additional ingestion sources, orchestration tools, and data quality checks.
