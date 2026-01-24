# Crypto Real-Time Streaming Analytics

This project implements a complete big data streaming pipeline that fetches real-time cryptocurrency data from the CoinGecko API and stores it in Google Cloud Storage. Then it cleans and enriches the data using Apache Spark Structured Streaming and save the results back to Google Cloud Storage in parquet format. Finally, it aggregates the data and sinks the results into Google BigQuery for analysis.

## Project Structure

```
.
│
├── credentials/
│   └── google_cloud_credentials.json   # GCP service account key - not included in repo
│
├── src/
│   ├── cloud_utils/                    # Google Cloud Storage utilities
│   │   └── save_to_gcs.py              # Upload data to GCS
│   │
│   ├── config/                         # Configuration management
│   │   ├── config.py                   # Config loader
│   │   └── config.yaml                 # Project settings (GCS, BigQuery, etc.)
│   │
│   ├── logging_utils/                  # Logging configuration
│   │   └── logger.py                   # Custom logger setup
│   │
│   ├── schemas/                        # Data schemas
│   │   └── crypto_schema.py            # Spark schema definitions
│   │
│   └── streaming/                      # Streaming pipeline components
│       ├── bronze/                     # Bronze layer (raw data ingestion)
│       │   └── api_producer.py         # Fetch data from CoinGecko API
│       │
│       ├── gold/                       # Gold layer (aggregated analytics)
│       │   ├── processor.py            # Spark streaming processor
│       │   └── transformations.py      # Aggregation logic
│       │
│       └── silver/                     # Silver layer (cleaned & enriched)
│           ├── processor.py            # Spark streaming processor
│           └── transformations.py      # Data transformations
│
├── tests/
│   ├── data/
│   │   ├── bronze_data.py              # Sample bronze layer data for testing
│   │   └── silver_data.py              # Sample silver layer data for testing
│   ├── test_gold/
│   │   └── test_transformations.py     # Gold layer tests
│   └── test_silver/
│       └── test_transformations.py     # Silver layer tests
│
├── README.md                           # Project documentation
├── pytest.ini                          # Pytest configuration
└── requirements.txt                    # Python dependencies
```

---

## Local Environment Setup

### 1. Prerequisites

* **Python 3.13+**
* **Java 8, 11 or other compatible versions with Spark** 
* **Google Cloud Account**: A project with BigQuery and GCS enabled.

### 2. Installation

Clone the repository and set up a virtual environment:
```bash
git clone https://github.com/KlipiecGD/big-data-streaming-project.git
cd big-data-streaming-project
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
pip install -r requirements.txt
```

### 3. Configuration

1. **Google Cloud Credentials**: Place your service account JSON key in the project root (e.g., `credentials.json`).
2. Authenticate with Google Cloud:
```bash
gcloud auth login
```
3. **Environment Variables**: Create a `.env` file in the project root:
```env
GOOGLE_APPLICATION_CREDENTIALS="/absolute/path/to/your/credentials.json"
```


4. **Config YAML**: Update `src/config/config.yaml` with relevant names and paths. If you don't have a GCS bucket or BigQuery dataset, create one in your Google Cloud Console.

---

## Running the Project

To run the full pipeline, you need to execute the producer and processor in separate terminal windows.

### Step 1: Start the API Producer

The producer must run first to create the initial data files for Spark to monitor.

```bash
python -m src.streaming.bronze.api_producer
```

It will fetch data every 60 seconds and save it to the Google Cloud Storage bucket specified in the config.

### Step 2: Start the Silver Layer Spark Processor

In a new terminal window, start the streaming engine:

```bash
python -m src.streaming.silver.processor
```

This script initializes a Spark session with necessary configurations, reads the raw data from GCS, applies transformations, and writes the processed data to Google Cloud Storage in parquet format.

### Step 3: Start the Gold Layer Spark Processor

In a new terminal window, start the streaming engine:

```bash
python -m src.streaming.gold.processor
```
This script initializes a Spark session with necessary configurations, reads the processed data from GCS, applies aggregations, and writes the final results to Google BigQuery.

### Step 4: Run Unit Tests

To verify that the transformations are calculating correctly:

```bash
pytest
```

The test suite validates logic for filtering null prices, calculating volatility, and ensuring no crashes occur during division-by-zero scenarios.

---

## Maintenance and Configuration

### Cleaning Folders

* **Data Folder**: If you want to ignore old market data, delete the contents of the GCS data folder specified in your config.
* **Checkpoints**: If you modify the transformation logic or schema and want to reset the streaming state, delete the checkpoint folders in GCS.

