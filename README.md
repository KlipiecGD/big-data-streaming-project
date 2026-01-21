# Crypto Real-Time Streaming Analytics

This project implements a complete big data streaming pipeline that fetches real-time cryptocurrency data from the CoinGecko API, processes it using Apache Spark Structured Streaming, and sinks the results into Google BigQuery.

## Project Structure

* **Producer (`src/streaming/api_producer.py`)**: Continuously fetches market data for 250 coins and saves them as local JSON files.
* **Processor (`src/streaming/processor.py`)**: A Spark application running two parallel streaming queries to process incoming files.
* **Transformations (`src/streaming/transformations.py`)**: Contains business logic for volatility calculation, trend categorization, and 2-minute rolling averages.
* **Tests (`tests/`)**: Pytest suite for validating data transformations.

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
2. **Environment Variables**: Create a `.env` file in the project root:
```env
GOOGLE_APPLICATION_CREDENTIALS="/absolute/path/to/your/credentials.json"
```


3. **Config YAML**: Update `src/config/config.yaml` with your specific GCS bucket name and BigQuery dataset/table names.

---

## Running the Project

To run the full pipeline, you need to execute the producer and processor in separate terminal windows.

### Step 1: Start the API Producer

The producer must run first to create the initial data files for Spark to monitor.

```bash
python -m src.streaming.api_producer
```

It will fetch data every 60 seconds and save it to the `data/` directory.

### Step 2: Start the Spark Processor

In a new terminal window, start the streaming engine:

```bash
python -m src.streaming.processor
```

This script initializes a Spark session with necessary BigQuery and GCS connectors and starts two pipelines:

1. **Main Pipeline**: Detailed coin analytics saved to your main BigQuery table.
2. **Rolling Average Pipeline**: Calculates averages over a 2-minute window and saves them to a separate table.

### Step 3: Run Unit Tests

To verify that the transformations are calculating correctly:

```bash
pytest
```

The test suite validates logic for filtering null prices, calculating volatility, and ensuring no crashes occur during division-by-zero scenarios.

---

## Maintenance and Configuration

### Cleaning Folders

* **Data Folder**: If you want to ignore old market data, clear the `data/` directory.
* **Checkpoints**: If you modify the transformation logic or schema in `transformations.py`, you **must** delete the `checkpoints/` directory before restarting the processor to avoid state incompatibility errors.

### Configuration Changes
In `src/config/config.yaml`, you can adjust:
* GCS bucket name
* BigQuery dataset and table names
* File paths for data and checkpoints
* Streaming intervals
* CoinGecko API parameters (e.g., coins to track, number of coins)
* Other relevant settings
