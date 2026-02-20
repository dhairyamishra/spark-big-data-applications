# NYC Urban Mobility Intelligence Platform

A full-stack big data application that processes **200M+ NYC taxi trips** through an Apache Spark pipeline and serves interactive analytics via a responsive React/Deck.gl dashboard.

![Spark](https://img.shields.io/badge/Apache_Spark-3.4-E25A1C?logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![React](https://img.shields.io/badge/React-18-61DAFB?logo=react&logoColor=black)
![Deck.gl](https://img.shields.io/badge/Deck.gl-9.0-000000)
![FastAPI](https://img.shields.io/badge/FastAPI-0.109-009688?logo=fastapi&logoColor=white)

---

## Architecture

```
┌───────────────────────────────────────────────────────┐
│              SPARK BATCH PIPELINE (PySpark)            │
│  01_ingest → 02_clean → 03_features → 04_analytics    │
│       → 05_ml_models → 06_streaming → 07_export       │
│                                                       │
│  HDFS Data Lake:  bronze/ → silver/ → gold/           │
└──────────────────────┬────────────────────────────────┘
                  JSON artifacts
                       ↓
┌───────────────────────────────────────────────────────┐
│           FASTAPI BACKEND (sub-ms latency)            │
│  Pre-computed JSON loaded into memory at startup      │
└──────────────────────┬────────────────────────────────┘
                    REST API
                       ↓
┌───────────────────────────────────────────────────────┐
│         REACT + DECK.GL FRONTEND (60fps maps)         │
│  Heatmap · Zone Choropleth · OD Flow Arcs             │
│  Demand Forecast · Anomaly Feed · Cluster Explorer    │
└───────────────────────────────────────────────────────┘
```

## Datasets

| Dataset | Records | Size | Source |
|---------|---------|------|--------|
| Yellow Taxi Trips (2023-2024) | ~140M | ~6 GB | [NYC TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| Green Taxi Trips (2023-2024) | ~3M | ~600 MB | NYC TLC |
| HVFHV Trips (2023-2024) | ~60M | ~30 GB | NYC TLC |
| Taxi Zone Lookup + Shapefile | 265 zones | ~500 KB | NYC TLC |

## Spark Features Demonstrated

| Feature | Pipeline Step |
|---------|--------------|
| Spark SQL (joins, pivots, cube) | `02_clean`, `04_analytics` |
| Window Functions (lag, rolling avg, z-score) | `03_feature_engineering` |
| Broadcast Joins | `01_ingest`, `02_clean` |
| MLlib Pipeline (VectorAssembler → Scaler → Model) | `05_ml_models` |
| GBTRegressor + CrossValidator | `05_ml_models` (demand prediction) |
| KMeans + ClusteringEvaluator | `05_ml_models` (zone clustering) |
| Statistical Anomaly Detection | `05_ml_models` (z-score method) |
| Structured Streaming + Watermarking | `06_streaming_sim` |
| Partitioned Parquet (year/month) | All pipeline steps |
| HDFS Medallion Architecture (bronze→silver→gold) | All pipeline steps |

## ML Models

### A. Demand Prediction (GBTRegressor)
- Predicts hourly trip count per zone
- Features: hour, day_of_week, month, lag_1h, lag_24h, lag_168h, rolling_7d_avg
- Trained on 2023, evaluated on 2024 with 3-fold cross-validation

### B. Zone Clustering (KMeans)
- Profiles zones by mobility pattern (airport hub, business district, residential, etc.)
- Features: demand volume, avg fare, distance, tip behavior, peak hour
- Optimal k selected via silhouette score

### C. Anomaly Detection (Statistical Z-Score)
- Detects zone-hours with abnormal demand
- Baseline computed from 2023 seasonal norms
- Flags events with severity levels (medium / high / critical)

---

## Quick Start (Demo Mode)

The app ships with a demo data generator — **no Spark or HDFS required** to see the dashboard.

### 1. Generate demo data
```bash
cd scripts
python generate_demo_data.py
```

### 2. Start the backend
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### 3. Start the frontend
```bash
cd frontend
npm install
npm run dev
```

Open **http://localhost:3000**

### Docker (one command)
```bash
# Generate demo data first
python scripts/generate_demo_data.py

# Then launch
docker-compose up --build
```

Open **http://localhost:3000**

---

## Full Pipeline (with Spark + HDFS)

### Prerequisites
- Apache Spark 3.4+
- Hadoop/HDFS (or the Docker setup from `setup-modern-spark.bat`)
- Python 3.9+

### 1. Download real data
```bash
# Download 2 years of data (~35 GB)
python scripts/download_data.py

# Or just 1 month for testing
python scripts/download_data.py --types yellow --years 2024 --months 1
```

### 2. Generate zone GeoJSON
```bash
python scripts/generate_geojson.py
```

### 3. Run the pipeline
```bash
cd pipeline

# Step 1: Ingest to HDFS
spark-submit 01_ingest.py

# Step 2: Clean and validate
spark-submit 02_clean.py

# Step 3: Feature engineering
spark-submit 03_feature_engineering.py

# Step 4: Analytics aggregations
spark-submit 04_analytics.py

# Step 5: Train ML models
spark-submit 05_ml_models.py

# Step 6: (Optional) Structured Streaming demo
spark-submit 06_streaming_sim.py

# Step 7: Export to JSON for web app
spark-submit 07_export.py
```

### 4. Start the web app
```bash
# Backend
cd backend && uvicorn main:app --port 8000

# Frontend
cd frontend && npm run dev
```

---

## Project Structure

```
├── pipeline/                    # PySpark batch pipeline
│   ├── config.py                # Centralized configuration
│   ├── utils.py                 # Shared helpers
│   ├── 01_ingest.py             # Raw → HDFS bronze
│   ├── 02_clean.py              # Bronze → silver (validation)
│   ├── 03_feature_engineering.py# Silver → enriched features
│   ├── 04_analytics.py          # Enriched → gold aggregations
│   ├── 05_ml_models.py          # GBT, KMeans, anomaly detection
│   ├── 06_streaming_sim.py      # Structured Streaming demo
│   └── 07_export.py             # Gold → JSON artifacts
├── backend/                     # FastAPI REST API
│   ├── main.py                  # App entry, CORS, data loading
│   └── routers/                 # Endpoint modules
├── frontend/                    # React + Deck.gl dashboard
│   └── src/
│       ├── components/          # MapView, Sidebar, Charts
│       ├── hooks/               # API data fetching
│       └── lib/                 # Constants, color scales
├── scripts/
│   ├── download_data.py         # TLC data downloader
│   ├── generate_geojson.py      # Shapefile → GeoJSON
│   └── generate_demo_data.py    # Demo data generator
├── docker-compose.yml           # One-command deployment
├── Dockerfile.backend           # FastAPI container
├── Dockerfile.frontend          # React → Nginx container
└── Dockerfile.pipeline          # Spark pipeline container
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/zones` | Zone GeoJSON + stats |
| `GET /api/zones/stats` | Zone-level statistics |
| `GET /api/zones/{id}` | Single zone detail |
| `GET /api/timeseries` | Daily demand time series |
| `GET /api/flows` | Top OD flows for arc map |
| `GET /api/predictions` | ML demand forecasts |
| `GET /api/anomalies` | Detected anomalies |
| `GET /api/clusters` | Zone cluster assignments |
| `GET /api/stats/overview` | Global summary |

## Tech Stack

- **Pipeline**: PySpark 3.4, HDFS, Parquet, MLlib
- **Backend**: FastAPI, uvicorn, orjson
- **Frontend**: React 18, Deck.gl 9, MapLibre GL, Recharts, TailwindCSS
- **Deployment**: Docker Compose, Nginx

## License

MIT
