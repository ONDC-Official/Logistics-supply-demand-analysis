# Logistics Supply-Demand Visualization

## Setup

### 1. Clone and Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Edit `.env` with your MongoDB connection string and other settings.

### 3. Prepare Data

Place your data files in the `datasets/` directory:
- `logistics_big_data.csv` - Main logistics data
- `pincode_simplified.geojson` - Pincode boundaries

### 4. Ingest Data (Run Once or Weekly)
```bash
python scripts/ingest_data.py
```

### 5. Run Application

**Development:**
```bash
python app.py
```

## API Endpoints

- `GET /` - Main visualization interface
- `POST /filter_hexagons` - Filter hexagons and supply points
