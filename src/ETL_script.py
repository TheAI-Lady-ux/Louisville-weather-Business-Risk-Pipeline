"""
Louisville Weather and Business Risk Monitor
ETL Pipeline — Week 3: Transformation & Data Quality
Developer: Oluwatosin Adelusi

This file follows the structure of the Week 3 teaching example and extends it
with a Supabase PostgreSQL database load for the Louisville Business Risk dashboard.

Pipeline stages:
  1. Extract   - Open-Meteo API (16-day rolling forecast) + CSV reference files
  2. Validate  - raw API response structure and field completeness
  3. Clean     - unix timestamps, column renaming, type coercion, derived metrics
  4. Enrich    - join weather-code descriptions from CSV lookup
  5. Validate  - clean data quality checks (nulls, ranges, business rules, duplicates)
  6. Aggregate - weekly summary layer for Power BI reporting
  7. Load CSV  - incremental date-key upsert to daily and weekly CSV outputs
  8. Load DB   - full schema reset + bulk insert to Supabase PostgreSQL
  9. Verify    - post-load row-count reconciliation

Incremental loading strategy:
  The CSV output (Step 7) uses a date-key upsert — existing dates are replaced
  with the freshest forecast values and new dates are appended. This prevents
  duplicate records while keeping the file current as the forecast window advances.

  The database (Step 8) uses a full reset because the Power BI dashboard requires
  a single coherent 16-day forecast window. Retaining old rows alongside a revised
  API response would produce stale risk scores. The CSV file handles the
  incremental history; the database is always a clean current snapshot.

Required packages:
    pip install pandas sqlalchemy psycopg2-binary python-dotenv requests

Database credentials (.env file — searched automatically from script folder upward):
    user=postgres.YOUR_PROJECT_REF
    password=YOUR_DB_PASSWORD
    host=aws-0-REGION.pooler.supabase.com
    port=5432
    dbname=postgres
"""

from pathlib import Path
import logging
import os
import sys
from urllib.parse import quote_plus

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.types import Boolean, Date, Float, Integer, SmallInteger, String, Time

# =============================================================================
# LOGGING
# Logging is preferred over print() — records are timestamped, filterable by
# severity, and capturable by schedulers or orchestration tools.
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_pipeline.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# All file paths, API settings, and business rules live here so the pipeline
# can be re-run without touching any function body.
#
# Folder structure:
#   Louisville_Weather_Business_Risk_Monitor/
#       src/
#           ETL_script.py   <-- this file
#       data/
#           weather_code.csv, location.csv, sector.csv,
#           sector_weather_sensitivity.csv
# =============================================================================
BASE_URL   = "https://api.open-meteo.com/v1/forecast"
SCRIPT_DIR = Path(__file__).resolve().parent        # .../src/
DATA_DIR   = SCRIPT_DIR.parent / "data"             # .../data/

LOOKUP_PATH        = DATA_DIR / "weather_code.csv"
LOCATION_PATH      = DATA_DIR / "location.csv"
SECTOR_PATH        = DATA_DIR / "sector.csv"
SENSITIVITY_PATH   = DATA_DIR / "sector_weather_sensitivity.csv"
DAILY_OUTPUT_PATH  = DATA_DIR / "daily_weather_forecast.csv"
WEEKLY_OUTPUT_PATH = DATA_DIR / "weekly_weather_summary.csv"

# API parameters — fields map directly to the weather_observation schema columns
PARAMS = {
    "latitude": 38.2542,
    "longitude": -85.7594,
    "daily": [
        "weather_code",           # → weather_code_id
        "temperature_2m_max",     # → temp_max_f
        "temperature_2m_min",     # → temp_min_f
        "apparent_temperature_max",  # → apparent_temp_f
        "precipitation_sum",      # → precipitation_in
        "wind_speed_10m_max",     # → wind_speed_max_mph
        "relative_humidity_2m_max",  # → humidity_pct
        "cloud_cover_mean",       # → cloud_cover_pct
        "uv_index_max",           # → uv_index
        "sunrise",                # → sunrise
        "sunset",                 # → sunset
    ],
    "timezone": "America/New_York",
    "forecast_days": 16,
    "timeformat": "unixtime",
    "temperature_unit": "fahrenheit",
    "wind_speed_unit": "mph",
    "precipitation_unit": "inch",
}

# Per-sector business impact rules applied during database load stage
SECTOR_RULES = {
    "Utilities": lambda r: "moderate" if (r["hdd"] > 15 or r["cdd"] > 15) else "low",
    "Retail":    lambda r: "moderate" if r["heavy_rain_flag"] else "low",
    "Logistics": lambda r: "high"     if (r["high_wind_flag"] or r["heavy_rain_flag"]) else "low",
    "Tourism":   lambda r: "severe"   if r["severe_weather_flag"] else (
                           "moderate" if r["heavy_rain_flag"] else "low"),
    "Insurance": lambda r: "high"     if r["severe_weather_flag"] else "low",
}


# =============================================================================
# STEP 1 — DATA EXTRACTION
# =============================================================================
def extract_weather_forecast() -> dict:
    """Fetch a 16-day daily weather forecast from the Open-Meteo API."""
    logger.info("Extracting weather forecast from Open-Meteo API")
    try:
        response = requests.get(BASE_URL, params=PARAMS, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as error:
        # Raise a clear failure with context instead of silently producing
        # a partial or empty dataset.
        logger.exception("Weather API request failed")
        raise RuntimeError("Unable to extract weather forecast data") from error


# =============================================================================
# STEP 2 — RAW RESPONSE VALIDATION
# =============================================================================
def validate_raw_response(raw_response: dict) -> None:
    """
    Validate that the API response contains the minimum structure we need.
    Catching source contract changes here gives a clear early failure point
    before any transformation has run.
    """
    if not isinstance(raw_response, dict):
        raise ValueError("API response must be a dictionary")

    daily_data = raw_response.get("daily")
    if not daily_data:
        raise ValueError(f"No daily data returned. Keys present: {list(raw_response.keys())}")

    required_fields = {"time", "weather_code", "temperature_2m_max", "temperature_2m_min"}
    missing_fields = required_fields.difference(daily_data)
    if missing_fields:
        raise ValueError(f"Daily forecast missing required fields: {sorted(missing_fields)}")

    logger.info("Raw response validation passed — %d forecast days received",
                len(daily_data["time"]))


# =============================================================================
# STEP 3 — CLEANING & NORMALIZATION
# =============================================================================
def clean_and_normalize_forecast(raw_response: dict) -> pd.DataFrame:
    """
    Convert the raw API payload into a clean DataFrame that matches the
    weather_observation schema.

    Cleaning steps:
    - Convert unix epoch timestamps to readable dates and HH:MM time strings
    - Rename API field names to schema column names
    - Enforce numeric types (errors='coerce' turns bad values into NaN)
    - Add derived metrics: avg temp, HDD, CDD, risk flags, risk score
    """
    daily_df = pd.DataFrame(raw_response["daily"])

    # Convert Unix timestamps to a real date column and use it as the index.
    # A stable date index makes downstream joins and incremental upserts reliable.
    daily_df["date"] = pd.to_datetime(daily_df["time"], unit="s").dt.date
    daily_df = daily_df.drop(columns=["time"]).set_index("date")

    # Convert sunrise and sunset from Unix seconds into readable HH:MM strings.
    for column in ["sunrise", "sunset"]:
        if column in daily_df.columns:
            daily_df[column] = pd.to_datetime(daily_df[column], unit="s").dt.strftime("%H:%M")

    # Standardize column names to match the weather_observation schema exactly.
    daily_df = daily_df.rename(columns={
        "temperature_2m_max":         "temp_max_f",
        "temperature_2m_min":         "temp_min_f",
        "apparent_temperature_max":   "apparent_temp_f",
        "precipitation_sum":          "precipitation_in",
        "wind_speed_10m_max":         "wind_speed_max_mph",
        "relative_humidity_2m_max":   "humidity_pct",
        "cloud_cover_mean":           "cloud_cover_pct",
        "uv_index_max":               "uv_index",
    })

    # Enforce numeric types. errors="coerce" turns non-numeric strings into NaN
    # so validation catches them rather than letting bad values reach the database.
    numeric_columns = [
        "weather_code", "temp_max_f", "temp_min_f", "apparent_temp_f",
        "precipitation_in", "wind_speed_max_mph", "humidity_pct",
        "cloud_cover_pct", "uv_index",
    ]
    for column in numeric_columns:
        if column in daily_df.columns:
            daily_df[column] = pd.to_numeric(daily_df[column], errors="coerce")

    # Derived metrics — deterministic transformations that belong in ETL.
    daily_df["temp_mean_f"]       = ((daily_df["temp_max_f"] + daily_df["temp_min_f"]) / 2).round(2)
    daily_df["temp_range_f"]      = (daily_df["temp_max_f"] - daily_df["temp_min_f"]).round(2)
    daily_df["hdd"]               = (65 - daily_df["temp_mean_f"]).clip(lower=0).round(2)
    daily_df["cdd"]               = (daily_df["temp_mean_f"] - 65).clip(lower=0).round(2)
    daily_df["heavy_rain_flag"]   = daily_df["precipitation_in"] > 1.0
    daily_df["high_wind_flag"]    = daily_df["wind_speed_max_mph"] > 25.0
    daily_df["extreme_temp_flag"] = (daily_df["temp_mean_f"] < 32) | (daily_df["temp_mean_f"] > 95)
    daily_df["severe_weather_flag"] = (
        daily_df["heavy_rain_flag"] | daily_df["high_wind_flag"] | daily_df["extreme_temp_flag"]
    )
    daily_df["precip_category"] = daily_df["precipitation_in"].apply(_classify_precip)
    score = (
        daily_df["heavy_rain_flag"].astype(int) * 30
        + daily_df["high_wind_flag"].astype(int) * 30
        + daily_df["extreme_temp_flag"].astype(int) * 25
        + (daily_df["precip_category"] == "Moderate Rain").astype(int) * 10
        + (daily_df["precipitation_in"] > 0).astype(int) * 10
    ).clip(upper=100)
    daily_df["weather_risk_score"] = score.astype(int)
    daily_df["risk_level"]         = daily_df["weather_risk_score"].apply(_banded_risk_level)
    daily_df["has_precipitation"]  = daily_df["precipitation_in"].fillna(0) > 0

    logger.info("Cleaned and normalized %s forecast rows", len(daily_df))
    return daily_df


def _classify_precip(inches):
    if inches <= 0:    return "Dry"
    if inches <= 0.25: return "Light Rain"
    if inches <= 1.0:  return "Moderate Rain"
    return "Heavy Rain"


def _banded_risk_level(score):
    if score < 20: return "calm"
    if score < 40: return "mild"
    if score < 60: return "elevated"
    if score < 80: return "high"
    return "severe"


# =============================================================================
# STEP 4 — ENRICHMENT (weather-code lookup join)
# =============================================================================
def load_weather_code_lookup(path: Path) -> pd.DataFrame:
    """Load and normalize the weather-code lookup from CSV."""
    logger.info("Loading weather-code lookup from %s", path)
    try:
        lookup_df = pd.read_csv(path)
    except FileNotFoundError as error:
        logger.exception("Weather-code lookup file was not found")
        raise FileNotFoundError(f"Missing lookup file: {path}") from error

    lookup_df = lookup_df.rename(columns={
        "weather_code_id": "Code",
        "description":     "Description",
    })

    required_columns = {"Code", "Description"}
    missing_columns = required_columns.difference(lookup_df.columns)
    if missing_columns:
        raise ValueError(f"Weather-code lookup missing columns: {sorted(missing_columns)}")

    lookup_df["Code"] = pd.to_numeric(lookup_df["Code"], errors="coerce")
    lookup_df = lookup_df.dropna(subset=["Code"])
    lookup_df["Code"] = lookup_df["Code"].astype(int)
    lookup_df = lookup_df.drop_duplicates(subset=["Code"])
    logger.info("Loaded %s normalized weather-code lookup rows", len(lookup_df))
    return lookup_df


def enrich_with_weather_descriptions(daily_df: pd.DataFrame,
                                      lookup_df: pd.DataFrame) -> pd.DataFrame:
    """Join weather-code descriptions onto the clean forecast DataFrame."""
    enriched_df = (
        daily_df.reset_index()
        .merge(lookup_df[["Code", "Description"]],
               left_on="weather_code", right_on="Code", how="left")
        .drop(columns=["Code"], errors="ignore")
        .set_index("date")
    )
    missing_descriptions = enriched_df["Description"].isna().sum()
    if missing_descriptions:
        logger.warning("%s forecast rows did not match a weather-code description",
                       missing_descriptions)
    return enriched_df


# =============================================================================
# STEP 5 — DATA VALIDATION & QUALITY CHECKS
# =============================================================================
def validate_clean_forecast(daily_df: pd.DataFrame) -> None:
    """
    Run data quality checks after transformation and enrichment.

    Checks performed:
    - Schema validation  : required columns must be present
    - Null value checks  : critical fields must not be null
    - Business rule      : temp_max must be >= temp_min
    - Range validation   : precipitation cannot be negative
    - Range validation   : temperature in realistic bounds
    - Duplicate detection: no duplicate date keys allowed
    """
    logger.info("Running data quality checks...")

    # 1. Schema / column validation
    required_columns = {
        "weather_code", "temp_max_f", "temp_min_f", "temp_mean_f",
        "precipitation_in", "wind_speed_max_mph", "Description",
    }
    missing_columns = required_columns.difference(daily_df.columns)
    if missing_columns:
        raise ValueError(f"Clean forecast missing required columns: {sorted(missing_columns)}")
    logger.info("  [PASS] Schema validation — all required columns present")

    # 2. Null value checks on critical analytical fields
    required_non_null = ["weather_code", "temp_max_f", "temp_min_f", "temp_mean_f"]
    null_counts = daily_df[required_non_null].isna().sum()
    if null_counts.any():
        raise ValueError(f"Null values found in required fields: {null_counts.to_dict()}")
    logger.info("  [PASS] Null check — no nulls in critical fields")

    # 3. Business rule: max temperature must be >= min temperature
    invalid_temp_rows = daily_df[daily_df["temp_max_f"] < daily_df["temp_min_f"]]
    if not invalid_temp_rows.empty:
        raise ValueError(f"temp_max_f < temp_min_f on {len(invalid_temp_rows)} row(s)")
    logger.info("  [PASS] Business rule — temp_max_f >= temp_min_f on all rows")

    # 4. Range validation: temperatures must be within realistic bounds
    for col in ["temp_max_f", "temp_min_f", "temp_mean_f"]:
        out = daily_df[(daily_df[col] < -60) | (daily_df[col] > 130)]
        if not out.empty:
            raise ValueError(f"{col}: {len(out)} value(s) outside realistic range [-60, 130]")
    logger.info("  [PASS] Range validation — all temperatures within realistic bounds")

    # 5. Range validation: precipitation cannot be negative
    neg_precip = daily_df[daily_df["precipitation_in"] < 0]
    if not neg_precip.empty:
        raise ValueError(f"Negative precipitation_in on {len(neg_precip)} row(s)")
    logger.info("  [PASS] Range validation — no negative precipitation values")

    # 6. Duplicate detection: each date must appear exactly once
    duplicate_dates = daily_df.index[daily_df.index.duplicated()].unique()
    if len(duplicate_dates) > 0:
        raise ValueError(f"Duplicate date keys found: {list(duplicate_dates)}")
    logger.info("  [PASS] Duplicate detection — no duplicate date keys")

    logger.info("All data quality checks passed")


# =============================================================================
# STEP 6 — AGGREGATION LAYER (weekly summary for Power BI)
# =============================================================================
def build_weekly_aggregation(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a weekly summary aggregation layer for reporting.

    A reporting layer summarizes row-level facts into business-friendly metrics.
    Each week receives average/max/min temperatures, total precipitation,
    count of rainy days, and average UV index — powering the weekly summary
    view in Power BI without requiring DAX aggregations.
    """
    aggregation_df = daily_df.copy()
    aggregation_df.index = pd.to_datetime(aggregation_df.index)

    weekly_df = aggregation_df.resample("W").agg(
        avg_temp_f=                ("temp_mean_f",       "mean"),
        max_temp_f=                ("temp_max_f",        "max"),
        min_temp_f=                ("temp_min_f",        "min"),
        total_precipitation_inches=("precipitation_in",  "sum"),
        rainy_days=                ("has_precipitation", "sum"),
        avg_uv_index=              ("uv_index",          "mean"),
        avg_wind_speed_mph=        ("wind_speed_max_mph","mean"),
    )
    weekly_df = weekly_df.round({
        "avg_temp_f": 1, "max_temp_f": 1, "min_temp_f": 1,
        "total_precipitation_inches": 2, "avg_uv_index": 1, "avg_wind_speed_mph": 1,
    })
    logger.info("Built weekly aggregation with %s rows", len(weekly_df))
    return weekly_df


# =============================================================================
# STEP 7 — INCREMENTAL LOADING (CSV outputs for Power BI)
# =============================================================================
def incremental_upsert(new_df: pd.DataFrame, output_path: Path) -> pd.DataFrame:
    """
    Date-key upsert: read the existing CSV if it exists, combine it with the
    new extract, keep the latest version for each date, and sort by date.

    This prevents duplicate loads while keeping a growing historical record
    that always holds the most current forecast for every date seen.
    """
    if output_path.exists():
        logger.info("Existing output found — applying incremental upsert into %s", output_path)
        existing_df = pd.read_csv(output_path)
        date_col = next(
            (c for c in existing_df.columns if "date" in c.lower()),
            existing_df.columns[0]
        )
        existing_df[date_col] = pd.to_datetime(existing_df[date_col]).dt.date
        existing_df = existing_df.rename(columns={date_col: "date"}).set_index("date")
        combined_df = pd.concat([existing_df, new_df])
        combined_df = combined_df[~combined_df.index.duplicated(keep="last")]
        combined_df = combined_df.sort_index()
    else:
        logger.info("No existing output found — performing initial full load")
        combined_df = new_df.sort_index()
    return combined_df


def load_outputs(daily_df: pd.DataFrame, weekly_df: pd.DataFrame) -> None:
    """Write the daily forecast and weekly summary to CSV for Power BI consumption."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    final_daily_df = incremental_upsert(daily_df, DAILY_OUTPUT_PATH)
    final_daily_df.to_csv(DAILY_OUTPUT_PATH, index=True)
    weekly_df.to_csv(WEEKLY_OUTPUT_PATH, index=True)
    logger.info("Saved daily forecast  → %s (%d rows)", DAILY_OUTPUT_PATH, len(final_daily_df))
    logger.info("Saved weekly summary  → %s (%d rows)", WEEKLY_OUTPUT_PATH, len(weekly_df))


# =============================================================================
# STEP 8 — DATABASE LOADING (Supabase PostgreSQL via SQLAlchemy)
# =============================================================================
def _find_and_load_dotenv() -> None:
    """Search for .env walking up from the script folder through all parents."""
    for folder in [Path(__file__).resolve().parent, *Path(__file__).resolve().parents]:
        env_file = folder / ".env"
        if env_file.exists():
            load_dotenv(env_file)
            logger.info("Loaded credentials from %s", env_file)
            return
    load_dotenv()


def get_engine():
    """Build and test a SQLAlchemy connection engine from .env credentials."""
    _find_and_load_dotenv()
    fields = {k: os.getenv(k) for k in ["user", "password", "host", "port", "dbname"]}
    missing = [k for k, v in fields.items() if not v]
    if missing:
        raise RuntimeError(
            f"Missing database credential(s): {missing}. "
            f"Ensure your .env file contains: user, password, host, port, dbname"
        )
    url = (
        f"postgresql+psycopg2://{quote_plus(fields['user'])}:{quote_plus(fields['password'])}"
        f"@{fields['host']}:{fields['port']}/{fields['dbname']}?sslmode=require"
    )
    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection successful")
        return engine
    except Exception as exc:
        raise RuntimeError(f"Database connection failed: {exc}") from exc


def create_schema(engine) -> None:
    """Drop and recreate all dashboard tables matching the project schema exactly."""
    logger.info("Resetting database schema...")
    with engine.begin() as conn:
        conn.execute(text("""
            DROP TABLE IF EXISTS public.sector_daily_impact CASCADE;
            DROP TABLE IF EXISTS public.sector_weather_sensitivity CASCADE;
            DROP TABLE IF EXISTS public.risk_metric CASCADE;
            DROP TABLE IF EXISTS public.weather_observation CASCADE;
            DROP TABLE IF EXISTS public.sector CASCADE;
            DROP TABLE IF EXISTS public.weather_code CASCADE;
            DROP TABLE IF EXISTS public.location CASCADE;
        """))
        conn.execute(text("""
            CREATE TABLE public.location (
                location_id INTEGER PRIMARY KEY,
                city TEXT NOT NULL, state TEXT NOT NULL,
                latitude NUMERIC(8,4) NOT NULL, longitude NUMERIC(8,4) NOT NULL,
                timezone TEXT NOT NULL, UNIQUE (city, state));

            CREATE TABLE public.weather_code (
                weather_code_id INTEGER PRIMARY KEY,
                description TEXT NOT NULL,
                severity_level TEXT NOT NULL DEFAULT 'normal');

            CREATE TABLE public.sector (
                sector_id INTEGER PRIMARY KEY,
                sector_name TEXT NOT NULL UNIQUE,
                description TEXT);

            CREATE TABLE public.weather_observation (
                observation_id BIGSERIAL PRIMARY KEY,
                location_id INTEGER NOT NULL REFERENCES public.location(location_id),
                weather_code_id INTEGER REFERENCES public.weather_code(weather_code_id),
                observation_date DATE NOT NULL,
                temp_max_f NUMERIC(5,2),
                temp_min_f NUMERIC(5,2),
                temp_mean_f NUMERIC(5,2),
                apparent_temp_f NUMERIC(5,2),
                precipitation_in NUMERIC(6,3),
                wind_speed_max_mph NUMERIC(5,2),
                humidity_pct NUMERIC(5,2),
                cloud_cover_pct NUMERIC(5,2),
                uv_index NUMERIC(4,2),
                sunrise TIME,
                sunset TIME,
                UNIQUE (location_id, observation_date));

            CREATE TABLE public.risk_metric (
                risk_id BIGSERIAL PRIMARY KEY,
                observation_id BIGINT NOT NULL UNIQUE
                    REFERENCES public.weather_observation(observation_id),
                hdd NUMERIC(6,2),
                cdd NUMERIC(6,2),
                precip_category VARCHAR(15),
                heavy_rain_flag BOOLEAN NOT NULL DEFAULT false,
                high_wind_flag BOOLEAN NOT NULL DEFAULT false,
                extreme_temp_flag BOOLEAN NOT NULL DEFAULT false,
                severe_weather_flag BOOLEAN NOT NULL DEFAULT false,
                weather_risk_score SMALLINT,
                risk_level VARCHAR(10));

            CREATE TABLE public.sector_weather_sensitivity (
                sensitivity_id INTEGER PRIMARY KEY,
                sector_id INTEGER NOT NULL REFERENCES public.sector(sector_id),
                weather_variable VARCHAR(60) NOT NULL,
                business_signal VARCHAR(255) NOT NULL,
                example_entities VARCHAR(255),
                UNIQUE (sector_id, weather_variable));

            CREATE TABLE public.sector_daily_impact (
                impact_id BIGSERIAL PRIMARY KEY,
                observation_id BIGINT NOT NULL
                    REFERENCES public.weather_observation(observation_id),
                sector_id INTEGER NOT NULL REFERENCES public.sector(sector_id),
                impact_level VARCHAR(10) NOT NULL,
                notes VARCHAR(255),
                UNIQUE (observation_id, sector_id));
        """))
    logger.info("Database schema created successfully")


def build_db_tables(daily_df: pd.DataFrame, sector_df: pd.DataFrame):
    """
    Build the three derived DB tables from the clean forecast.
    Only columns that exist in the project schema are included — no extras.
    """

    # --- weather_observation ---
    # Select only the exact columns that exist in the schema
    obs = daily_df.reset_index().rename(columns={
        "date":           "observation_date",
        "weather_code":   "weather_code_id",
    })[["observation_date", "weather_code_id", "temp_max_f", "temp_min_f",
        "temp_mean_f", "apparent_temp_f", "precipitation_in", "wind_speed_max_mph",
        "humidity_pct", "cloud_cover_pct", "uv_index", "sunrise", "sunset"]].copy()

    obs["location_id"]      = 1
    obs["observation_date"] = pd.to_datetime(obs["observation_date"]).dt.date
    obs["sunrise"]          = pd.to_datetime(obs["sunrise"], format="%H:%M").dt.time
    obs["sunset"]           = pd.to_datetime(obs["sunset"],  format="%H:%M").dt.time
    obs.insert(0, "observation_id", range(1, len(obs) + 1))

    # --- risk_metric ---
    # Use the risk columns already computed in clean_and_normalize_forecast
    risk = daily_df.reset_index()[["hdd", "cdd", "precip_category", "heavy_rain_flag",
                                    "high_wind_flag", "extreme_temp_flag",
                                    "severe_weather_flag", "weather_risk_score",
                                    "risk_level"]].copy()
    risk.insert(0, "observation_id", obs["observation_id"].values)

    # --- sector_daily_impact ---
    sid = dict(zip(sector_df["sector_name"], sector_df["sector_id"]))
    impact_rows = [
        {"observation_id": int(r["observation_id"]), "sector_id": int(sid[name]),
         "impact_level": rule(r), "notes": None}
        for _, r in risk.iterrows()
        for name, rule in SECTOR_RULES.items()
    ]
    impact = pd.DataFrame(impact_rows)

    logger.info("Built observation(%d rows), risk(%d rows), impact(%d rows)",
                len(obs), len(risk), len(impact))
    return obs, risk, impact


def load_db_table(df: pd.DataFrame, name: str, engine, dtype: dict) -> None:
    """Bulk-insert a DataFrame into a Supabase PostgreSQL table."""
    logger.info("Loading '%s' — %d rows...", name, len(df))
    df.to_sql(name, engine, schema="public", if_exists="append",
              index=False, method="multi", chunksize=500, dtype=dtype)
    logger.info("'%s' loaded successfully", name)


def load_all_db_tables(engine, location_df, weather_code_df, sector_df,
                        obs, risk, sensitivity_df, impact) -> None:
    """Insert all tables in foreign-key dependency order."""
    load_db_table(location_df, "location", engine,
                  {"location_id": Integer(), "city": String(), "state": String(),
                   "latitude": Float(), "longitude": Float(), "timezone": String()})

    load_db_table(weather_code_df, "weather_code", engine,
                  {"weather_code_id": Integer(), "description": String(),
                   "severity_level": String()})

    load_db_table(sector_df, "sector", engine,
                  {"sector_id": Integer(), "sector_name": String(), "description": String()})

    load_db_table(obs, "weather_observation", engine,
                  {"observation_id": Integer(), "location_id": Integer(),
                   "weather_code_id": Integer(), "observation_date": Date(),
                   "temp_max_f": Float(), "temp_min_f": Float(), "temp_mean_f": Float(),
                   "apparent_temp_f": Float(), "precipitation_in": Float(),
                   "wind_speed_max_mph": Float(), "humidity_pct": Float(),
                   "cloud_cover_pct": Float(), "uv_index": Float(),
                   "sunrise": Time(), "sunset": Time()})

    load_db_table(risk, "risk_metric", engine,
                  {"observation_id": Integer(), "hdd": Float(), "cdd": Float(),
                   "precip_category": String(), "heavy_rain_flag": Boolean(),
                   "high_wind_flag": Boolean(), "extreme_temp_flag": Boolean(),
                   "severe_weather_flag": Boolean(), "weather_risk_score": SmallInteger(),
                   "risk_level": String()})

    load_db_table(sensitivity_df, "sector_weather_sensitivity", engine,
                  {"sensitivity_id": Integer(), "sector_id": Integer(),
                   "weather_variable": String(), "business_signal": String(),
                   "example_entities": String()})

    load_db_table(impact, "sector_daily_impact", engine,
                  {"observation_id": Integer(), "sector_id": Integer(),
                   "impact_level": String(), "notes": String()})


# =============================================================================
# STEP 9 — POST-LOAD ROW COUNT VERIFICATION
# =============================================================================
def verify_row_counts(engine, expected: dict) -> None:
    """Query each table and confirm actual row counts match what was loaded."""
    logger.info("Running post-load row count verification...")
    with engine.connect() as conn:
        for table, exp in expected.items():
            actual = conn.execute(
                text(f"SELECT COUNT(*) FROM public.{table}")
            ).scalar()
            status = "PASS" if actual == exp else "MISMATCH"
            logger.info("  %-35s expected=%-4d  actual=%-4d  [%s]",
                        table, exp, actual, status)
    logger.info("Row count verification complete")


# =============================================================================
# MAIN — full extract, transform, validate, and load process
# =============================================================================
def main() -> None:
    try:
        # Steps 1-2: Extract from API and validate raw response
        raw_response = extract_weather_forecast()
        validate_raw_response(raw_response)

        # Step 3: Clean and normalize (produces schema-aligned columns + derived metrics)
        daily_df = clean_and_normalize_forecast(raw_response)

        # Step 4: Load weather-code lookup and enrich with descriptions
        weather_codes_df = load_weather_code_lookup(LOOKUP_PATH)
        daily_df = enrich_with_weather_descriptions(daily_df, weather_codes_df)

        # Step 5: Validate clean data
        validate_clean_forecast(daily_df)

        # Step 6: Build weekly aggregation layer for Power BI
        weekly_df = build_weekly_aggregation(daily_df)

        # Step 7: Save CSV outputs with incremental upsert
        load_outputs(daily_df, weekly_df)

        logger.info("ETL pipeline (CSV stages) completed successfully")
        logger.info("Sample enriched forecast rows:\n%s", daily_df.head().to_string())
        logger.info("Sample weekly aggregation rows:\n%s", weekly_df.head().to_string())

    except Exception:
        logger.exception("ETL pipeline failed during CSV stages")
        raise

    # Step 8: Load to Supabase — in a separate block so CSV outputs are
    # preserved even if the database load fails.
    try:
        engine = get_engine()

        location_df    = pd.read_csv(LOCATION_PATH)
        sector_df      = pd.read_csv(SECTOR_PATH)
        sensitivity_df = pd.read_csv(SENSITIVITY_PATH)
        weather_code_df = pd.read_csv(LOOKUP_PATH).rename(
            columns={"Code": "weather_code_id", "Description": "description"}
        ) if "Code" in pd.read_csv(LOOKUP_PATH).columns else pd.read_csv(LOOKUP_PATH)

        obs, risk, impact = build_db_tables(daily_df, sector_df)

        create_schema(engine)
        load_all_db_tables(engine, location_df, weather_code_df, sector_df,
                           obs, risk, sensitivity_df, impact)

        # Step 9: Verify row counts
        verify_row_counts(engine, {
            "location":                   len(location_df),
            "weather_code":               len(weather_code_df),
            "sector":                     len(sector_df),
            "weather_observation":        len(obs),
            "risk_metric":                len(risk),
            "sector_weather_sensitivity": len(sensitivity_df),
            "sector_daily_impact":        len(impact),
        })

        logger.info("ETL pipeline completed successfully — database loaded")

    except Exception:
        logger.exception("ETL pipeline failed during database stages")
        raise


if __name__ == "__main__":
    main()