"""
Louisville Weather and Business Risk Monitor
ETL Pipeline — Week 3: Transformation & Data Quality
Developer: Oluwatosin Adelusi

This file follows the structure of the Week 3 teaching example and extends it
with a Supabase PostgreSQL database load for the Louisville Business Risk dashboard.

Additions beyond the teaching example:
- weather_code.csv used in place of weather_codes.xlsx
- Supabase database loading (location, weather_observation, risk_metric,
  sector_daily_impact, sector_weather_sensitivity tables)
- Post-load row count verification

Required packages:
    pip install pandas sqlalchemy psycopg2-binary python-dotenv requests openpyxl

.env file must contain:
    user, password, host, port, dbname
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

# ---------------------------------------------------------------------------
# Logging and error handling:
# A reliable pipeline should leave a useful execution trail. Logging is better
# than print statements because logs can be filtered, timestamped, and captured
# by schedulers or orchestration tools.
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_pipeline.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Reproducibility:
# Keep source URLs, request parameters, and file locations in one predictable
# configuration area so a future user can rerun the same pipeline.
# ---------------------------------------------------------------------------
BASE_URL = "https://api.open-meteo.com/v1/forecast"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

LOOKUP_PATH        = DATA_DIR / "weather_code.csv"       # weather-code reference
LOCATION_PATH      = DATA_DIR / "location.csv"
SECTOR_PATH        = DATA_DIR / "sector.csv"
SENSITIVITY_PATH   = DATA_DIR / "sector_weather_sensitivity.csv"
DAILY_OUTPUT_PATH  = DATA_DIR / "daily_weather_forecast.csv"
WEEKLY_OUTPUT_PATH = DATA_DIR / "weekly_weather_summary.csv"

PARAMS = {
    "latitude": 38.2542,
    "longitude": -85.7594,
    "daily": [
        "weather_code",
        "temperature_2m_max",
        "temperature_2m_min",
        "sunrise",
        "sunset",
        "precipitation_sum",
        "precipitation_hours",
        "precipitation_probability_max",
        "daylight_duration",
        "sunshine_duration",
        "uv_index_max",
    ],
    "timezone": "America/New_York",
    "forecast_days": 16,
    "timeformat": "unixtime",
    "wind_speed_unit": "mph",
    "temperature_unit": "fahrenheit",
    "precipitation_unit": "inch",
}

# Per-sector business impact rules (applied during database load stage)
SECTOR_RULES = {
    "Utilities": lambda r: "moderate" if (r["hdd"] > 15 or r["cdd"] > 15) else "low",
    "Retail":    lambda r: "moderate" if r["heavy_rain_flag"] else "low",
    "Logistics": lambda r: "high"     if (r["high_wind_flag"] or r["heavy_rain_flag"]) else "low",
    "Tourism":   lambda r: "severe"   if r["severe_weather_flag"] else (
                           "moderate" if r["heavy_rain_flag"] else "low"),
    "Insurance": lambda r: "high"     if r["severe_weather_flag"] else "low",
}


# =============================================================================
# STEP 1 — EXTRACT
# =============================================================================
def extract_weather_forecast() -> dict:
    """Extract raw weather data from the API."""
    logger.info("Extracting weather forecast from Open-Meteo API")
    try:
        response = requests.get(BASE_URL, params=PARAMS, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as error:
        # Error handling:
        # Raise a clear failure with context instead of silently producing a
        # partial or empty dataset.
        logger.exception("Weather API request failed")
        raise RuntimeError("Unable to extract weather forecast data") from error


# =============================================================================
# STEP 2 — VALIDATE RAW RESPONSE
# =============================================================================
def validate_raw_response(raw_response: dict) -> None:
    """Validate that the API response contains the minimum structure we need."""
    # Validate required source fields before transformation. This catches source
    # contract changes early and gives a clear failure point.
    if not isinstance(raw_response, dict):
        raise ValueError("API response must be a dictionary")

    daily_data = raw_response.get("daily")
    if not daily_data:
        raise ValueError(f"No daily data returned. Raw response: {raw_response}")

    required_fields = {"time", "weather_code", "temperature_2m_max", "temperature_2m_min"}
    missing_fields = required_fields.difference(daily_data)
    if missing_fields:
        raise ValueError(f"Daily forecast is missing required fields: {sorted(missing_fields)}")

    logger.info("Raw response validation passed")


# =============================================================================
# STEP 3 — CLEAN & NORMALIZE
# =============================================================================
def clean_and_normalize_forecast(raw_response: dict) -> pd.DataFrame:
    """Clean, normalize, and standardize the raw API response."""
    daily_df = pd.DataFrame(raw_response["daily"])

    # Convert Unix timestamps into a real date column, use a stable date index,
    # and remove the raw timestamp column once it has served its purpose.
    daily_df["date"] = pd.to_datetime(daily_df["time"], unit="s").dt.date
    daily_df = daily_df.drop(columns=["time"]).set_index("date")

    # Convert sunrise and sunset from Unix seconds into readable local time.
    for column in ["sunrise", "sunset"]:
        if column in daily_df.columns:
            daily_df[column] = pd.to_datetime(daily_df[column], unit="s").dt.strftime("%H:%M")

    # Standardize column names. A predictable naming convention makes downstream
    # joins, BI tools, and tests easier to maintain.
    daily_df = daily_df.rename(columns={
        "temperature_2m_max":            "temp_max_f",
        "temperature_2m_min":            "temp_min_f",
        "precipitation_sum":             "precipitation_inches",
        "precipitation_probability_max": "precipitation_probability_pct",
        "daylight_duration":             "daylight_seconds",
        "sunshine_duration":             "sunshine_seconds",
    })

    # Enforce numeric types after extraction. errors="coerce" turns bad values
    # into NaN so validation can catch them instead of letting strings leak into
    # analytical calculations.
    numeric_columns = [
        "weather_code", "temp_max_f", "temp_min_f", "precipitation_inches",
        "precipitation_hours", "precipitation_probability_pct",
        "daylight_seconds", "sunshine_seconds", "uv_index_max",
    ]
    for column in numeric_columns:
        if column in daily_df.columns:
            daily_df[column] = pd.to_numeric(daily_df[column], errors="coerce")

    # Derived metrics: create fields that are easier for analysts to consume
    # than raw source values. These deterministic transformations belong in ETL.
    daily_df["temp_range_f"]  = daily_df["temp_max_f"] - daily_df["temp_min_f"]
    daily_df["avg_temp_f"]    = (daily_df["temp_max_f"] + daily_df["temp_min_f"]) / 2
    daily_df["daylight_hours"] = daily_df["daylight_seconds"] / 3600
    daily_df["sunshine_hours"] = daily_df["sunshine_seconds"] / 3600
    daily_df["sunshine_pct_of_daylight"] = (
        daily_df["sunshine_seconds"] / daily_df["daylight_seconds"] * 100
    ).round(1)
    daily_df["has_precipitation"] = daily_df["precipitation_inches"].fillna(0) > 0

    logger.info("Cleaned and normalized %s forecast rows", len(daily_df))
    return daily_df


# =============================================================================
# STEP 4 — LOAD LOOKUP & ENRICH
# =============================================================================
def load_weather_code_lookup(path: Path) -> pd.DataFrame:
    """
    Load and normalize the weather-code lookup table.
    Accepts the project's weather_code.csv (columns: weather_code_id, description).
    """
    logger.info("Loading weather-code lookup from %s", path)
    try:
        lookup_df = pd.read_csv(path)
    except FileNotFoundError as error:
        logger.exception("Weather-code lookup file was not found")
        raise FileNotFoundError(f"Missing lookup file: {path}") from error

    # Rename CSV columns to match the teaching example's Code / Description convention
    lookup_df = lookup_df.rename(columns={
        "weather_code_id": "Code",
        "description":     "Description",
    })

    # A lookup table must contain the keys and descriptive attributes required
    # for a trustworthy enrichment join.
    required_columns = {"Code", "Description"}
    missing_columns = required_columns.difference(lookup_df.columns)
    if missing_columns:
        raise ValueError(f"Weather-code lookup is missing columns: {sorted(missing_columns)}")

    lookup_df["Code"] = pd.to_numeric(lookup_df["Code"], errors="coerce")
    lookup_df = lookup_df.dropna(subset=["Code"])
    lookup_df["Code"] = lookup_df["Code"].astype(int)

    # Drop duplicate lookup keys so the enrichment join remains many-to-one.
    lookup_df = lookup_df.drop_duplicates(subset=["Code"])
    logger.info("Loaded %s normalized weather-code lookup rows", len(lookup_df))
    return lookup_df


def enrich_with_weather_descriptions(daily_df: pd.DataFrame, lookup_df: pd.DataFrame) -> pd.DataFrame:
    """Join normalized forecast data to normalized weather-code descriptions."""
    # Enrichment joins convert coded source data into business-readable data.
    enriched_df = (
        daily_df.reset_index()
        .merge(lookup_df[["Code", "Description"]],
               left_on="weather_code", right_on="Code", how="left")
        .drop(columns=["Code"], errors="ignore")
        .set_index("date")
    )

    # After a lookup join, check whether any source codes failed to match.
    missing_descriptions = enriched_df["Description"].isna().sum()
    if missing_descriptions:
        logger.warning("%s forecast rows did not match a weather-code description",
                       missing_descriptions)
    return enriched_df


# =============================================================================
# STEP 5 — VALIDATE CLEAN DATA
# =============================================================================
def validate_clean_forecast(daily_df: pd.DataFrame) -> None:
    """Run data quality checks after transformation and enrichment."""
    required_columns = {
        "weather_code", "temp_max_f", "temp_min_f", "avg_temp_f",
        "temp_range_f", "precipitation_inches", "Description",
    }
    missing_columns = required_columns.difference(daily_df.columns)
    if missing_columns:
        raise ValueError(f"Clean forecast is missing required columns: {sorted(missing_columns)}")

    # Required analytical fields should not be null after cleaning.
    required_non_null = ["weather_code", "temp_max_f", "temp_min_f", "avg_temp_f"]
    null_counts = daily_df[required_non_null].isna().sum()
    if null_counts.any():
        raise ValueError(f"Null values found in required fields: {null_counts.to_dict()}")

    # Check business rules, not only data types. A max temperature lower than a
    # min temperature indicates a transformation or source-quality problem.
    invalid_temperature_rows = daily_df[daily_df["temp_max_f"] < daily_df["temp_min_f"]]
    if not invalid_temperature_rows.empty:
        raise ValueError("Found rows where temp_max_f is lower than temp_min_f")

    # Percentages and physical measurements should stay in expected ranges.
    invalid_probability_rows = daily_df[
        ~daily_df["precipitation_probability_pct"].between(0, 100, inclusive="both")
    ]
    if not invalid_probability_rows.empty:
        raise ValueError("Found precipitation probability outside 0-100 percent")

    invalid_precipitation_rows = daily_df[daily_df["precipitation_inches"] < 0]
    if not invalid_precipitation_rows.empty:
        raise ValueError("Found negative precipitation values")

    duplicate_dates = daily_df.index[daily_df.index.duplicated()].unique()
    if len(duplicate_dates) > 0:
        raise ValueError(f"Duplicate date keys found: {list(duplicate_dates)}")

    logger.info("Clean forecast validation passed")


# =============================================================================
# STEP 6 — WEEKLY AGGREGATION
# =============================================================================
def build_weekly_aggregation(daily_df: pd.DataFrame) -> pd.DataFrame:
    """Create an aggregation layer for weekly reporting."""
    aggregation_df = daily_df.copy()
    aggregation_df.index = pd.to_datetime(aggregation_df.index)

    # A reporting layer summarizes row-level facts into business-friendly
    # metrics. Each week receives average temperatures, total precipitation,
    # and counts of rainy days.
    weekly_df = aggregation_df.resample("W").agg(
        avg_temp_f=                ("avg_temp_f",           "mean"),
        max_temp_f=                ("temp_max_f",           "max"),
        min_temp_f=                ("temp_min_f",           "min"),
        total_precipitation_inches=("precipitation_inches", "sum"),
        rainy_days=                ("has_precipitation",    "sum"),
        avg_uv_index=              ("uv_index_max",         "mean"),
    )
    weekly_df = weekly_df.round({
        "avg_temp_f": 1, "max_temp_f": 1, "min_temp_f": 1,
        "total_precipitation_inches": 2, "avg_uv_index": 1,
    })
    logger.info("Built weekly aggregation with %s rows", len(weekly_df))
    return weekly_df


# =============================================================================
# STEP 7 — INCREMENTAL LOADING (CSV outputs)
# =============================================================================
def incremental_upsert(new_df: pd.DataFrame, output_path: Path) -> pd.DataFrame:
    """Append new data and replace matching date keys from prior pipeline runs."""
    # Instead of overwriting blindly, read the existing target if it exists,
    # combine it with the new extract, and keep the latest version for each date.
    # This is a simple date-key upsert pattern.
    if output_path.exists():
        logger.info("Existing output found. Applying incremental upsert into %s", output_path)
        existing_df = pd.read_csv(output_path, parse_dates=["date"])
        existing_df["date"] = existing_df["date"].dt.date
        existing_df = existing_df.set_index("date")
        combined_df = pd.concat([existing_df, new_df])
        combined_df = combined_df[~combined_df.index.duplicated(keep="last")]
        combined_df = combined_df.sort_index()
    else:
        logger.info("No existing output found. Performing initial full load")
        combined_df = new_df.sort_index()
    return combined_df


def load_outputs(daily_df: pd.DataFrame, weekly_df: pd.DataFrame) -> None:
    """Load cleaned daily data and weekly aggregates to reproducible CSV outputs."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    final_daily_df = incremental_upsert(daily_df, DAILY_OUTPUT_PATH)

    # Write index=True because date is the natural key for this dataset.
    final_daily_df.to_csv(DAILY_OUTPUT_PATH, index=True)
    weekly_df.to_csv(WEEKLY_OUTPUT_PATH, index=True)
    logger.info("Saved daily forecast to %s", DAILY_OUTPUT_PATH)
    logger.info("Saved weekly summary to %s", WEEKLY_OUTPUT_PATH)


# =============================================================================
# STEP 8 — DATABASE LOADING (Supabase PostgreSQL)
# Incremental loading note for the database:
# The database uses a full reset rather than an incremental append because the
# Power BI dashboard requires a single coherent 16-day forecast window. Keeping
# old database rows alongside a revised API response would produce stale risk
# scores. The CSV output in Step 7 handles the incremental record; the database
# is always a clean, current snapshot.
# =============================================================================
def get_engine():
    """Connect to Supabase PostgreSQL using credentials from the .env file."""
    load_dotenv()
    fields = {k: os.getenv(k) for k in ["user", "password", "host", "port", "dbname"]}
    missing = [k for k, v in fields.items() if not v]
    if missing:
        raise RuntimeError(f"Missing .env variable(s): {missing}")

    url = (
        f"postgresql+psycopg2://{quote_plus(fields['user'])}:{quote_plus(fields['password'])}"
        f"@{fields['host']}:{fields['port']}/{fields['dbname']}?sslmode=require"
    )
    engine = create_engine(url)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("Database connection successful")
    return engine


def create_schema(engine) -> None:
    """Drop and recreate all dashboard tables in Supabase."""
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
                location_id INTEGER PRIMARY KEY, city TEXT NOT NULL, state TEXT NOT NULL,
                latitude NUMERIC(8,4) NOT NULL, longitude NUMERIC(8,4) NOT NULL,
                timezone TEXT NOT NULL, UNIQUE (city, state));
            CREATE TABLE public.weather_code (
                weather_code_id INTEGER PRIMARY KEY, description TEXT NOT NULL,
                severity_level TEXT NOT NULL DEFAULT 'normal');
            CREATE TABLE public.sector (
                sector_id INTEGER PRIMARY KEY,
                sector_name TEXT NOT NULL UNIQUE, description TEXT);
            CREATE TABLE public.weather_observation (
                observation_id BIGSERIAL PRIMARY KEY,
                location_id INTEGER NOT NULL REFERENCES public.location(location_id),
                weather_code_id INTEGER REFERENCES public.weather_code(weather_code_id),
                observation_date DATE NOT NULL, temp_max_f NUMERIC(5,2),
                temp_min_f NUMERIC(5,2), temp_mean_f NUMERIC(5,2),
                apparent_temp_f NUMERIC(5,2), precipitation_in NUMERIC(6,3),
                wind_speed_max_mph NUMERIC(5,2), humidity_pct NUMERIC(5,2),
                cloud_cover_pct NUMERIC(5,2), uv_index NUMERIC(4,2),
                sunrise TIME, sunset TIME,
                UNIQUE (location_id, observation_date));
            CREATE TABLE public.risk_metric (
                risk_id BIGSERIAL PRIMARY KEY,
                observation_id BIGINT NOT NULL UNIQUE
                    REFERENCES public.weather_observation(observation_id),
                hdd NUMERIC(6,2), cdd NUMERIC(6,2), precip_category VARCHAR(15),
                heavy_rain_flag BOOLEAN NOT NULL DEFAULT false,
                high_wind_flag BOOLEAN NOT NULL DEFAULT false,
                extreme_temp_flag BOOLEAN NOT NULL DEFAULT false,
                severe_weather_flag BOOLEAN NOT NULL DEFAULT false,
                weather_risk_score SMALLINT, risk_level VARCHAR(10));
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
                impact_level VARCHAR(10) NOT NULL, notes VARCHAR(255),
                UNIQUE (observation_id, sector_id));
        """))
    logger.info("Database schema created successfully")


def build_db_tables(daily_df: pd.DataFrame, sector_df: pd.DataFrame):
    """Build weather_observation, risk_metric, and sector_daily_impact DataFrames."""

    # --- weather_observation ---
    obs = daily_df.reset_index().rename(columns={
        "date":                  "observation_date",
        "weather_code":          "weather_code_id",
        "precipitation_inches":  "precipitation_in",
        "avg_temp_f":            "temp_mean_f",
    }).copy()
    obs["location_id"] = 1
    obs["observation_date"] = pd.to_datetime(obs["observation_date"]).dt.date
    obs["sunrise"] = pd.to_datetime(obs["sunrise"], format="%H:%M").dt.time
    obs["sunset"]  = pd.to_datetime(obs["sunset"],  format="%H:%M").dt.time
    obs.insert(0, "observation_id", range(1, len(obs) + 1))

    # --- risk_metric ---
    risk = obs[["observation_id", "temp_mean_f", "precipitation_in"]].copy()
    risk["hdd"] = (65 - risk["temp_mean_f"]).clip(lower=0).round(2)
    risk["cdd"] = (risk["temp_mean_f"] - 65).clip(lower=0).round(2)
    risk["precip_category"]    = risk["precipitation_in"].apply(_classify_precip)
    risk["heavy_rain_flag"]    = risk["precipitation_in"] > 1.0
    risk["high_wind_flag"]     = daily_df["wind_speed_10m_max"].values > 25.0 \
                                 if "wind_speed_10m_max" in daily_df.columns \
                                 else False
    risk["extreme_temp_flag"]  = (risk["temp_mean_f"] < 32) | (risk["temp_mean_f"] > 95)
    risk["severe_weather_flag"] = risk["heavy_rain_flag"] | risk["high_wind_flag"] | risk["extreme_temp_flag"]
    score = (
        risk["heavy_rain_flag"].astype(int) * 30
        + risk["high_wind_flag"].astype(int) * 30
        + risk["extreme_temp_flag"].astype(int) * 25
        + (risk["precip_category"] == "Moderate Rain").astype(int) * 10
        + (risk["precipitation_in"] > 0).astype(int) * 10
    ).clip(upper=100)
    risk["weather_risk_score"] = score.astype(int)
    risk["risk_level"]         = risk["weather_risk_score"].apply(_banded_risk_level)
    risk = risk.drop(columns=["temp_mean_f", "precipitation_in"])

    # --- sector_daily_impact ---
    sid = dict(zip(sector_df["sector_name"], sector_df["sector_id"]))
    impact_rows = [
        {"observation_id": int(r["observation_id"]), "sector_id": int(sid[name]),
         "impact_level": rule(r), "notes": None}
        for _, r in risk.iterrows()
        for name, rule in SECTOR_RULES.items()
    ]
    impact = pd.DataFrame(impact_rows)

    logger.info("Built observation(%d), risk(%d), impact(%d) rows",
                len(obs), len(risk), len(impact))
    return obs, risk, impact


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


def load_db_table(df: pd.DataFrame, name: str, engine, dtype: dict) -> None:
    """Load a single DataFrame into a Supabase table."""
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
                  {"weather_code_id": Integer(), "description": String(), "severity_level": String()})
    load_db_table(sector_df, "sector", engine,
                  {"sector_id": Integer(), "sector_name": String(), "description": String()})
    load_db_table(obs, "weather_observation", engine,
                  {"observation_id": Integer(), "location_id": Integer(), "weather_code_id": Integer(),
                   "observation_date": Date(), "temp_max_f": Float(), "temp_min_f": Float(),
                   "temp_mean_f": Float(), "apparent_temp_f": Float(), "precipitation_in": Float(),
                   "wind_speed_max_mph": Float(), "humidity_pct": Float(), "cloud_cover_pct": Float(),
                   "uv_index": Float(), "sunrise": Time(), "sunset": Time()})
    load_db_table(risk, "risk_metric", engine,
                  {"observation_id": Integer(), "hdd": Float(), "cdd": Float(),
                   "precip_category": String(), "heavy_rain_flag": Boolean(),
                   "high_wind_flag": Boolean(), "extreme_temp_flag": Boolean(),
                   "severe_weather_flag": Boolean(), "weather_risk_score": SmallInteger(),
                   "risk_level": String()})
    load_db_table(sensitivity_df, "sector_weather_sensitivity", engine,
                  {"sensitivity_id": Integer(), "sector_id": Integer(), "weather_variable": String(),
                   "business_signal": String(), "example_entities": String()})
    load_db_table(impact, "sector_daily_impact", engine,
                  {"observation_id": Integer(), "sector_id": Integer(),
                   "impact_level": String(), "notes": String()})


def verify_row_counts(engine, expected: dict) -> None:
    """Row count verification — confirm each table matches what was loaded."""
    logger.info("Running post-load row count verification...")
    with engine.connect() as conn:
        for table, exp in expected.items():
            actual = conn.execute(text(f"SELECT COUNT(*) FROM public.{table}")).scalar()
            status = "PASS" if actual == exp else "MISMATCH"
            logger.info("  %-35s expected=%-4d  actual=%-4d  [%s]", table, exp, actual, status)


# =============================================================================
# MAIN — full extract, transform, validate, and load process
# =============================================================================
def main() -> None:
    try:
        # --- Steps 1-2: Extract and validate raw response ---
        raw_response = extract_weather_forecast()
        validate_raw_response(raw_response)

        # --- Step 3: Clean and normalize ---
        daily_df = clean_and_normalize_forecast(raw_response)

        # --- Step 4: Load lookup and enrich ---
        weather_codes_df = load_weather_code_lookup(LOOKUP_PATH)
        daily_df = enrich_with_weather_descriptions(daily_df, weather_codes_df)

        # --- Step 5: Validate clean data ---
        validate_clean_forecast(daily_df)

        # --- Step 6: Weekly aggregation ---
        weekly_df = build_weekly_aggregation(daily_df)

        # --- Step 7: Save CSV outputs (incremental upsert) ---
        load_outputs(daily_df, weekly_df)

        logger.info("ETL pipeline (CSV stages) completed successfully")
        logger.info("Sample enriched forecast rows:\n%s", daily_df.head().to_string())
        logger.info("Sample weekly aggregation rows:\n%s", weekly_df.head().to_string())

    except Exception:
        # Log the full traceback, then re-raise so automation tools correctly
        # mark the pipeline run as failed.
        logger.exception("ETL pipeline failed during CSV stages")
        raise

    # --- Step 8: Load to Supabase database ---
    # Kept in a separate try block so the CSV outputs above are preserved
    # even if the database load fails.
    try:
        engine = get_engine()

        location_df    = pd.read_csv(LOCATION_PATH)
        sector_df      = pd.read_csv(SECTOR_PATH)
        sensitivity_df = pd.read_csv(SENSITIVITY_PATH)
        wc_db_df       = pd.read_csv(LOOKUP_PATH).rename(
            columns={"Code": "weather_code_id", "Description": "description"}
        ) if "Code" in pd.read_csv(LOOKUP_PATH).columns else pd.read_csv(LOOKUP_PATH)

        obs, risk, impact = build_db_tables(daily_df, sector_df)

        create_schema(engine)
        load_all_db_tables(engine, location_df, wc_db_df, sector_df,
                           obs, risk, sensitivity_df, impact)

        verify_row_counts(engine, {
            "location":                   len(location_df),
            "weather_code":               len(wc_db_df),
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