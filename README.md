# Louisville Weather and Business Risk Monitor

**Developer:** Oluwatosin Adelusi

A data pipeline and warehouse that translates daily weather forecasts for Louisville, Kentucky into business-relevant risk signals across five local industry sectors, powering a Power BI dashboard.

---

## Project Overview

The system pulls a 16-day rolling weather forecast from the [Open-Meteo API](https://api.open-meteo.com/v1/forecast), applies a decision-engine to compute risk metrics (Heating/Cooling Degree Days, severe-weather flags, and a composite risk score), and loads the results into a Supabase PostgreSQL database. A parallel CSV output supports incremental Power BI reporting.

### Monitored Sectors
- **Utilities** — impact driven by heating and cooling demand (HDD/CDD)
- **Retail** — impact driven by heavy rain events
- **Logistics** — impact driven by high wind or heavy rain
- **Tourism** — impact driven by severe weather and heavy rain
- **Insurance** — impact driven by severe weather composite flag

---

## Repository Structure

```
Louisville_Weather_Business_Risk_Monitor/
├── src/
│   └── ETL_script.py           # Full ETL pipeline (extract → validate → clean → enrich → load)
├── data/
│   ├── weather_code.csv         # WMO code lookup (source-controlled reference)
│   ├── location.csv             # Louisville, KY coordinates and timezone
│   ├── sector.csv               # Business sector definitions
│   ├── sector_weather_sensitivity.csv  # Weather variable → business signal mapping
│   ├── daily_weather_forecast.csv      # Generated output (gitignored)
│   └── weekly_weather_summary.csv      # Generated output (gitignored)
├── docs/
│   ├── Database_Schema_Documentation.pdf
│   └── VALIDATION.md            # Data quality rules and threshold reference
├── .env                         # Database credentials (gitignored — never commit)
├── .env.example                 # Credential template (safe to commit)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Setup

### 1. Clone the repository

```bash
git clone <repo-url>
cd Louisville_Weather_Business_Risk_Monitor
```

### 2. Create and activate a virtual environment

```bash
python -m venv .venv
# macOS / Linux
source .venv/bin/activate
# Windows
.venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure database credentials

Copy `.env.example` to `.env` and fill in your Supabase connection details:

```bash
cp .env.example .env
```

`.env` format:

```
user=postgres.YOUR_PROJECT_REF
password=YOUR_DB_PASSWORD
host=aws-0-REGION.pooler.supabase.com
port=5432
dbname=postgres
```

> ⚠️ **Never commit `.env` to version control.** It is listed in `.gitignore`.

---

## Running the Pipeline

```bash
python src/ETL_script.py
```

The pipeline runs nine sequential stages and logs progress to both the console and `etl_pipeline.log`:

| Stage | Description |
|-------|-------------|
| 1 | Extract 16-day forecast from Open-Meteo API |
| 2 | Validate raw API response structure |
| 3 | Clean and normalize fields; compute derived metrics |
| 4 | Enrich with weather-code descriptions (CSV join) |
| 5 | Data quality validation (nulls, ranges, business rules, duplicates) |
| 6 | Build weekly aggregation layer for Power BI |
| 7 | Incremental date-key upsert to CSV outputs |
| 8 | Full schema reset and bulk insert to Supabase PostgreSQL |
| 9 | Post-load row count reconciliation |

CSV outputs are preserved even if the database load fails (stages 1–7 run in a separate try/except block).

---

## Database Schema

The PostgreSQL database contains seven tables normalized to 3NF:

| Table | Description |
|-------|-------------|
| `location` | Single monitored location — Louisville, KY (lat 38.2542, lon -85.7594) |
| `weather_code` | WMO code lookup: numeric code → description + severity level |
| `sector` | Five Louisville business sectors |
| `weather_observation` | Daily raw weather metrics from Open-Meteo |
| `risk_metric` | Derived degree days, weather flags, risk score (one-to-one with observation) |
| `sector_weather_sensitivity` | Reference mapping of weather variables to business signals per sector |
| `sector_daily_impact` | Per-sector impact assessment for each forecast day |

See `docs/Database_Schema_Documentation.pdf` for the full ERD and column definitions.

---

## Loading Strategy

**CSV outputs** use an incremental date-key upsert: existing dates are overwritten with the latest forecast values and new dates are appended, preserving a growing historical record.

**Database** uses a full schema reset on every run. Because Power BI requires a coherent 16-day forecast window, retaining stale rows alongside a revised API response would produce incorrect risk scores. The CSV files carry the incremental history; the database is always a clean current snapshot.

---

## Risk Scoring Reference

| Score Range | Risk Level |
|-------------|------------|
| 0 – 19 | calm |
| 20 – 39 | mild |
| 40 – 59 | elevated |
| 60 – 79 | high |
| 80 – 100 | severe |

Score components: heavy rain flag (+30), high wind flag (+30), extreme temperature flag (+25), moderate rain (+10), any precipitation (+10), capped at 100.

---

## Data Sources

- **Weather forecasts:** [Open-Meteo API](https://open-meteo.com/) — free, no API key required
- **Reference data:** Project-maintained CSV files (`data/` folder)
- **Database host:** [Supabase](https://supabase.com/) (PostgreSQL)

---

## Logging

The pipeline writes structured logs to `etl_pipeline.log` (overwritten each run) and mirrors output to stdout. Log entries include timestamps and severity levels.

---

## License

For academic and portfolio use. Weather data sourced from Open-Meteo under the [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) license.
