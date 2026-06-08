# Louisville Weather and Business Risk Monitor

**Developer:** Oluwatosin Adelusi  
**Course:** Data Engineering — Weeks 3 & 4  
**Stack:** Python · Open-Meteo API · Supabase PostgreSQL · Plotly Dash

---

## Project Overview

This project builds a complete end-to-end data engineering solution that monitors how Louisville, KY weather forecasts affect five key business sectors: **Utilities, Retail, Logistics, Tourism, and Insurance**.

The ETL pipeline fetches a rolling 16-day weather forecast from the Open-Meteo API, applies multi-stage cleaning, transformation, and validation, computes per-sector business risk scores, and loads the results into a normalized 7-table PostgreSQL database on Supabase. A Plotly Dash web application then queries the live database to deliver an interactive analytics dashboard with real-time filtering and business intelligence.

---

## Dashboard Preview

> **Run the app and open http://127.0.0.1:8050 to see the live dashboard.**

The dashboard includes:

- 5 colour-coded KPI summary cards
- 5 interactive charts covering temperature, risk, sector impact, precipitation, and wind
- 3 dynamic filters that update all charts simultaneously
- A dark professional layout with a collapsible sidebar

---

## Business Insights

The dashboard answers five key operational questions for Louisville businesses:

**1. Which days carry the highest weather risk?**  
The Daily Weather Risk Score chart assigns each forecast day a composite score (0–100) based on wind speed, precipitation, and temperature extremes. Risk levels range from *calm* (green) to *severe* (purple), giving operations teams a one-glance view of the riskiest days in the 16-day window.

**2. How will temperature trends affect energy demand?**  
Heating Degree Days (HDD) and Cooling Degree Days (CDD) are computed for every forecast day. Days where HDD > 15 or CDD > 15 drive elevated impact for the Utilities sector, indicating periods of peak energy consumption. The temperature trend chart makes these spikes immediately visible.

**3. Which business sectors face disruption — and when?**  
The Sector Business Impact heatmap shows a matrix of all 5 sectors against all 16 forecast days. Each cell is colour-coded (green → orange → red → purple) so sector managers can see at a glance exactly which days require contingency planning.

**4. How much rain is coming and how does it affect retail foot traffic?**  
The Precipitation chart categorises each day as Dry, Light Rain, Moderate Rain, or Heavy Rain. Heavy rain (> 1 inch) triggers a Moderate impact on the Retail sector, representing reduced foot traffic and revenue risk.

**5. Are wind conditions safe for logistics operations?**  
The Wind & Humidity chart flags days where max wind speed exceeds 25 mph. Any such day triggers a High impact rating for the Logistics sector, signalling potential for delivery delays, rerouting, and elevated operational costs.

| Sector | Key Weather Trigger | Impact Level | Business Signal |
|---|---|---|---|
| **Logistics** | Wind > 25 mph or rain > 1 in | High | Delivery delays, rerouting costs |
| **Utilities** | HDD or CDD > 15 | Moderate | Heating/cooling demand spike |
| **Retail** | Precipitation > 1 in | Moderate | Foot traffic and revenue drop |
| **Tourism** | Severe weather composite | Severe | Event cancellations, visitor loss |
| **Insurance** | Severe weather composite | High | Elevated claims risk |

---

## Project Structure

```
Louisville_Weather_Business_Risk_Monitor/
├── src/
│   ├── ETL_script.py                  # Week 3 — Full ETL pipeline
│   └── dash_app.py                    # Week 4 — Interactive Dash dashboard
├── data/
│   ├── weather_code.csv               # WMO weather code descriptions (reference)
│   ├── location.csv                   # Louisville location record
│   ├── sector.csv                     # Five business sectors
│   ├── sector_weather_sensitivity.csv # Per-sector sensitivity rules
│   ├── daily_weather_forecast.csv     # Generated — rolling daily forecast history
│   └── weekly_weather_summary.csv     # Generated — weekly aggregation layer
├── .env                               # Database credentials (never committed)
├── .gitignore
├── requirements.txt
├── validation.md                      # Data quality documentation
└── README.md
```

---

## Prerequisites

- Python 3.11 or higher
- A Supabase project (free tier works fine)
- Microsoft Power BI Desktop (optional — for additional reporting)

### `.env` file

Create a file named `.env` in the project root. **Never commit this file.**

```
user=postgres.YOUR_PROJECT_REF
password=YOUR_DB_PASSWORD
host=aws-0-REGION.pooler.supabase.com
port=5432
dbname=postgres
```

Find these values in your Supabase project under **Settings → Database → Connection parameters**.

---

## Installation

```bash
# 1. Clone the repository
git clone https://github.com/YOUR_USERNAME/Louisville_Weather_Business_Risk_Monitor.git
cd Louisville_Weather_Business_Risk_Monitor

# 2. Create and activate a virtual environment
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

# 3. Install all dependencies
pip install -r requirements.txt
```

---

## Step 1 — Run the ETL Pipeline

The ETL script must be run at least once before launching the dashboard. It populates the Supabase database with the current 16-day forecast.

```bash
cd src
python ETL_script.py
```

**Pipeline stages:**
1. Fetch 16-day daily forecast from Open-Meteo API (Louisville, KY)
2. Validate raw API response structure
3. Clean and normalise — convert unix timestamps, rename columns, coerce types
4. Enrich with weather-code descriptions from lookup CSV
5. Validate clean data — nulls, ranges, business rules, duplicate detection
6. Aggregate weekly summary layer
7. Save incremental CSV outputs
8. Reset Supabase schema and bulk-load all 7 tables
9. Verify post-load row counts

**Expected terminal output:**
```
INFO | Database connection successful
INFO | Raw response validation passed — 16 forecast days received
INFO | All data quality checks passed
INFO | ETL pipeline completed successfully — database loaded
INFO | location          expected=1   actual=1   [PASS]
INFO | weather_code      expected=21  actual=21  [PASS]
INFO | weather_observation expected=16 actual=16 [PASS]
INFO | risk_metric       expected=16  actual=16  [PASS]
INFO | sector_daily_impact expected=80 actual=80 [PASS]
INFO | Row count verification complete
```

---

## Step 2 — Run the Dash Application

```bash
cd src
python dash_app.py
```

Open your browser and go to: **http://127.0.0.1:8050**

Press `Ctrl+C` in the terminal to stop the server.

---

## Dashboard Components

### KPI Summary Cards

Five cards display summary metrics for the currently filtered date window:

| Card | Metric | Colour |
|---|---|---|
| Avg Temperature | Mean daily temperature in °F | Blue |
| Total Precipitation | Accumulated rainfall in inches | Green |
| Avg Risk Score | Composite weather risk score (0–100) | Red-pink |
| High-Risk Days | Days rated *high* or *severe* | Orange |
| Severe Weather Flags | Days with a composite severe flag | Purple |

### Charts

| Chart | Type | Business Use |
|---|---|---|
| 16-Day Temperature Forecast | Line chart (max / mean / min) | Energy demand planning, staff scheduling |
| Daily Weather Risk Score | Bar chart — colour-coded by risk level | Operations risk triage |
| Sector Business Impact | Heatmap — sector × day matrix | Multi-sector contingency planning |
| Daily Precipitation Forecast | Bar chart — categorised by intensity | Retail foot traffic, logistics planning |
| Wind Speed & Humidity | Dual-axis line + bar chart | Logistics safety, outdoor operations |

### Interactive Filters

All three filters update every chart and KPI card simultaneously without restarting the app:

| Filter | Type | What it controls |
|---|---|---|
| **Date Range** | Date range picker | Narrows all charts to a specific forecast window |
| **Business Sector** | Multi-select checklist | Focuses the sector impact heatmap |
| **Risk Level** | Dropdown | Highlights only days matching the selected risk band |

---

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| `requests` | 2.32.3 | Open-Meteo API HTTP calls |
| `pandas` | 2.2.3 | Data transformation and aggregation |
| `psycopg2-binary` | 2.9.9 | PostgreSQL database driver |
| `SQLAlchemy` | 2.0.36 | Database engine and query execution |
| `python-dotenv` | 1.0.1 | `.env` credential loading |
| `dash` | 2.17.1 | Interactive web dashboard framework |
| `plotly` | 5.22.0 | Interactive chart library |

```bash
pip install -r requirements.txt
```

---

## Database Schema

Seven normalised tables (3NF) in Supabase PostgreSQL:

```
location ──< weather_observation >── weather_code
                    │
                    ├──< risk_metric
                    └──< sector_daily_impact >── sector
                                                    │
                                          sector_weather_sensitivity
```

| Table | Rows | Description |
|---|---|---|
| `location` | 1 | Louisville, KY coordinates and timezone |
| `weather_code` | 21 | WMO weather code descriptions and severity |
| `sector` | 5 | Business sectors tracked |
| `weather_observation` | 16 | Daily forecast — temperature, wind, rain, UV |
| `risk_metric` | 16 | Derived risk scores, flags, HDD/CDD per day |
| `sector_weather_sensitivity` | 15 | Per-sector weather sensitivity rules |
| `sector_daily_impact` | 80 | Impact level per sector per forecast day |

---

## Data Quality

See [validation.md](validation.md) for full documentation. The pipeline runs three validation rounds on every execution:

- **Round 1 — Raw response** (4 checks): API contract, required fields, row count
- **Round 2 — Clean data** (6 checks): schema, nulls, business rules, temperature bounds, negative precipitation, duplicate dates
- **Round 3 — Post-load** (7 table checks): row count reconciliation between source DataFrames and Supabase

---

## Incremental Loading Strategy

**CSV layer** uses a date-key upsert — existing dates are overwritten with the freshest forecast values and new dates are appended. This builds a growing historical record across multiple pipeline runs.

**Database layer** uses a full schema reset on every run. The dashboard shows a single coherent 16-day rolling window; retaining stale rows alongside a revised forecast would produce incorrect risk scores. The CSVs handle historical retention; the database is always a clean, current snapshot.

---

## API Reference

Weather data: [Open-Meteo](https://open-meteo.com/) — free, open-source, no API key required.  
Forecast location: Louisville, KY — 38.2542°N, 85.7594°W  
Forecast window: 16 days rolling, updated on every ETL run
