# ETL Data Validation Reference

**Project:** Louisville Weather and Business Risk Monitor  
**Developer:** Oluwatosin Adelusi  
**Scope:** All validation checks applied in the ETL pipeline (`ETL_script.py`), in execution order.

---

## Overview

Validation is applied at two points in the pipeline:

- **Stage 2 — Raw Response Validation:** Checks the API payload before any transformation runs.
- **Stage 5 — Clean Data Validation:** Checks the transformed DataFrame after cleaning and enrichment.

A failure at either stage raises an exception, halts the pipeline, and logs a descriptive error message. This prevents partial or corrupt data from reaching the CSV outputs or the database.

---

## Stage 2: Raw Response Validation

Applied in `validate_raw_response()` immediately after the Open-Meteo API call.

### Check 1 — Response Type

| Property | Detail |
|----------|--------|
| **What is checked** | The API response must be a Python dictionary |
| **Failure condition** | Response is not of type `dict` |
| **Error raised** | `ValueError: API response must be a dictionary` |
| **Rationale** | Catches network errors or unexpected response formats before any field access |

---

### Check 2 — Daily Data Presence

| Property | Detail |
|----------|--------|
| **What is checked** | The response must contain a non-empty `daily` key |
| **Failure condition** | `daily` key is absent or evaluates to falsy |
| **Error raised** | `ValueError: No daily data returned. Keys present: [...]` |
| **Rationale** | A valid but empty or restructured API response would produce zero rows without this guard |

---

### Check 3 — Required Raw Fields

| Property | Detail |
|----------|--------|
| **What is checked** | The `daily` object must contain all of: `time`, `weather_code`, `temperature_2m_max`, `temperature_2m_min` |
| **Failure condition** | Any of the four fields are missing from the `daily` payload |
| **Error raised** | `ValueError: Daily forecast missing required fields: [...]` |
| **Rationale** | These fields are the minimum needed to drive downstream transformation and risk scoring |

---

## Stage 5: Clean Data Validation

Applied in `validate_clean_forecast()` after cleaning, normalization, and weather-code enrichment.

### Check 1 — Schema / Column Presence

| Property | Detail |
|----------|--------|
| **What is checked** | The transformed DataFrame must contain all required columns |
| **Required columns** | `weather_code`, `temp_max_f`, `temp_min_f`, `temp_mean_f`, `precipitation_in`, `wind_speed_max_mph`, `Description` |
| **Failure condition** | One or more required columns are absent |
| **Error raised** | `ValueError: Clean forecast missing required columns: [...]` |
| **Rationale** | Verifies that all rename and enrichment steps completed correctly before downstream checks run |

---

### Check 2 — Null Value Check

| Property | Detail |
|----------|--------|
| **What is checked** | Critical analytical fields must contain no null values |
| **Fields checked** | `weather_code`, `temp_max_f`, `temp_min_f`, `temp_mean_f` |
| **Failure condition** | Any null is found in any of the four fields |
| **Error raised** | `ValueError: Null values found in required fields: {field: null_count, ...}` |
| **Rationale** | Null temperatures would produce undefined HDD/CDD values and invalidate risk scores |

---

### Check 3 — Temperature Business Rule

| Property | Detail |
|----------|--------|
| **What is checked** | Daily maximum temperature must be greater than or equal to daily minimum temperature |
| **Condition** | `temp_max_f >= temp_min_f` for every row |
| **Failure condition** | Any row where `temp_max_f < temp_min_f` |
| **Error raised** | `ValueError: temp_max_f < temp_min_f on N row(s)` |
| **Rationale** | A physically impossible inversion indicates a data corruption or API field swap and would produce a negative temperature range |

---

### Check 4 — Temperature Range Validation

| Property | Detail |
|----------|--------|
| **What is checked** | All temperature values must fall within realistic meteorological bounds |
| **Fields checked** | `temp_max_f`, `temp_min_f`, `temp_mean_f` |
| **Acceptable range** | −60 °F to 130 °F (inclusive) |
| **Failure condition** | Any value outside the range `[-60, 130]` |
| **Error raised** | `ValueError: {col}: N value(s) outside realistic range [-60, 130]` |
| **Rationale** | Louisville's all-time recorded range is approximately 20 °F to 107 °F. The wider bounds accommodate data quality margins while still catching extreme outliers that would corrupt degree-day and risk calculations |

---

### Check 5 — Precipitation Non-Negativity

| Property | Detail |
|----------|--------|
| **What is checked** | Precipitation must be zero or positive for every row |
| **Field checked** | `precipitation_in` |
| **Failure condition** | Any row where `precipitation_in < 0` |
| **Error raised** | `ValueError: Negative precipitation_in on N row(s)` |
| **Rationale** | Negative precipitation is physically impossible and would misclassify the `precip_category` and `heavy_rain_flag` fields |

---

### Check 6 — Duplicate Date Detection

| Property | Detail |
|----------|--------|
| **What is checked** | The date index must be unique — no date may appear more than once |
| **Failure condition** | Any duplicated date key in the DataFrame index |
| **Error raised** | `ValueError: Duplicate date keys found: [date1, date2, ...]` |
| **Rationale** | The `weather_observation` table enforces `UNIQUE (location_id, observation_date)`; duplicate rows detected here prevent a database constraint violation during the load stage |

---

## Derived Metric Thresholds

These thresholds are applied during cleaning (Stage 3) to compute flag and category columns. They are not validation failures but define the business logic baked into the pipeline.

### Precipitation Classification (`precip_category`)

| Range (inches/day) | Category |
|--------------------|----------|
| ≤ 0 | Dry |
| 0.01 – 0.25 | Light Rain |
| 0.26 – 1.00 | Moderate Rain |
| > 1.00 | Heavy Rain |

### Boolean Flags

| Flag | Trigger Condition |
|------|-------------------|
| `heavy_rain_flag` | `precipitation_in > 1.0 inches` |
| `high_wind_flag` | `wind_speed_max_mph > 25.0 mph` |
| `extreme_temp_flag` | `temp_mean_f < 32 °F` OR `temp_mean_f > 95 °F` |
| `severe_weather_flag` | `heavy_rain_flag OR high_wind_flag OR extreme_temp_flag` |

### Risk Score Composition

| Condition | Points Added |
|-----------|-------------|
| `heavy_rain_flag = true` | +30 |
| `high_wind_flag = true` | +30 |
| `extreme_temp_flag = true` | +25 |
| `precip_category = 'Moderate Rain'` | +10 |
| Any precipitation (`precipitation_in > 0`) | +10 |
| **Maximum (capped)** | **100** |

### Risk Level Bands

| Score Range | Risk Level |
|-------------|------------|
| 0 – 19 | calm |
| 20 – 39 | mild |
| 40 – 59 | elevated |
| 60 – 79 | high |
| 80 – 100 | severe |

---

## Sector Impact Rules

Applied in Stage 8 (`build_db_tables`) to derive `sector_daily_impact` rows. Each rule maps risk metric field values to an impact level for the sector.

| Sector | Trigger Condition | Impact Level Assigned |
|--------|-------------------|-----------------------|
| Utilities | `hdd > 15` OR `cdd > 15` | moderate |
| Utilities | All other conditions | low |
| Retail | `heavy_rain_flag = true` | moderate |
| Retail | All other conditions | low |
| Logistics | `high_wind_flag = true` OR `heavy_rain_flag = true` | high |
| Logistics | All other conditions | low |
| Tourism | `severe_weather_flag = true` | severe |
| Tourism | `heavy_rain_flag = true` (no severe weather) | moderate |
| Tourism | All other conditions | low |
| Insurance | `severe_weather_flag = true` | high |
| Insurance | All other conditions | low |

---

## Post-Load Verification (Stage 9)

After all tables are inserted, `verify_row_counts()` queries each table and compares actual row counts to the expected values calculated from the source DataFrames. Results are logged as `PASS` or `MISMATCH` for each table.

| Table Verified |
|----------------|
| `location` |
| `weather_code` |
| `sector` |
| `weather_observation` |
| `risk_metric` |
| `sector_weather_sensitivity` |
| `sector_daily_impact` |

A `MISMATCH` log entry indicates a partial load or silent insertion failure and should be investigated before the next pipeline run.

---

## Warnings (Non-Failing Conditions)

The following conditions log a `WARNING` but do not halt the pipeline:

| Condition | Log Message |
|-----------|-------------|
| One or more forecast rows did not match a weather-code description after the enrichment join | `N forecast rows did not match a weather-code description` |

This can occur if the Open-Meteo API returns a WMO code not yet present in `weather_code.csv`. The `Description` field will be null for those rows; the risk scoring is unaffected since it operates on numeric weather fields, not the description.
