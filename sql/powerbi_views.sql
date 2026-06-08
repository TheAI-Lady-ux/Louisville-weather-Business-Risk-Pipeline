-- =============================================================================
-- Louisville Weather and Business Risk Monitor
-- Power BI SQL Views — Run these in Supabase SQL Editor BEFORE opening Power BI
-- Developer: Oluwatosin Adelusi
--
-- How to run:
--   1. Open your Supabase project → SQL Editor
--   2. Paste this entire file and click Run
--   3. All 4 views will be created in the public schema
--   4. They will appear as tables in Power BI when you connect
-- =============================================================================


-- =============================================================================
-- VIEW 1: v_forecast_full
-- Purpose : Main analytical view — one row per forecast day
-- Used for: Temperature trend, Risk score bar chart, Precipitation chart,
--           Wind/Humidity chart, KPI cards, and Date slicer
-- =============================================================================
CREATE OR REPLACE VIEW public.v_forecast_full AS
SELECT
    wo.observation_id,
    wo.observation_date,

    -- Date parts for Power BI time intelligence
    EXTRACT(YEAR  FROM wo.observation_date)::INT          AS forecast_year,
    EXTRACT(MONTH FROM wo.observation_date)::INT          AS forecast_month,
    EXTRACT(DAY   FROM wo.observation_date)::INT          AS forecast_day,
    TO_CHAR(wo.observation_date, 'Mon DD')                AS date_label,
    TO_CHAR(wo.observation_date, 'Day')                   AS day_of_week,

    -- Location
    l.city,
    l.state,

    -- Weather description
    wc.description                                        AS weather_description,
    wc.severity_level,

    -- Temperature columns
    wo.temp_max_f,
    wo.temp_min_f,
    wo.temp_mean_f,
    wo.apparent_temp_f,
    ROUND(wo.temp_max_f - wo.temp_min_f, 2)              AS temp_range_f,

    -- Precipitation
    wo.precipitation_in,
    CASE
        WHEN wo.precipitation_in IS NULL OR wo.precipitation_in = 0 THEN 'Dry'
        WHEN wo.precipitation_in <= 0.25                             THEN 'Light Rain'
        WHEN wo.precipitation_in <= 1.0                              THEN 'Moderate Rain'
        ELSE                                                              'Heavy Rain'
    END                                                   AS precip_category,
    CASE WHEN wo.precipitation_in > 0 THEN 1 ELSE 0 END  AS has_precipitation,

    -- Wind & atmosphere
    wo.wind_speed_max_mph,
    wo.humidity_pct,
    wo.cloud_cover_pct,
    wo.uv_index,

    -- Sunrise / sunset
    wo.sunrise,
    wo.sunset,

    -- Risk metrics (from risk_metric table)
    rm.hdd,
    rm.cdd,
    rm.weather_risk_score,
    rm.risk_level,

    -- Risk flags as integers (1/0) so Power BI can SUM them as counts
    CASE WHEN rm.heavy_rain_flag    THEN 1 ELSE 0 END     AS heavy_rain_flag,
    CASE WHEN rm.high_wind_flag     THEN 1 ELSE 0 END     AS high_wind_flag,
    CASE WHEN rm.extreme_temp_flag  THEN 1 ELSE 0 END     AS extreme_temp_flag,
    CASE WHEN rm.severe_weather_flag THEN 1 ELSE 0 END    AS severe_weather_flag,

    -- Numeric risk level for conditional formatting in Power BI
    CASE rm.risk_level
        WHEN 'calm'     THEN 1
        WHEN 'mild'     THEN 2
        WHEN 'elevated' THEN 3
        WHEN 'high'     THEN 4
        WHEN 'severe'   THEN 5
        ELSE 0
    END                                                   AS risk_level_num

FROM public.weather_observation wo
JOIN public.location        l   ON l.location_id    = wo.location_id
LEFT JOIN public.weather_code wc ON wc.weather_code_id = wo.weather_code_id
LEFT JOIN public.risk_metric  rm ON rm.observation_id  = wo.observation_id
ORDER BY wo.observation_date;


-- =============================================================================
-- VIEW 2: v_sector_impact
-- Purpose : One row per (date × sector) combination
-- Used for: Sector impact matrix / heatmap visual
-- =============================================================================
CREATE OR REPLACE VIEW public.v_sector_impact AS
SELECT
    wo.observation_date,
    TO_CHAR(wo.observation_date, 'Mon DD')                AS date_label,
    s.sector_name,
    sdi.impact_level,

    -- Numeric impact for conditional colour formatting in Power BI
    CASE sdi.impact_level
        WHEN 'low'      THEN 1
        WHEN 'moderate' THEN 2
        WHEN 'high'     THEN 3
        WHEN 'severe'   THEN 4
        ELSE 0
    END                                                   AS impact_level_num,

    -- Risk score from the matching day (useful for tooltips)
    rm.weather_risk_score,
    rm.risk_level,
    wo.temp_mean_f,
    wo.precipitation_in,
    wo.wind_speed_max_mph

FROM public.sector_daily_impact sdi
JOIN public.weather_observation wo ON wo.observation_id = sdi.observation_id
JOIN public.sector              s  ON s.sector_id       = sdi.sector_id
LEFT JOIN public.risk_metric    rm ON rm.observation_id = wo.observation_id
ORDER BY wo.observation_date, s.sector_name;


-- =============================================================================
-- VIEW 3: v_weekly_summary
-- Purpose : Week-level aggregations for trend and summary visuals
-- Used for: Weekly temperature trend, weekly precipitation totals
-- =============================================================================
CREATE OR REPLACE VIEW public.v_weekly_summary AS
SELECT
    DATE_TRUNC('week', wo.observation_date)::DATE         AS week_start,
    DATE_TRUNC('week', wo.observation_date)::DATE + 6     AS week_end,
    TO_CHAR(DATE_TRUNC('week', wo.observation_date), 'Mon DD') AS week_label,

    COUNT(*)                                              AS days_in_week,
    ROUND(AVG(wo.temp_max_f)::NUMERIC, 1)                 AS avg_max_temp_f,
    ROUND(AVG(wo.temp_min_f)::NUMERIC, 1)                 AS avg_min_temp_f,
    ROUND(AVG(wo.temp_mean_f)::NUMERIC, 1)                AS avg_mean_temp_f,
    MAX(wo.temp_max_f)                                    AS peak_temp_f,
    MIN(wo.temp_min_f)                                    AS lowest_temp_f,
    ROUND(SUM(wo.precipitation_in)::NUMERIC, 2)           AS total_precip_in,
    SUM(CASE WHEN wo.precipitation_in > 0 THEN 1 ELSE 0 END) AS rainy_days,
    ROUND(AVG(wo.wind_speed_max_mph)::NUMERIC, 1)         AS avg_wind_mph,
    MAX(wo.wind_speed_max_mph)                            AS peak_wind_mph,
    ROUND(AVG(wo.humidity_pct)::NUMERIC, 1)               AS avg_humidity_pct,
    ROUND(AVG(wo.uv_index)::NUMERIC, 1)                   AS avg_uv_index,

    -- Risk aggregations
    ROUND(AVG(rm.weather_risk_score)::NUMERIC, 0)         AS avg_risk_score,
    MAX(rm.weather_risk_score)                            AS peak_risk_score,
    SUM(CASE WHEN rm.severe_weather_flag THEN 1 ELSE 0 END) AS severe_weather_days,
    SUM(CASE WHEN rm.risk_level IN ('high','severe') THEN 1 ELSE 0 END) AS high_risk_days

FROM public.weather_observation wo
LEFT JOIN public.risk_metric rm ON rm.observation_id = wo.observation_id
GROUP BY DATE_TRUNC('week', wo.observation_date)
ORDER BY week_start;


-- =============================================================================
-- VIEW 4: v_kpi_summary
-- Purpose : Single-row summary of the entire current forecast window
-- Used for: KPI cards in Power BI (card visuals)
-- =============================================================================
CREATE OR REPLACE VIEW public.v_kpi_summary AS
SELECT
    MIN(wo.observation_date)                              AS forecast_start,
    MAX(wo.observation_date)                              AS forecast_end,
    COUNT(*)                                              AS total_forecast_days,

    ROUND(AVG(wo.temp_mean_f)::NUMERIC, 1)                AS avg_temp_f,
    MAX(wo.temp_max_f)                                    AS peak_temp_f,
    MIN(wo.temp_min_f)                                    AS lowest_temp_f,
    ROUND(SUM(wo.precipitation_in)::NUMERIC, 2)           AS total_precip_in,
    SUM(CASE WHEN wo.precipitation_in > 0 THEN 1 ELSE 0 END) AS rainy_days,

    ROUND(AVG(rm.weather_risk_score)::NUMERIC, 0)         AS avg_risk_score,
    MAX(rm.weather_risk_score)                            AS peak_risk_score,
    SUM(CASE WHEN rm.risk_level IN ('high','severe') THEN 1 ELSE 0 END) AS high_risk_days,
    SUM(CASE WHEN rm.severe_weather_flag THEN 1 ELSE 0 END) AS severe_weather_days,
    SUM(CASE WHEN rm.heavy_rain_flag    THEN 1 ELSE 0 END)  AS heavy_rain_days,
    SUM(CASE WHEN rm.high_wind_flag     THEN 1 ELSE 0 END)  AS high_wind_days,

    ROUND(AVG(wo.wind_speed_max_mph)::NUMERIC, 1)         AS avg_wind_mph,
    ROUND(AVG(wo.humidity_pct)::NUMERIC, 1)               AS avg_humidity_pct

FROM public.weather_observation wo
LEFT JOIN public.risk_metric rm ON rm.observation_id = wo.observation_id;


-- =============================================================================
-- VERIFY: Quick check — run this after creating the views to confirm they work
-- =============================================================================
-- SELECT COUNT(*) AS rows FROM public.v_forecast_full;
-- SELECT COUNT(*) AS rows FROM public.v_sector_impact;
-- SELECT COUNT(*) AS rows FROM public.v_weekly_summary;
-- SELECT * FROM public.v_kpi_summary;
