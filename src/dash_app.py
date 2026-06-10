"""
Louisville Weather and Business Risk Monitor
Dash Application — Week 4: Interactive Analytics MVP
Developer: Oluwatosin Adelusi

Dashboard features:
  - Live data pulled from Supabase PostgreSQL via SQLAlchemy
  - KPI cards: average temperature, total precipitation, avg risk score, high-risk day count
  - Temperature trend chart (16-day max / mean / min line chart)
  - Weather risk score bar chart (colour-coded by risk level)
  - Sector business impact heatmap (sector x day impact matrix)
  - Interactive filters: date range picker + sector multi-select + risk level dropdown
  - All charts update dynamically via Dash callbacks

How to run:
    cd src
    python dash_app.py
    Open http://127.0.0.1:8050 in your browser

Required packages:
    pip install dash plotly pandas sqlalchemy psycopg2-binary python-dotenv
"""

import os
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import plotly.graph_objects as go
from dash import Dash, Input, Output, callback, dcc, html
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =============================================================================
# DATABASE CONNECTION
# =============================================================================

def _find_and_load_dotenv() -> None:
    """Walk up from this file's folder until a .env is found."""
    for folder in [Path(__file__).resolve().parent, *Path(__file__).resolve().parents]:
        env_file = folder / ".env"
        if env_file.exists():
            load_dotenv(env_file)
            return
    load_dotenv()


def get_engine():
    """Build a SQLAlchemy engine from .env credentials."""
    _find_and_load_dotenv()
    fields = {k: os.getenv(k) for k in ["user", "password", "host", "port", "dbname"]}
    missing = [k for k, v in fields.items() if not v]
    if missing:
        raise RuntimeError(
            f"Missing database credential(s): {missing}. "
            "Ensure your .env contains: user, password, host, port, dbname"
        )
    url = (
        f"postgresql+psycopg2://{quote_plus(fields['user'])}:{quote_plus(fields['password'])}"
        f"@{fields['host']}:{fields['port']}/{fields['dbname']}?sslmode=require"
    )
    return create_engine(url, pool_pre_ping=True)


ENGINE = get_engine()
# =============================================================================
# DATA LOADERS
# =============================================================================

def load_forecast_data() -> pd.DataFrame:
    query = text("""
        SELECT
            wo.observation_date,
            wo.temp_max_f,
            wo.temp_min_f,
            wo.temp_mean_f,
            wo.apparent_temp_f,
            wo.precipitation_in,
            wo.wind_speed_max_mph,
            wo.humidity_pct,
            wo.cloud_cover_pct,
            wo.uv_index,
            rm.hdd,
            rm.cdd,
            rm.precip_category,
            rm.heavy_rain_flag,
            rm.high_wind_flag,
            rm.extreme_temp_flag,
            rm.severe_weather_flag,
            rm.weather_risk_score,
            rm.risk_level,
            wc.description AS weather_description
        FROM public.weather_observation wo
        LEFT JOIN public.risk_metric rm
               ON rm.observation_id = wo.observation_id
        LEFT JOIN public.weather_code wc
               ON wc.weather_code_id = wo.weather_code_id
        ORDER BY wo.observation_date
    """)
    with ENGINE.connect() as conn:
        df = pd.read_sql(query, conn)
    df["observation_date"] = pd.to_datetime(df["observation_date"])
    return df

def load_sector_impact() -> pd.DataFrame:
    query = text("""
        SELECT
            wo.observation_date,
            s.sector_name,
            sdi.impact_level
        FROM public.sector_daily_impact sdi
        JOIN public.weather_observation wo
               ON wo.observation_id = sdi.observation_id
        JOIN public.sector s
               ON s.sector_id = sdi.sector_id
        ORDER BY wo.observation_date, s.sector_name
    """)
    with ENGINE.connect() as conn:
        df = pd.read_sql(query, conn)
    df["observation_date"] = pd.to_datetime(df["observation_date"])
    return df

# Load once at startup
FORECAST_DF = load_forecast_data()
IMPACT_DF   = load_sector_impact()

MIN_DATE    = FORECAST_DF["observation_date"].min().date()
MAX_DATE    = FORECAST_DF["observation_date"].max().date()
ALL_SECTORS = sorted(IMPACT_DF["sector_name"].unique().tolist())

# =============================================================================
# COLOURS
# =============================================================================

RISK_COLOURS = {
    "calm":     "#2ecc71",
    "mild":     "#f1c40f",
    "elevated": "#e67e22",
    "high":     "#e74c3c",
    "severe":   "#8e44ad",
}

IMPACT_ORDER = {"low": 1, "moderate": 2, "high": 3, "severe": 4}

PRIMARY   = "#1a1a2e"
SECONDARY = "#16213e"
ACCENT    = "#0f3460"
HIGHLIGHT = "#e94560"
LIGHT     = "#f5f5f5"

GRID_FAINT  = "rgba(255,255,255,0.08)"
LINE_FAINT  = "rgba(255,255,255,0.13)"
TRANSPARENT = "rgba(255,255,255,0)"

# =============================================================================
# SHARED CHART LAYOUT — applied to every figure via **CHART_LAYOUT
# =============================================================================

CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=LIGHT, family="Segoe UI, Arial, sans-serif", size=12),
    margin=dict(l=48, r=16, t=48, b=48),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
    xaxis=dict(
        gridcolor=GRID_FAINT,
        linecolor=LINE_FAINT,
        tickcolor=LINE_FAINT,
    ),
    yaxis=dict(
        gridcolor=GRID_FAINT,
        linecolor=LINE_FAINT,
        tickcolor=LINE_FAINT,
    ),
)
# =============================================================================
# LAYOUT HELPERS
# =============================================================================

def kpi_card(title: str, value: str, subtitle: str = "", colour: str = HIGHLIGHT):
    return html.Div(
        children=[
            html.P(title,    style={"color": "#aaa", "fontSize": "11px",
                                    "fontWeight": "600", "letterSpacing": "1px",
                                    "margin": "0 0 6px"}),
            html.H2(value,   style={"color": colour, "fontSize": "28px",
                                    "fontWeight": "700", "margin": "0 0 4px"}),
            html.P(subtitle, style={"color": "#888", "fontSize": "11px", "margin": "0"}),
        ],
        style={
            "background": SECONDARY,
            "borderRadius": "8px",
            "padding": "20px 24px",
            "flex": "1",
            "minWidth": "160px",
            "borderLeft": f"4px solid {colour}",
            "boxShadow": "0 2px 8px rgba(0,0,0,0.3)",
        },
    )
# =============================================================================
# APP LAYOUT
# =============================================================================

app = Dash(
    __name__,
    title="Louisville Weather & Business Risk Monitor",
    suppress_callback_exceptions=True,
)

app.layout = html.Div(
    style={"fontFamily": "'Segoe UI', Arial, sans-serif",
           "background": "#0d0d1a", "minHeight": "100vh"},
    children=[

        # Header
        html.Div(
            style={
                "background": PRIMARY,
                "padding": "18px 32px",
                "display": "flex",
                "alignItems": "center",
                "justifyContent": "space-between",
                "borderBottom": f"3px solid {HIGHLIGHT}",
            },
            children=[
                html.Div([
                    html.H1(
                        "Louisville Weather & Business Risk Monitor",
                        style={"color": LIGHT, "margin": 0, "fontSize": "22px",
                               "fontWeight": "700", "letterSpacing": "0.5px"},
                    ),
                    html.P(
                        "16-Day Rolling Forecast · Powered by Open-Meteo + Supabase",
                        style={"color": "#aaa", "margin": "4px 0 0", "fontSize": "12px"},
                    ),
                ]),
                html.Div("Louisville, KY · 38.25°N 85.76°W",
                         style={"color": "#aaa", "fontSize": "12px"}),
            ],
        ),

        # Main body
        html.Div(
            style={"display": "flex", "minHeight": "calc(100vh - 80px)"},
            children=[

                # Sidebar
                html.Div(
                    style={
                        "width": "260px", "minWidth": "260px",
                        "background": ACCENT,
                        "padding": "24px 16px",
                        "display": "flex",
                        "flexDirection": "column",
                        "gap": "24px",
                    },
                    children=[
                        html.Div([
                            html.Label("DATE RANGE",
                                       style={"color": "#aaa", "fontSize": "11px",
                                              "fontWeight": "600", "letterSpacing": "1px",
                                              "marginBottom": "8px", "display": "block"}),
                            dcc.DatePickerRange(
                                id="date-range-picker",
                                min_date_allowed=MIN_DATE,
                                max_date_allowed=MAX_DATE,
                                start_date=MIN_DATE,
                                end_date=MAX_DATE,
                                display_format="MMM D, YYYY",
                                style={"width": "100%"},
                            ),
                        ]),
                        html.Div([
                            html.Label("SECTORS",
                                       style={"color": "#aaa", "fontSize": "11px",
                                              "fontWeight": "600", "letterSpacing": "1px",
                                              "marginBottom": "8px", "display": "block"}),
                            dcc.Checklist(
                                id="sector-checklist",
                                options=[{"label": s, "value": s} for s in ALL_SECTORS],
                                value=ALL_SECTORS,
                                labelStyle={"display": "block", "color": LIGHT,
                                            "fontSize": "13px", "padding": "4px 0",
                                            "cursor": "pointer"},
                                inputStyle={"marginRight": "8px", "accentColor": HIGHLIGHT},
                            ),
                        ]),
                        html.Div([
                            html.Label("RISK LEVEL FILTER",
                                       style={"color": "#aaa", "fontSize": "11px",
                                              "fontWeight": "600", "letterSpacing": "1px",
                                              "marginBottom": "8px", "display": "block"}),
                            dcc.Dropdown(
                                id="risk-level-filter",
                                options=[{"label": r.capitalize(), "value": r}
                                         for r in ["calm", "mild", "elevated",
                                                   "high", "severe"]],
                                value=None,
                                placeholder="All risk levels",
                                clearable=True,
                                style={"fontSize": "13px"},
                            ),
                        ]),
                        html.Hr(style={"borderColor": "#555", "margin": "8px 0"}),
                        html.P(
                            "Filters apply to all charts simultaneously. "
                            "Select sectors to focus the impact heatmap.",
                            style={"color": "#888", "fontSize": "11px",
                                   "lineHeight": "1.5"},
                        ),
                    ],
                ),

                # Dashboard content
                html.Div(
                    style={"flex": "1", "padding": "24px", "overflowY": "auto"},
                    children=[

                        # KPI row
                        html.Div(
                            id="kpi-row",
                            style={"display": "flex", "gap": "16px",
                                   "flexWrap": "wrap", "marginBottom": "24px"},
                        ),

                        # Temperature trend + Risk score bar
                        html.Div(
                            style={"display": "flex", "gap": "16px",
                                   "flexWrap": "wrap", "marginBottom": "24px"},
                            children=[
                                html.Div(
                                    dcc.Graph(id="temp-trend-chart",
                                              style={"height": "360px"},
                                              config={"displayModeBar": False}),
                                    style={"flex": "2", "minWidth": "380px",
                                           "background": SECONDARY, "borderRadius": "8px",
                                           "padding": "8px",
                                           "boxShadow": "0 2px 8px rgba(0,0,0,0.3)"},
                                ),
                                html.Div(
                                    dcc.Graph(id="risk-score-chart",
                                              style={"height": "360px"},
                                              config={"displayModeBar": False}),
                                    style={"flex": "1", "minWidth": "300px",
                                           "background": SECONDARY, "borderRadius": "8px",
                                           "padding": "8px",
                                           "boxShadow": "0 2px 8px rgba(0,0,0,0.3)"},
                                ),
                            ],
                        ),

                        # Sector impact heatmap
                        html.Div(
                            dcc.Graph(id="impact-heatmap",
                                      style={"height": "320px"},
                                      config={"displayModeBar": False}),
                            style={"background": SECONDARY, "borderRadius": "8px",
                                   "padding": "8px", "marginBottom": "24px",
                                   "boxShadow": "0 2px 8px rgba(0,0,0,0.3)"},
                        ),

                        # Precipitation + Wind/Humidity
                        html.Div(
                            style={"display": "flex", "gap": "16px",
                                   "flexWrap": "wrap", "marginBottom": "24px"},
                            children=[
                                html.Div(
                                    dcc.Graph(id="precip-chart",
                                              style={"height": "280px"},
                                              config={"displayModeBar": False}),
                                    style={"flex": "1", "minWidth": "300px",
                                           "background": SECONDARY, "borderRadius": "8px",
                                           "padding": "8px",
                                           "boxShadow": "0 2px 8px rgba(0,0,0,0.3)"},
                                ),
                                html.Div(
                                    dcc.Graph(id="wind-humidity-chart",
                                              style={"height": "280px"},
                                              config={"displayModeBar": False}),
                                    style={"flex": "1", "minWidth": "300px",
                                           "background": SECONDARY, "borderRadius": "8px",
                                           "padding": "8px",
                                           "boxShadow": "0 2px 8px rgba(0,0,0,0.3)"},
                                ),
                            ],
                        ),

                        # Footer
                        html.P(
                            "Data: Open-Meteo API · Storage: Supabase PostgreSQL · "
                            "Dashboard: Plotly Dash · Developer: Oluwatosin Adelusi",
                            style={"color": "#555", "fontSize": "11px",
                                   "textAlign": "center", "paddingTop": "8px"},
                        ),
                    ],
                ),
            ],
        ),
    ],
)


# =============================================================================
# FILTER HELPERS
# =============================================================================

def _filter_forecast(start_date, end_date, risk_level):
    df = FORECAST_DF.copy()
    if start_date:
        df = df[df["observation_date"] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df["observation_date"] <= pd.to_datetime(end_date)]
    if risk_level:
        df = df[df["risk_level"] == risk_level]
    return df

def _filter_impact(start_date, end_date, sectors):
    df = IMPACT_DF.copy()
    if start_date:
        df = df[df["observation_date"] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df["observation_date"] <= pd.to_datetime(end_date)]
    if sectors:
        df = df[df["sector_name"].isin(sectors)]
    return df

# =============================================================================
# CALLBACKS
# =============================================================================

@callback(
    Output("kpi-row", "children"),
    Input("date-range-picker", "start_date"),
    Input("date-range-picker", "end_date"),
    Input("risk-level-filter", "value"),
)
def update_kpis(start_date, end_date, risk_level):
    df = _filter_forecast(start_date, end_date, risk_level)
    if df.empty:
        return [kpi_card("No data", "—", "Adjust filters")]

    avg_temp        = f"{df['temp_mean_f'].mean():.1f}°F"
    total_precip    = f"{df['precipitation_in'].sum():.2f} in"
    avg_risk        = f"{df['weather_risk_score'].mean():.0f}/100"
    high_risk_days  = int(df["risk_level"].isin(["high", "severe"]).sum())
    severe_days     = int(df["severe_weather_flag"].sum())

    return [
        kpi_card("Avg Temperature",     avg_temp,
                 f"{len(df)}-day window",         "#3498db"),
        kpi_card("Total Precipitation", total_precip,
                 "Accumulated inches",             "#2ecc71"),
        kpi_card("Avg Risk Score",      avg_risk,
                 "Composite weather risk",         HIGHLIGHT),
        kpi_card("High-Risk Days",
                 f"{high_risk_days} day{'s' if high_risk_days != 1 else ''}",
                 "Levels: high or severe",         "#e67e22"),
        kpi_card("Severe Weather Flags",
                 f"{severe_days} flag{'s' if severe_days != 1 else ''}",
                 "Wind / rain / temp composite",   "#8e44ad"),
    ]

@callback(
    Output("temp-trend-chart", "figure"),
    Input("date-range-picker", "start_date"),
    Input("date-range-picker", "end_date"),
    Input("risk-level-filter", "value"),
)
def update_temp_trend(start_date, end_date, risk_level):
    df = _filter_forecast(start_date, end_date, risk_level)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["observation_date"], y=df["temp_max_f"],
        name="Max Temp", mode="lines+markers",
        line=dict(color="#e74c3c", width=2), marker=dict(size=5),
    ))
    fig.add_trace(go.Scatter(
        x=df["observation_date"], y=df["temp_mean_f"],
        name="Mean Temp", mode="lines+markers",
        line=dict(color="#f39c12", width=2, dash="dash"), marker=dict(size=4),
    ))
    fig.add_trace(go.Scatter(
        x=df["observation_date"], y=df["temp_min_f"],
        name="Min Temp", mode="lines+markers",
        line=dict(color="#3498db", width=2), marker=dict(size=5),
        fill="tonexty", fillcolor="rgba(52,152,219,0.08)",
    ))

    fig.update_layout(
        **CHART_LAYOUT,
        title=dict(text="16-Day Temperature Forecast (°F)",
                   font=dict(size=14), x=0.02),
        yaxis_title="Temperature (°F)",
        hovermode="x unified",
    )
    return fig

@callback(
    Output("risk-score-chart", "figure"),
    Input("date-range-picker", "start_date"),
    Input("date-range-picker", "end_date"),
    Input("risk-level-filter", "value"),
)
def update_risk_score(start_date, end_date, risk_level):
    df = _filter_forecast(start_date, end_date, risk_level)
    bar_colours = [RISK_COLOURS.get(r, "#aaa") for r in df["risk_level"]]

    fig = go.Figure(go.Bar(
        x=df["observation_date"].dt.strftime("%b %d"),
        y=df["weather_risk_score"],
        marker_color=bar_colours,
        text=df["risk_level"].str.capitalize(),
        textposition="outside",
        textfont=dict(size=9, color=LIGHT),
        customdata=df[["weather_description", "temp_mean_f"]].values,
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Risk Score: %{y}<br>"
            "Conditions: %{customdata[0]}<br>"
            "Mean Temp: %{customdata[1]:.1f}°F<extra></extra>"
        ),
    ))
    fig.update_layout(
        **CHART_LAYOUT,
        title=dict(text="Daily Weather Risk Score",
                   font=dict(size=14), x=0.02),
        yaxis_title="Risk Score (0–100)",
        yaxis_range=[0, 110],
        showlegend=False,
    )
    return fig

@callback(
    Output("impact-heatmap", "figure"),
    Input("date-range-picker", "start_date"),
    Input("date-range-picker", "end_date"),
    Input("sector-checklist", "value"),
)
def update_impact_heatmap(start_date, end_date, sectors):
    df = _filter_impact(start_date, end_date, sectors)

    if df.empty:
        fig = go.Figure()
        fig.update_layout(**CHART_LAYOUT,
                          title="Sector Business Impact — No Data")
        return fig

    df["impact_numeric"] = df["impact_level"].map(IMPACT_ORDER).fillna(0)
    df["date_label"]     = df["observation_date"].dt.strftime("%b %d")

    pivot = df.pivot_table(
        index="sector_name", columns="date_label",
        values="impact_numeric", aggfunc="max",
    )
    pivot_text = df.pivot_table(
        index="sector_name", columns="date_label",
        values="impact_level", aggfunc="first",
    )

    date_order = (
        df[["date_label", "observation_date"]]
        .drop_duplicates()
        .sort_values("observation_date")["date_label"]
        .tolist()
    )
    date_order = [d for d in date_order if d in pivot.columns]
    pivot      = pivot.reindex(columns=date_order)
    pivot_text = pivot_text.reindex(columns=date_order)

    colorscale = [
        [0.00, "#1a1a2e"],
        [0.25, "#2ecc71"],
        [0.50, "#e67e22"],
        [0.75, "#e74c3c"],
        [1.00, "#8e44ad"],
    ]

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        text=pivot_text.values,
        texttemplate="%{text}",
        textfont=dict(size=10, color="white"),
        colorscale=colorscale,
        zmin=0, zmax=4,
        showscale=False,
        hovertemplate=(
            "<b>%{y}</b> · %{x}<br>"
            "Impact: %{text}<extra></extra>"
        ),
    ))
    # Apply shared base layout first, then override xaxis separately
    # (can't pass xaxis= twice in the same call — CHART_LAYOUT already has it)
    fig.update_layout(**CHART_LAYOUT)
    fig.update_layout(
        title=dict(text="Sector Business Impact by Day",
                   font=dict(size=14), x=0.02),
        xaxis=dict(side="bottom", tickangle=-30,
                   gridcolor=GRID_FAINT,
                   linecolor=LINE_FAINT,
                   tickcolor=LINE_FAINT),
        margin=dict(l=100, r=16, t=48, b=80),
    )
    return fig

@callback(
    Output("precip-chart", "figure"),
    Input("date-range-picker", "start_date"),
    Input("date-range-picker", "end_date"),
    Input("risk-level-filter", "value"),
)
def update_precip_chart(start_date, end_date, risk_level):
    df = _filter_forecast(start_date, end_date, risk_level)

    cat_colours = {
        "Dry":           "#2ecc71",
        "Light Rain":    "#3498db",
        "Moderate Rain": "#e67e22",
        "Heavy Rain":    "#e74c3c",
    }
    bar_colours = [cat_colours.get(c, "#aaa") for c in df["precip_category"]]

    fig = go.Figure(go.Bar(
        x=df["observation_date"].dt.strftime("%b %d"),
        y=df["precipitation_in"],
        marker_color=bar_colours,
        text=df["precip_category"],
        textposition="outside",
        textfont=dict(size=9, color=LIGHT),
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Precipitation: %{y:.2f} in<br>"
            "Category: %{text}<extra></extra>"
        ),
    ))
    fig.update_layout(
        **CHART_LAYOUT,
        title=dict(text="Daily Precipitation Forecast (inches)",
                   font=dict(size=14), x=0.02),
        yaxis_title="Precipitation (in)",
        showlegend=False,
    )
    return fig


@callback(
    Output("wind-humidity-chart", "figure"),
    Input("date-range-picker", "start_date"),
    Input("date-range-picker", "end_date"),
    Input("risk-level-filter", "value"),
)
def update_wind_humidity(start_date, end_date, risk_level):
    df = _filter_forecast(start_date, end_date, risk_level)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["observation_date"].dt.strftime("%b %d"),
        y=df["wind_speed_max_mph"],
        name="Max Wind (mph)",
        marker_color="#e74c3c",
        yaxis="y",
    ))
    fig.add_trace(go.Scatter(
        x=df["observation_date"].dt.strftime("%b %d"),
        y=df["humidity_pct"],
        name="Humidity (%)",
        mode="lines+markers",
        line=dict(color="#3498db", width=2),
        marker=dict(size=5),
        yaxis="y2",
    ))
    # Apply shared base layout first, then override yaxis separately
    # (can't pass yaxis= twice in the same call — CHART_LAYOUT already has it)
    fig.update_layout(**CHART_LAYOUT)
    fig.update_layout(
        title=dict(text="Wind Speed & Humidity Forecast",
                   font=dict(size=14), x=0.02),
        yaxis=dict(
            title="Max Wind (mph)",
            gridcolor=GRID_FAINT,
            linecolor=LINE_FAINT,
            tickcolor=LINE_FAINT,
        ),
        yaxis2=dict(
            title="Humidity (%)",
            overlaying="y",
            side="right",
            gridcolor=TRANSPARENT,
            linecolor=LINE_FAINT,
            tickcolor=LINE_FAINT,
            range=[0, 120],
        ),
        hovermode="x unified",
        legend=dict(x=0.01, y=0.99, bgcolor="rgba(0,0,0,0)"),
    )
    return fig

# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Louisville Weather & Business Risk Monitor")
    print("Dashboard running at: http://127.0.0.1:8050")
    print("Press Ctrl+C to stop")
    print("=" * 60 + "\n")
    app.run(debug=True, host="127.0.0.1", port=8050)