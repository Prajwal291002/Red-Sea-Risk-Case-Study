import streamlit as st
import pandas as pd
import pyodbc
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Red Sea Risk Radar",
    page_icon="🚢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 1. DATA LOADING ---
# Cache data to prevent reloading on every interaction
@st.cache_data
def load_data():
    # Configuration
    # Note: If running locally, ensure your SQL Server is accessible via localhost
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost,1433;"
        "UID=sa;"
        "PWD=MyComplexPassword123!;"
        "TrustServerCertificate=yes;"
    )

    try:
        conn = pyodbc.connect(conn_str)
        query = """
        SELECT full_date, Price, news_count, avg_conflict_score 
        FROM Gold_Analytics_RedSea 
        ORDER BY full_date ASC
        """
        df = pd.read_sql(query, conn)
        conn.close()

        # Pre-processing
        df['full_date'] = pd.to_datetime(df['full_date'])
        
        # Risk Score Calculation (Volume * Intensity)
        # Since avg_conflict_score is already positive (High=Bad), we just multiply.
        df['Risk_Score'] = df['news_count'] * df['avg_conflict_score']
        
        # Rounding
        df['Price'] = df['Price'].round(2)
        df['Risk_Score'] = df['Risk_Score'].round(2)
        df['avg_conflict_score'] = df['avg_conflict_score'].round(2)

        return df

    except Exception as e:
        st.error(f"❌ Database Connection Error: {e}")
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("No data loaded. Please check your database connection.")
    st.stop()

# --- 2. SIDEBAR CONTROLS ---
st.sidebar.header("⚙️ Control Panel")
st.sidebar.write("Filter the analysis window:")

# Date Range Picker
min_date = df['full_date'].min().date()
max_date = df['full_date'].max().date()

# Set default range to the "Crisis Window" (Oct 2023 - Feb 2024)
default_start = pd.to_datetime("2023-11-15").date()
default_end = pd.to_datetime("2024-01-31").date()

# Ensure defaults are within data bounds
if default_start < min_date: default_start = min_date
if default_end > max_date: default_end = max_date

start_date, end_date = st.sidebar.date_input(
    "Select Date Range",
    value=(default_start, default_end),
    min_value=min_date,
    max_value=max_date
)

# Filter Data
mask = (df['full_date'].dt.date >= start_date) & (df['full_date'].dt.date <= end_date)
filtered_df = df.loc[mask]

if filtered_df.empty:
    st.warning("No data available for the selected date range.")
    st.stop()


# --- 3. MAIN DASHBOARD LAYOUT ---

# Title Section
st.title("🛑 Red Sea Risk Radar")
st.markdown("""
**Executive Control Tower:** Correlating unstructured geopolitical news signals with structured logistics financial data.
This dashboard demonstrates the **Lag Effect** between conflict events and shipping price surges.
""")
st.divider()

# KPI Row
col1, col2, col3 = st.columns(3)

# KPI 1: Peak Price
max_price = filtered_df['Price'].max()
col1.metric(
    label="💰 Peak Shipping Rate ($/FEU)",
    value=f"${max_price:,.0f}",
    delta=None
)

# KPI 2: Total Events
total_news = filtered_df['news_count'].sum()
col2.metric(
    label="📰 Total Conflict Events",
    value=f"{total_news:,.0f}",
    delta=None
)

# KPI 3: Correlation
if len(filtered_df) > 1:
    correlation = filtered_df['Price'].corr(filtered_df['Risk_Score'])
    corr_val = f"{correlation:.2f}"
else:
    corr_val = "N/A"
    
col3.metric(
    label="🔗 Correlation Strength (r)",
    value=corr_val,
    help="Pearson correlation between Risk Score and Price. Higher is stronger."
)

st.markdown("---")

# --- CHART 1: MAIN TREND (Dual Axis) ---
st.subheader("📈 Lag Analysis: News Signals vs. Market Reaction")

fig_trend = make_subplots(specs=[[{"secondary_y": True}]])

# Trace 1: Risk Score (Bars)
fig_trend.add_trace(
    go.Bar(
        x=filtered_df['full_date'], 
        y=filtered_df['Risk_Score'], 
        name="Risk Intensity", 
        marker_color='rgba(231, 76, 60, 0.7)'
    ),
    secondary_y=False
)

# Trace 2: Price (Line)
fig_trend.add_trace(
    go.Scatter(
        x=filtered_df['full_date'], 
        y=filtered_df['Price'], 
        name="Shipping Price", 
        line=dict(color='#2c3e50', width=4)
    ),
    secondary_y=True
)

fig_trend.update_layout(
    template="plotly_white",
    legend=dict(orientation="h", y=1.1, x=0.5, xanchor='center'),
    height=500,
    hovermode="x unified"
)
fig_trend.update_yaxes(title_text="Risk Score (Vol x Tone)", secondary_y=False, showgrid=False)
fig_trend.update_yaxes(title_text="Container Price ($)", secondary_y=True, showgrid=True)

st.plotly_chart(fig_trend, use_container_width=True)


# --- ROW 2: DEEP DIVE CHARTS ---
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("🔥 The 'Panic Meter'")
    st.caption("Bubble Size = News Volume | Color = Price Level")
    
    fig_scatter = px.scatter(
        filtered_df, 
        x='full_date', 
        y='avg_conflict_score', 
        size='news_count', 
        color='Price',
        size_max=35, 
        color_continuous_scale='RdBu_r',
        template='plotly_white'
    )
    fig_scatter.update_yaxes(title="Sentiment Severity (High = Bad)")
    fig_scatter.update_layout(height=400)
    st.plotly_chart(fig_scatter, use_container_width=True)

with col_right:
    st.subheader("⚡ Risk Signal Volatility")
    st.caption("Cumulative pressure of the news cycle over time")
    
    fig_risk = px.line(
        filtered_df, 
        x='full_date', 
        y='Risk_Score',
        template='plotly_white'
    )
    fig_risk.update_traces(
        line_color='#e74c3c', 
        line_width=3, 
        fill='tozeroy', 
        fillcolor='rgba(231, 76, 60, 0.1)'
    )
    fig_risk.update_yaxes(title="Cumulative Risk Score")
    fig_risk.update_layout(height=400)
    st.plotly_chart(fig_risk, use_container_width=True)


# --- RAW DATA EXPANDER ---
with st.expander("📂 View Raw Data Table"):
    st.dataframe(filtered_df.sort_values(by='full_date', ascending=False), use_container_width=True)