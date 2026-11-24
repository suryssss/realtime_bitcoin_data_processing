import os
import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# Databricks SDK and SQL Connector
from databricks import sql
from databricks.sdk.core import Config

# ==============================================================================
# Page Configuration & Styling
# ==============================================================================
st.set_page_config(
    page_title="MarketPulse — Crypto Lakehouse Intelligence",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Institutional Dark Theme CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #1e222d;
        border-radius: 8px;
        padding: 16px;
        border: 1px solid #2a2e39;
    }
    .stMetric > div {
        background-color: #181a20;
        padding: 12px;
        border-radius: 8px;
        border: 1px solid #2b313a;
    }
    .badge-bull {
        background-color: rgba(0, 192, 135, 0.15);
        color: #00C087;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: 600;
    }
    .badge-bear {
        background-color: rgba(255, 59, 48, 0.15);
        color: #FF3B30;
        padding: 4px 8px;
        border-radius: 4px;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


# ==============================================================================
# Connection & Query Helper
# ==============================================================================
@st.cache_resource(ttl=300)
def get_db_client():
    """Initialize Databricks SDK client and resolve SQL Warehouse endpoint."""
    cfg = Config()
    http_path = os.getenv("DATABRICKS_HTTP_PATH") or "/sql/1.0/warehouses/c45421db6f944f52"
    return cfg, http_path


def get_access_token():
    """Extract raw bearer token string from environment or SDK config."""
    token = os.getenv("DATABRICKS_TOKEN")
    if token:
        return token

    cfg = Config()
    auth_headers = cfg.authenticate()
    if isinstance(auth_headers, dict):
        auth_val = auth_headers.get("Authorization", "")
        if auth_val.startswith("Bearer "):
            return auth_val[7:].strip()
        return auth_val.strip()
    return str(auth_headers).strip()


def run_query(query: str, http_path: str) -> pd.DataFrame:
    """Execute SQL query against Databricks Lakehouse via SQL Warehouse."""
    cfg = Config()
    host = os.getenv("DATABRICKS_HOST") or cfg.host

    if not host or not http_path:
        st.error("⚠️ Connection parameters to Databricks are missing.")
        st.stop()

    server_host = host.replace("https://", "").split("?")[0].rstrip("/")
    token = get_access_token()

    with sql.connect(
        server_hostname=server_host,
        http_path=http_path,
        access_token=token
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            try:
                # Use Apache Arrow for high performance
                df = cursor.fetchall_arrow().to_pandas()
            except Exception:
                rows = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=cols)
            return df


# ==============================================================================
# Sidebar Navigation & Settings
# ==============================================================================
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png", width=180)
st.sidebar.title("MarketPulse Controls")

selected_product = st.sidebar.selectbox(
    "Asset / Product Pair",
    options=["BTC-USD", "ETH-USD", "SOL-USD"],
    index=0
)

time_window = st.sidebar.selectbox(
    "History Horizon",
    options=["Last 1 Hour", "Last 6 Hours", "Last 24 Hours"],
    index=0
)

hours_lookup = {"Last 1 Hour": 1, "Last 6 Hours": 6, "Last 24 Hours": 24}[time_window]

auto_refresh = st.sidebar.checkbox("Auto Refresh (Every 10s)", value=False)
if auto_refresh:
    time.sleep(10)
    st.rerun()

st.sidebar.divider()
st.sidebar.markdown("""
**Business Perspectives:**
- 📈 **Market Microstructure:** Candlestick & VWAP
- 💼 **Volume & Flow:** Retail vs. Whale Segmentation
- 🥧 **Market Share:** Asset Allocation & Buy/Sell
- 🛡️ **Risk Surveillance:** Whale Alerts & Spread BPS
""")


# ==============================================================================
# Header & Top KPIs
# ==============================================================================
st.title(f"⚡ MarketPulse — Real-Time Crypto Lakehouse [{selected_product}]")
st.caption(f"Real-time institutional trade execution, VWAP tracking, and anomaly surveillance as of {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")

cfg, http_path = get_db_client()

try:
    with st.spinner("Streaming latest analytics from Databricks Lakehouse..."):

        # ----------------------------------------------------------------------
        # Top KPI Metrics Query
        # ----------------------------------------------------------------------
        kpi_query = f"""
        SELECT 
            close_price,
            price_change,
            price_change_pct,
            candle_type,
            spread_bps,
            window_end
        FROM marketpulse.gold.price_analysis
        WHERE product_id = '{selected_product}'
        ORDER BY window_end DESC
        LIMIT 1;
        """
        kpi_df = run_query(kpi_query, http_path)

        vol_query = f"""
        SELECT 
            ROUND(SUM(total_volume), 4) AS session_vol,
            ROUND(SUM(total_turnover), 2) AS session_turnover,
            SUM(total_trades) AS total_trades,
            ROUND(SUM(buy_volume), 4) AS buy_vol,
            ROUND(SUM(sell_volume), 4) AS sell_vol,
            ROUND(SUM(buy_volume) / NULLIF(SUM(total_volume), 0), 4) AS buy_ratio
        FROM marketpulse.gold.market_metrics
        WHERE product_id = '{selected_product}'
          AND window_start >= NOW() - INTERVAL {hours_lookup} HOURS;
        """
        vol_df = run_query(vol_query, http_path)

        whale_query = f"""
        SELECT 
            SUM(whale_trade_count) AS whale_trades,
            ROUND(SUM(whale_volume) / NULLIF(SUM(total_volume), 0) * 100, 2) AS whale_pct
        FROM marketpulse.gold.volume_analysis
        WHERE product_id = '{selected_product}'
          AND window_start >= NOW() - INTERVAL {hours_lookup} HOURS;
        """
        whale_df = run_query(whale_query, http_path)

        # ----------------------------------------------------------------------
        # Render Top KPI Cards
        # ----------------------------------------------------------------------
        c1, c2, c3, c4, c5 = st.columns(5)

        latest_price = kpi_df["close_price"].iloc[0] if not kpi_df.empty else 0.0
        price_change_pct = kpi_df["price_change_pct"].iloc[0] if not kpi_df.empty else 0.0
        c1.metric(
            label=f"{selected_product} Price",
            value=f"${latest_price:,.2f}",
            delta=f"{price_change_pct:+.2f}%"
        )

        spread_bps = kpi_df["spread_bps"].iloc[0] if not kpi_df.empty else 0.0
        c2.metric(
            label="Candle Spread",
            value=f"{spread_bps:.1f} BPS",
            delta=kpi_df["candle_type"].iloc[0] if not kpi_df.empty else "N/A"
        )

        turnover = vol_df["session_turnover"].iloc[0] if not vol_df.empty and vol_df["session_turnover"].iloc[0] is not None else 0.0
        c3.metric(
            label=f"Session Turnover ({time_window})",
            value=f"${turnover:,.2f}"
        )

        buy_ratio = vol_df["buy_ratio"].iloc[0] if not vol_df.empty and vol_df["buy_ratio"].iloc[0] is not None else 0.50
        sentiment = "Bullish Skew" if buy_ratio > 0.52 else ("Bearish Skew" if buy_ratio < 0.48 else "Neutral")
        c4.metric(
            label="Order Flow Sentiment",
            value=f"{buy_ratio * 100:.1f}% BUY",
            delta=sentiment
        )

        whale_pct = whale_df["whale_pct"].iloc[0] if not whale_df.empty and whale_df["whale_pct"].iloc[0] is not None else 0.0
        whale_trades = whale_df["whale_trades"].iloc[0] if not whale_df.empty and whale_df["whale_trades"].iloc[0] is not None else 0
        c5.metric(
            label="Whale Volume % (> $100k)",
            value=f"{whale_pct:.1f}%",
            delta=f"{whale_trades} Whale Trades"
        )

        st.divider()

        # ======================================================================
        # Four Main Executive Tabs
        # ======================================================================
        tab_biz, tab_price, tab_risk, tab_compare = st.tabs([
            "💼 Business & Flow Intelligence",
            "📈 Price & VWAP Microstructure",
            "🛡️ Risk & Anomaly Surveillance",
            "🌐 Multi-Asset Portfolio Comparison"
        ])

        # ----------------------------------------------------------------------
        # TAB 1: Business & Flow Intelligence (Pie charts, Bar graphs, Flow)
        # ----------------------------------------------------------------------
        with tab_biz:
            st.markdown("### 📊 Executive Liquidity & Trading Flow Analysis")
            st.caption("Distribution of trading volume, order flow directionality, and customer tier segmentation.")

            b_row1_left, b_row1_right = st.columns(2)

            # --- Chart 1A: Donut Chart: Order Flow Sentiment (Buy vs. Sell Volume) ---
            with b_row1_left:
                st.subheader("1. Order Flow Directionality (Taker Buy vs. Sell)")
                buy_vol = vol_df["buy_vol"].iloc[0] if not vol_df.empty and vol_df["buy_vol"].iloc[0] is not None else 0.0
                sell_vol = vol_df["sell_vol"].iloc[0] if not vol_df.empty and vol_df["sell_vol"].iloc[0] is not None else 0.0

                if buy_vol > 0 or sell_vol > 0:
                    fig_flow_donut = go.Figure(data=[go.Pie(
                        labels=["Buyer-Initiated (Taker Buy)", "Seller-Initiated (Taker Sell)"],
                        values=[buy_vol, sell_vol],
                        hole=0.6,
                        marker_colors=["#00C087", "#FF3B30"],
                        textinfo="label+percent",
                        hoverinfo="label+value+percent"
                    )])
                    fig_flow_donut.update_layout(
                        template="plotly_dark",
                        height=360,
                        margin=dict(l=20, r=20, t=30, b=20),
                        annotations=[dict(text=f"{buy_ratio*100:.1f}%<br>BUY", x=0.5, y=0.5, font_size=20, showarrow=False)],
                        legend=dict(orientation="h", y=-0.1)
                    )
                    st.plotly_chart(fig_flow_donut, use_container_width=True)
                else:
                    st.info(f"Awaiting order flow trades for {selected_product}...")

            # --- Chart 1B: Donut Chart: Market Volume Share Across All Coins ---
            with b_row1_right:
                st.subheader("2. Market Turnover Share Across Assets")
                share_query = f"""
                SELECT 
                    product_id,
                    ROUND(SUM(total_turnover), 2) AS turnover_usd,
                    SUM(total_trades) AS trades
                FROM marketpulse.gold.market_metrics
                WHERE window_start >= NOW() - INTERVAL {hours_lookup} HOURS
                GROUP BY product_id;
                """
                share_df = run_query(share_query, http_path)

                if not share_df.empty:
                    fig_share_donut = go.Figure(data=[go.Pie(
                        labels=share_df["product_id"],
                        values=share_df["turnover_usd"],
                        hole=0.6,
                        marker_colors=["#F7931A", "#627EEA", "#14F195"],  # BTC, ETH, SOL Brand Colors
                        textinfo="label+percent",
                        hoverinfo="label+value+percent"
                    )])
                    total_market_turnover = share_df["turnover_usd"].sum()
                    fig_share_donut.update_layout(
                        template="plotly_dark",
                        height=360,
                        margin=dict(l=20, r=20, t=30, b=20),
                        annotations=[dict(text=f"${total_market_turnover:,.0f}<br>Total", x=0.5, y=0.5, font_size=18, showarrow=False)],
                        legend=dict(orientation="h", y=-0.1)
                    )
                    st.plotly_chart(fig_share_donut, use_container_width=True)
                else:
                    st.info("Awaiting multi-asset turnover records...")

            st.divider()

            # --- Chart 1C: Stacked Bar Chart: Customer Tier Volume Segmentation ---
            st.subheader("3. Trader Persona Flow: Retail (<$10k) vs. Mid-Tier ($10k-$100k) vs. Whale (>$100k)")
            vol_seg_query = f"""
            SELECT 
                window_start,
                retail_volume,
                medium_volume,
                whale_volume
            FROM marketpulse.gold.volume_analysis
            WHERE product_id = '{selected_product}'
              AND window_start >= NOW() - INTERVAL {hours_lookup} HOURS
            ORDER BY window_start ASC;
            """
            vol_seg_df = run_query(vol_seg_query, http_path)

            if not vol_seg_df.empty:
                fig_tier_bar = go.Figure()
                fig_tier_bar.add_trace(go.Bar(
                    x=vol_seg_df["window_start"],
                    y=vol_seg_df["retail_volume"],
                    name="Retail (<$10k)",
                    marker_color="#26A69A"
                ))
                fig_tier_bar.add_trace(go.Bar(
                    x=vol_seg_df["window_start"],
                    y=vol_seg_df["medium_volume"],
                    name="Mid-Tier Pro ($10k-$100k)",
                    marker_color="#42A5F5"
                ))
                fig_tier_bar.add_trace(go.Bar(
                    x=vol_seg_df["window_start"],
                    y=vol_seg_df["whale_volume"],
                    name="Institutional Whale (>$100k)",
                    marker_color="#AB47BC"
                ))
                fig_tier_bar.update_layout(
                    barmode="stack",
                    template="plotly_dark",
                    height=400,
                    margin=dict(l=20, r=20, t=30, b=20),
                    xaxis_title="Timeline (UTC)",
                    yaxis_title=f"{selected_product} Volume",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig_tier_bar, use_container_width=True)
            else:
                st.info("Awaiting volume segmentation data...")

            # --- Chart 1D: Trading Velocity: Hourly Turnover vs. Trade Count ---
            st.subheader("4. Trading Velocity & Market Activity: Turnover ($ USD) & Executions")
            velocity_query = f"""
            SELECT 
                window_start,
                ROUND(total_turnover, 2) AS turnover_usd,
                total_trades
            FROM marketpulse.gold.market_metrics
            WHERE product_id = '{selected_product}'
              AND window_start >= NOW() - INTERVAL {hours_lookup} HOURS
            ORDER BY window_start ASC;
            """
            vel_df = run_query(velocity_query, http_path)

            if not vel_df.empty:
                fig_vel = make_subplots(specs=[[{"secondary_y": True}]])
                fig_vel.add_trace(
                    go.Bar(
                        x=vel_df["window_start"],
                        y=vel_df["turnover_usd"],
                        name="Turnover ($ USD)",
                        marker_color="#2962FF",
                        opacity=0.75
                    ),
                    secondary_y=False
                )
                fig_vel.add_trace(
                    go.Scatter(
                        x=vel_df["window_start"],
                        y=vel_df["total_trades"],
                        name="Total Executions",
                        mode="lines+markers",
                        line=dict(color="#FFD600", width=2),
                        marker=dict(size=6)
                    ),
                    secondary_y=True
                )
                fig_vel.update_layout(
                    template="plotly_dark",
                    height=380,
                    margin=dict(l=20, r=20, t=30, b=20),
                    legend=dict(orientation="h", y=1.1)
                )
                fig_vel.update_yaxes(title_text="Turnover ($ USD)", secondary_y=False)
                fig_vel.update_yaxes(title_text="Trade Count", secondary_y=True)
                st.plotly_chart(fig_vel, use_container_width=True)
            else:
                st.info("Awaiting trade velocity metrics...")

        # ----------------------------------------------------------------------
        # TAB 2: Price & VWAP Microstructure (Candlestick, Spread, Tape)
        # ----------------------------------------------------------------------
        with tab_price:
            st.markdown("### 📈 Real-Time Price Action & Execution Microstructure")
            st.caption("1-minute OHLCV candles, 5-minute institutional VWAP spreads, and execution tape.")

            # Candlestick Chart
            st.subheader("1. 1-Minute OHLCV Candlestick & Volume Chart")
            candles_query = f"""
            SELECT 
                window_start,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                trade_count
            FROM marketpulse.gold.price_analysis
            WHERE product_id = '{selected_product}'
              AND window_start >= NOW() - INTERVAL {hours_lookup} HOURS
            ORDER BY window_start ASC;
            """
            candles_df = run_query(candles_query, http_path)

            if not candles_df.empty:
                fig_candle = make_subplots(
                    rows=2, cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.08,
                    row_heights=[0.75, 0.25],
                    subplot_titles=(f"{selected_product} OHLC Price", "Volume")
                )
                fig_candle.add_trace(
                    go.Candlestick(
                        x=candles_df["window_start"],
                        open=candles_df["open_price"],
                        high=candles_df["high_price"],
                        low=candles_df["low_price"],
                        close=candles_df["close_price"],
                        name="Candles",
                        increasing_line_color="#00C087",
                        decreasing_line_color="#FF3B30"
                    ),
                    row=1, col=1
                )
                colors = ['#00C087' if c >= o else '#FF3B30' for o, c in zip(candles_df["open_price"], candles_df["close_price"])]
                fig_candle.add_trace(
                    go.Bar(
                        x=candles_df["window_start"],
                        y=candles_df["volume"],
                        name="Volume",
                        marker_color=colors,
                        opacity=0.8
                    ),
                    row=2, col=1
                )
                fig_candle.update_layout(
                    template="plotly_dark",
                    xaxis_rangeslider_visible=False,
                    height=520,
                    margin=dict(l=20, r=20, t=40, b=20),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig_candle, use_container_width=True)
            else:
                st.info(f"Waiting for incoming candle stream for {selected_product}...")

            # VWAP Benchmark Multi-Line & Slippage Intelligence from gold.vwap_analysis
            st.subheader("2. Institutional 5m VWAP Benchmark & Execution Slippage")
            vwap_query = f"""
            SELECT 
                window_start,
                window_end,
                ROUND(close_price, 2) AS close_price,
                ROUND(vwap, 2) AS vwap_5m,
                ROUND(price_vwap_spread, 2) AS spread,
                ROUND(slippage_bps, 2) AS slippage_bps,
                execution_regime,
                ROUND(total_volume, 4) AS total_volume,
                ROUND(total_turnover, 2) AS turnover_usd,
                ROUND(avg_trade_size, 6) AS avg_trade_size,
                vwap_trend,
                vwap_change_pct
            FROM marketpulse.gold.vwap_analysis
            WHERE product_id = '{selected_product}'
              AND window_start >= NOW() - INTERVAL {hours_lookup} HOURS
            ORDER BY window_start ASC;
            """
            vwap_df = run_query(vwap_query, http_path)

            if not vwap_df.empty:
                # Latest VWAP Microstructure Metrics
                latest_vwap = vwap_df.iloc[-1]
                v_col1, v_col2, v_col3, v_col4 = st.columns(4)
                v_col1.metric("Latest 5m VWAP", f"${latest_vwap['vwap_5m']:,.2f}")
                v_col2.metric("Market Close vs. VWAP", f"${latest_vwap['close_price']:,.2f}", delta=f"{latest_vwap['spread']:+,.2f}")
                v_col3.metric("Execution Slippage", f"{latest_vwap['slippage_bps']:+.2f} bps", delta=latest_vwap['execution_regime'])
                v_col4.metric("5m VWAP Trend", latest_vwap['vwap_trend'], delta=f"{latest_vwap['vwap_change_pct']:+.2f}%")

                fig_vwap = make_subplots(
                    rows=2, cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.1,
                    row_heights=[0.7, 0.3],
                    subplot_titles=("Execution Price vs. 5m VWAP Benchmark", "Slippage Spread (Basis Points)")
                )
                fig_vwap.add_trace(
                    go.Scatter(
                        x=vwap_df["window_start"],
                        y=vwap_df["close_price"],
                        mode="lines+markers",
                        name="Execution Price",
                        line=dict(color="#2962FF", width=2),
                        marker=dict(size=4)
                    ),
                    row=1, col=1
                )
                fig_vwap.add_trace(
                    go.Scatter(
                        x=vwap_df["window_start"],
                        y=vwap_df["vwap_5m"],
                        mode="lines",
                        name="5m VWAP Benchmark",
                        line=dict(color="#FF9800", width=2, dash="dash")
                    ),
                    row=1, col=1
                )
                slippage_colors = ['#FF3B30' if s > 0 else '#00C087' for s in vwap_df["slippage_bps"]]
                fig_vwap.add_trace(
                    go.Bar(
                        x=vwap_df["window_start"],
                        y=vwap_df["slippage_bps"],
                        name="Slippage (bps)",
                        marker_color=slippage_colors,
                        opacity=0.85
                    ),
                    row=2, col=1
                )
                fig_vwap.update_layout(
                    template="plotly_dark",
                    height=500,
                    margin=dict(l=20, r=20, t=30, b=20),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                fig_vwap.update_yaxes(title_text="Price ($ USD)", row=1, col=1)
                fig_vwap.update_yaxes(title_text="Spread (bps)", row=2, col=1)
                st.plotly_chart(fig_vwap, use_container_width=True)

                # Recent VWAP Windows Table
                with st.expander("🔍 View Detailed 5-Minute VWAP Execution Windows"):
                    display_vwap_cols = [
                        "window_start", "window_end", "vwap_5m", "close_price",
                        "spread", "slippage_bps", "execution_regime", "turnover_usd", "total_volume"
                    ]
                    st.dataframe(
                        vwap_df[display_vwap_cols].sort_values(by="window_start", ascending=False),
                        use_container_width=True,
                        hide_index=True
                    )
            else:
                st.info(f"Awaiting continuous pipeline sync for marketpulse.gold.vwap_analysis on {selected_product}...")

            # Live Execution Tape
            st.subheader("3. Live Execution Tape (Recent Trades)")
            tape_query = f"""
            SELECT 
                trade_time,
                trade_id,
                side,
                ROUND(price, 2) AS price,
                ROUND(quantity, 6) AS quantity,
                ROUND(price * quantity, 2) AS notional_usd,
                exchange
            FROM marketpulse.silver.trades
            WHERE product_id = '{selected_product}'
            ORDER BY trade_time DESC
            LIMIT 50;
            """
            tape_df = run_query(tape_query, http_path)
            if not tape_df.empty:
                st.dataframe(tape_df, use_container_width=True, hide_index=True)
            else:
                st.info("Awaiting trades in silver.trades...")

        # ----------------------------------------------------------------------
        # TAB 3: Risk & Anomaly Surveillance (Whale distribution, Spread BPS)
        # ----------------------------------------------------------------------
        with tab_risk:
            st.markdown("### 🛡️ Institutional Risk & Surveillance Analytics")
            st.caption("Whale trade detection, price impact classification, and spread volatility tracking.")

            r_col1, r_col2 = st.columns(2)

            with r_col1:
                # Candle Classification Distribution (Bullish vs. Bearish vs. Inactive)
                st.subheader("1. Price Action Regime Distribution")
                regime_query = f"""
                SELECT 
                    candle_type,
                    COUNT(*) AS count
                FROM marketpulse.gold.price_analysis
                WHERE product_id = '{selected_product}'
                  AND window_start >= NOW() - INTERVAL {hours_lookup} HOURS
                GROUP BY candle_type;
                """
                regime_df = run_query(regime_query, http_path)

                if not regime_df.empty:
                    fig_regime = px.bar(
                        regime_df,
                        x="candle_type",
                        y="count",
                        color="candle_type",
                        color_discrete_map={
                            "Bullish": "#00C087",
                            "Bearish": "#FF3B30",
                            "Doji": "#FFD600",
                            "Neutral": "#757575"
                        },
                        template="plotly_dark",
                        height=360
                    )
                    fig_regime.update_layout(margin=dict(l=20, r=20, t=30, b=20), showlegend=False)
                    st.plotly_chart(fig_regime, use_container_width=True)
                else:
                    st.info("Awaiting price regime classifications...")

            with r_col2:
                # Spread in Basis Points (BPS) Volatility Line
                st.subheader("2. Market Spread Volatility (BPS)")
                spread_query = f"""
                SELECT 
                    window_start,
                    spread_bps
                FROM marketpulse.gold.price_analysis
                WHERE product_id = '{selected_product}'
                  AND window_start >= NOW() - INTERVAL {hours_lookup} HOURS
                ORDER BY window_start ASC;
                """
                spread_df = run_query(spread_query, http_path)

                if not spread_df.empty:
                    fig_spread = px.area(
                        spread_df,
                        x="window_start",
                        y="spread_bps",
                        template="plotly_dark",
                        height=360
                    )
                    fig_spread.update_traces(line_color="#E040FB", fillcolor="rgba(224, 64, 251, 0.15)")
                    fig_spread.update_layout(margin=dict(l=20, r=20, t=30, b=20))
                    st.plotly_chart(fig_spread, use_container_width=True)
                else:
                    st.info("Awaiting spread BPS records...")

            st.divider()

            # Whale Anomalies Table
            st.subheader("3. Institutional Whale Executions (> $100k USD)")
            anomaly_query = f"""
            SELECT 
                trade_time,
                trade_id,
                product_id,
                ROUND(price, 2) AS price,
                ROUND(quantity, 4) AS quantity,
                ROUND(trade_value, 2) AS trade_value_usd,
                anomaly_type
            FROM marketpulse.silver.anomalies
            WHERE trade_time >= NOW() - INTERVAL {hours_lookup} HOURS
            ORDER BY trade_time DESC
            LIMIT 50;
            """
            anomaly_df = run_query(anomaly_query, http_path)

            if not anomaly_df.empty:
                st.dataframe(anomaly_df, use_container_width=True, hide_index=True)
            else:
                st.info("No whale trade anomalies (> $100k USD) detected in this window.")

        # ----------------------------------------------------------------------
        # TAB 4: Multi-Asset Portfolio Comparison (BTC vs ETH vs SOL)
        # ----------------------------------------------------------------------
        with tab_compare:
            st.markdown("### 🌐 Cross-Asset Comparative Benchmarks")
            st.caption("Cross-asset volume turnover, trade intensity, and order flow sentiment comparison.")

            multi_query = f"""
            SELECT 
                product_id,
                ROUND(SUM(total_turnover), 2) AS total_turnover_usd,
                ROUND(SUM(total_volume), 4) AS total_volume_coin,
                SUM(total_trades) AS total_trades,
                ROUND(SUM(buy_volume) / NULLIF(SUM(total_volume), 0) * 100, 2) AS buy_ratio_pct,
                ROUND(SUM(total_turnover) / NULLIF(SUM(total_trades), 0), 2) AS avg_trade_size_usd
            FROM marketpulse.gold.market_metrics
            WHERE window_start >= NOW() - INTERVAL {hours_lookup} HOURS
            GROUP BY product_id
            ORDER BY total_turnover_usd DESC;
            """
            multi_df = run_query(multi_query, http_path)

            if not multi_df.empty:
                st.dataframe(multi_df, use_container_width=True, hide_index=True)

                m_col1, m_col2 = st.columns(2)
                with m_col1:
                    fig_comp_turnover = px.bar(
                        multi_df,
                        x="product_id",
                        y="total_turnover_usd",
                        color="product_id",
                        color_discrete_map={"BTC-USD": "#F7931A", "ETH-USD": "#627EEA", "SOL-USD": "#14F195"},
                        title="Session Turnover ($ USD)",
                        template="plotly_dark",
                        height=360
                    )
                    st.plotly_chart(fig_comp_turnover, use_container_width=True)

                with m_col2:
                    fig_comp_sentiment = px.bar(
                        multi_df,
                        x="product_id",
                        y="buy_ratio_pct",
                        color="product_id",
                        color_discrete_map={"BTC-USD": "#F7931A", "ETH-USD": "#627EEA", "SOL-USD": "#14F195"},
                        title="Order Flow Sentiment (% Buyer-Initiated)",
                        template="plotly_dark",
                        height=360
                    )
                    fig_comp_sentiment.add_hline(y=50, line_dash="dash", line_color="white", annotation_text="Neutral 50%")
                    st.plotly_chart(fig_comp_sentiment, use_container_width=True)
            else:
                st.info("Awaiting cross-asset metrics from the Lakehouse...")

except Exception as err:
    import traceback
    st.error(f"Error querying Databricks Lakehouse: {err}")
    with st.expander("Diagnostic Traceback"):
        st.code(traceback.format_exc())
    st.info("Ensure your Databricks SQL Warehouse is running and permissions are granted on the `marketpulse` catalog.")
