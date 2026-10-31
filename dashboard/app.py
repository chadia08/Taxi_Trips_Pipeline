
import streamlit as st
import requests
import pandas as pd
import plotly.express as px


# --------------------------------------------------
# Configuration
# --------------------------------------------------

API_URL = "http://127.0.0.1:8000"

st.set_page_config(
    page_title="Chicago Taxi Analytics",
    page_icon="🚕",
    layout="wide"
)


# --------------------------------------------------
# API functions
# --------------------------------------------------

@st.cache_data(ttl=300)
def fetch_data(endpoint):
    try:
        response = requests.get(
            f"{API_URL}{endpoint}",
            timeout=120
        )
        response.raise_for_status()
        return pd.DataFrame(response.json())

    except requests.exceptions.RequestException as e:
        st.error(f"Unable to retrieve data from API: {e}")
        return pd.DataFrame()


# --------------------------------------------------
# Header
# --------------------------------------------------

st.title("🚕 Chicago Taxi Trips Analytics")
st.caption("Business intelligence dashboard | Q1 2023")

st.divider()


# --------------------------------------------------
# Tabs
# --------------------------------------------------

tab_overview, tab_payments, tab_geography, tab_hourly = st.tabs([
    "📊 Overview",
    "💳 Payments",
    "📍 Geography",
    "🕒 Hourly Analysis"
])


# ==================================================
# TAB 1 - OVERVIEW
# ==================================================

with tab_overview:

    st.subheader("Daily Trip Performance")

    df_daily = fetch_data("/api/daily-performance")

    if not df_daily.empty:

        total_trips = df_daily["total_trips"].sum()
        total_revenue = df_daily["total_revenue"].sum()
        avg_daily_trips = df_daily["total_trips"].mean()
        avg_revenue_per_trip = (
            total_revenue / total_trips
            if total_trips > 0 else 0
        )

        col1, col2, col3, col4 = st.columns(4)

        col1.metric("Total Trips", f"{total_trips:,.0f}")
        col2.metric("Total Revenue", f"${total_revenue:,.2f}")
        col3.metric("Avg Daily Trips", f"{avg_daily_trips:,.0f}")
        col4.metric("Avg Revenue / Trip", f"${avg_revenue_per_trip:,.2f}")

        st.divider()

        fig = px.line(
            df_daily,
            x="pickup_date",
            y="total_trips",
            title="Daily Trips Evolution",
            markers=True
        )

        st.plotly_chart(fig, use_container_width=True)

        fig_revenue = px.area(
            df_daily,
            x="pickup_date",
            y="total_revenue",
            title="Daily Revenue Evolution"
        )

        st.plotly_chart(fig_revenue, use_container_width=True)

        with st.expander("View daily data"):
            st.dataframe(df_daily, use_container_width=True)


# ==================================================
# TAB 2 - PAYMENTS
# ==================================================

with tab_payments:

    st.subheader("Payment Analysis")

    df_payments = fetch_data("/api/payments")

    if not df_payments.empty:

        col1, col2 = st.columns(2)

        with col1:
            fig = px.pie(
                df_payments,
                names="payment_type",
                values="total_trips",
                title="Trips by Payment Type",
                hole=0.4
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                df_payments,
                x="payment_type",
                y="total_revenue",
                title="Revenue by Payment Type",
                text_auto=".2s"
            )
            st.plotly_chart(fig, use_container_width=True)

        st.dataframe(df_payments, use_container_width=True)


# ==================================================
# TAB 3 - GEOGRAPHY
# ==================================================

with tab_geography:

    st.subheader("Pickup Area Analysis")

    df_areas = fetch_data("/api/pickup-areas")

    if not df_areas.empty:

        fig = px.bar(
            df_areas.head(15),
            x="pickup_community_area",
            y="total_trips",
            title="Top 15 Pickup Areas by Number of Trips",
            labels={
                "pickup_community_area": "Pickup Area",
                "total_trips": "Total Trips"
            }
        )

        st.plotly_chart(fig, use_container_width=True)

        fig_revenue = px.bar(
            df_areas.head(15),
            x="pickup_community_area",
            y="total_revenue",
            title="Revenue by Pickup Area"
        )

        st.plotly_chart(fig_revenue, use_container_width=True)

        st.dataframe(df_areas, use_container_width=True)


# ==================================================
# TAB 4 - HOURLY ANALYSIS
# ==================================================

with tab_hourly:

    st.subheader("Hourly Trip Analysis")

    df_hourly = fetch_data("/api/hourly-analysis")

    if not df_hourly.empty:

        fig = px.bar(
            df_hourly,
            x="pickup_hour",
            y="total_trips",
            title="Trips Distribution by Hour",
            labels={
                "pickup_hour": "Hour of Day",
                "total_trips": "Total Trips"
            }
        )

        st.plotly_chart(fig, use_container_width=True)

        fig_revenue = px.line(
            df_hourly,
            x="pickup_hour",
            y="total_revenue",
            title="Revenue by Hour",
            markers=True
        )

        st.plotly_chart(fig_revenue, use_container_width=True)

        st.dataframe(df_hourly, use_container_width=True)
