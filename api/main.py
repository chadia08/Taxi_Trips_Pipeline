
from fastapi import FastAPI, HTTPException

from api.database import execute_query


app = FastAPI(
    title="Chicago Taxi Analytics API",
    description="API for querying Chicago Taxi Trips Gold datasets",
    version="1.0.0"
)


@app.get("/")
def root():
    return {
        "message": "Welcome to Chicago Taxi Analytics API",
        "docs": "/docs"
    }


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "Chicago Taxi Analytics API"
    }


@app.get("/api/daily-performance")
def get_daily_performance():
    try:
        return execute_query("daily_trip_performance.sql")
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving daily performance: {str(e)}"
        )


@app.get("/api/payments")
def get_payment_analysis():
    try:
        return execute_query("payment_analysis.sql")
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving payment analysis: {str(e)}"
        )


@app.get("/api/pickup-areas")
def get_pickup_area_analysis():
    try:
        return execute_query("pickup_area_analysis.sql")
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving pickup area analysis: {str(e)}"
        )


@app.get("/api/hourly-analysis")
def get_hourly_analysis():
    try:
        return execute_query("hourly_trip_analysis.sql")
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving hourly analysis: {str(e)}"
        )
