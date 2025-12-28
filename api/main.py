from fastapi import FastAPI, Depends, Query
from sqlalchemy.orm import Session
import time
import uuid

from core.database import SessionLocal, engine, Base
from schemas.crypto import CryptoAsset, ETLRun
from services.etl_service import get_last_etl_status

# --------------------------------------------------
# App initialization
# --------------------------------------------------

app = FastAPI(
    title="Kasparro Crypto ETL API",
    version="1.0.0",
    description="Backend API for Crypto ETL System (Kasparro Assignment)"
)

# --------------------------------------------------
# Database Dependency
# --------------------------------------------------

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --------------------------------------------------
# Root Endpoint (IMPORTANT)
# --------------------------------------------------

@app.get("/")
def root():
    return {
        "message": "Kasparro Crypto ETL API is running",
        "docs": "/docs",
        "health": "/health",
        "data": "/data"
    }

# --------------------------------------------------
# Health Endpoint (P0 REQUIRED)
# --------------------------------------------------

@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute("SELECT 1")
        db_status = "connected"
    except Exception:
        db_status = "disconnected"

    etl_status = get_last_etl_status(db)

    return {
        "database": db_status,
        "etl_last_run": etl_status
    }

# --------------------------------------------------
# Data Endpoint (P0 REQUIRED)
# --------------------------------------------------

@app.get("/data")
def get_data(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    symbol: str | None = None,
    db: Session = Depends(get_db)
):
    start_time = time.time()
    request_id = str(uuid.uuid4())

    query = db.query(CryptoAsset)


    if symbol:
       query = query.filter(CryptoAsset.symbol.ilike(symbol.upper()))


    total = query.count()

    results = (
        query
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    latency_ms = int((time.time() - start_time) * 1000)

    return {
        "request_id": request_id,
        "api_latency_ms": latency_ms,
        "page": page,
        "limit": limit,
        "total_records": total,
        "data": [
            {
                "name": r.name,
                "symbol": r.symbol,
                "price_usd": r.price_usd,
                "market_cap": r.market_cap,
                "source": r.source,
                "last_updated": r.last_updated
            }
            for r in results
        ]
    }

# --------------------------------------------------
# Stats Endpoint (P1 REQUIRED)
# --------------------------------------------------

@app.get("/stats")
def stats(db: Session = Depends(get_db)):
    runs = (
        db.query(ETLRun)
        .order_by(ETLRun.run_time.desc())
        .limit(10)
        .all()
    )

    return {
        "total_runs": len(runs),
        "recent_runs": [
            {
                "status": r.status,
                "records_processed": r.records_processed,
                "duration_sec": r.duration_sec,
                "run_time": r.run_time
            }
            for r in runs
        ]
    }
