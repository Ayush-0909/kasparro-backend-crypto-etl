from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
import time
import uuid

from core.database import get_db
from core.config import init_db
from schemas.crypto import CryptoAsset, ETLRun

# -------------------------
# CREATE APP FIRST
# -------------------------
app = FastAPI(title="Crypto ETL Backend")

# -------------------------
# STARTUP EVENT
# -------------------------
@app.on_event("startup")
def startup_event():
    init_db()

# -------------------------
# HEALTH ENDPOINT
# -------------------------
@app.get("/health")
def health_check(db: Session = Depends(get_db)):
    try:
        db.execute("SELECT 1")
        return {
            "status": "ok",
            "db": "connected"
        }
    except Exception as e:
        return {
            "status": "error",
            "db": "disconnected",
            "error": str(e)
        }

# -------------------------
# DATA ENDPOINT
# -------------------------
@app.get("/data")
def get_data(
    limit: int = 10,
    offset: int = 0,
    source: str | None = None,
    db: Session = Depends(get_db)
):
    request_id = str(uuid.uuid4())
    start_time = time.time()

    query = db.query(CryptoAsset)

    if source:
        query = query.filter(CryptoAsset.source == source)

    records = query.offset(offset).limit(limit).all()

    data = [
        {
            "coin_name": r.coin_name,
            "symbol": r.symbol,
            "price_usd": r.price_usd,
            "market_cap": r.market_cap,
            "volume_24h": r.volume_24h,
            "source": r.source,
            "last_updated": r.last_updated
        }
        for r in records
    ]

    latency_ms = int((time.time() - start_time) * 1000)

    return {
        "request_id": request_id,
        "api_latency_ms": latency_ms,
        "count": len(data),
        "data": data
    }

# -------------------------
# STATS ENDPOINT
# -------------------------
@app.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    last_run = (
        db.query(ETLRun)
        .order_by(ETLRun.run_time.desc())
        .first()
    )

    if not last_run:
        return {"message": "No ETL runs found"}

    return {
        "records_processed": last_run.records_processed,
        "duration_sec": last_run.duration_sec,
        "status": last_run.status,
        "last_run_time": last_run.run_time
    }
