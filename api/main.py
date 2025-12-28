import time
import uuid
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session

from core.database import SessionLocal
from schemas.crypto import CryptoAsset, ETLRun

app = FastAPI(title="Crypto ETL Backend")


# =========================
# DB Dependency
# =========================
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# =========================
# HEALTH CHECK
# =========================
@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        last_run = (
            db.query(ETLRun)
            .order_by(ETLRun.run_time.desc())
            .first()
        )

        return {
            "status": "ok",
            "db_connected": True,
            "last_etl_status": last_run.status if last_run else "unknown"
        }

    except Exception:
        return {
            "status": "error",
            "db_connected": False
        }


# =========================
# DATA ENDPOINT
# =========================
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


# =========================
# STATS ENDPOINT
# =========================
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
        "last_success": next(
            (r.run_time for r in runs if r.status == "success"), None
        ),
        "last_failure": next(
            (r.run_time for r in runs if r.status == "failed"), None
        ),
        "runs": [
            {
                "records_processed": r.records_processed,
                "duration_sec": r.duration_sec,
                "status": r.status,
                "run_time": r.run_time
            }
            for r in runs
        ]
    }
