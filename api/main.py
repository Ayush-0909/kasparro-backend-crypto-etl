from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
import time
import uuid

from core.database import get_db
from schemas.crypto import CryptoAsset, ETLRun

app = FastAPI(title="Kasparro Crypto ETL API")


# -----------------------------
# ROOT (CRITICAL FOR RENDER)
# -----------------------------
@app.get("/")
def root():
    return {"message": "Kasparro Backend is running"}


# -----------------------------
# HEALTH CHECK
# -----------------------------
@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute("SELECT 1")
        etl = db.query(ETLRun).order_by(ETLRun.run_time.desc()).first()
        return {
            "status": "ok",
            "database": "connected",
            "last_etl_status": etl.status if etl else "never run"
        }
    except Exception as e:
        return {"status": "error", "details": str(e)}


# -----------------------------
# DATA ENDPOINT
# -----------------------------
@app.get("/data")
def get_data(
    page: int = 1,
    limit: int = 20,
    source: str | None = None,
    db: Session = Depends(get_db)
):
    start = time.time()
    request_id = str(uuid.uuid4())

    query = db.query(CryptoAsset)
    if source:
        query = query.filter(CryptoAsset.source == source)

    total = query.count()
    results = (
        query
        .offset((page - 1) * limit)
        .limit(limit)
        .all()
    )

    latency = int((time.time() - start) * 1000)

    return {
        "request_id": request_id,
        "api_latency_ms": latency,
        "page": page,
        "limit": limit,
        "total": total,
        "data": results
    }


# -----------------------------
# STATS ENDPOINT
# -----------------------------
@app.get("/stats")
def stats(db: Session = Depends(get_db)):
    runs = db.query(ETLRun).order_by(ETLRun.run_time.desc()).all()

    if not runs:
        return {"message": "No ETL runs yet"}

    return {
        "total_runs": len(runs),
        "last_run": runs[0].run_time,
        "last_status": runs[0].status,
        "runs": [
            {
                "status": r.status,
                "duration_sec": r.duration_sec,
                "records_processed": r.records_processed,
                "run_time": r.run_time
            }
            for r in runs
        ]
    }
