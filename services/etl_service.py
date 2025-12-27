import time
from datetime import datetime

from ingestion.coinpaprika import ingest_coinpaprika
from ingestion.coingecko import ingest_coingecko
from core.database import SessionLocal
from schemas.crypto import ETLRun

def run_etl():
    db = SessionLocal()
    start_time = time.time()

    try:
        print("🚀 ETL started")

        # Run ingestion jobs
        ingest_coinpaprika()
        ingest_coingecko()

        duration = int(time.time() - start_time)

        # Store ETL run metadata (P2)
        etl_run = ETLRun(
            records_processed=100,  # simple static count for now
            duration_sec=duration,
            status="success",
            run_time=datetime.utcnow()
        )
        db.add(etl_run)
        db.commit()

        print("✅ ETL completed successfully")

    except Exception as e:
        db.rollback()
        print("❌ ETL failed:", e)

        etl_run = ETLRun(
            records_processed=0,
            duration_sec=0,
            status="failed",
            run_time=datetime.utcnow()
        )
        db.add(etl_run)
        db.commit()

    finally:
        db.close()
