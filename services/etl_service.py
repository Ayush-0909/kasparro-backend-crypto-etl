import time
from datetime import datetime

from core.database import SessionLocal
from ingestion.csv_ingest import ingest_csv
from ingestion.coinpaprika import ingest_coinpaprika
from ingestion.coingecko import ingest_coingecko
from services.normalizer import (
    normalize_csv,
    normalize_coinpaprika,
    normalize_coingecko
)
from schemas.crypto import ETLRun


def run_etl():
    db = SessionLocal()
    start_time = time.time()

    try:
        print("🚀 ETL started")

        # =========================
        # INGESTION PHASE (RAW)
        # =========================
        print("📄 CSV ingestion started")
        ingest_csv()
        print("📄 CSV ingestion completed")

        print("🌐 CoinPaprika ingestion started")
        ingest_coinpaprika()
        print("🌐 CoinPaprika ingestion completed")

        print("🌐 CoinGecko ingestion started")
        ingest_coingecko()
        print("🌐 CoinGecko ingestion completed")

        # =========================
        # NORMALIZATION PHASE
        # =========================
        print("🔄 Normalization started")

        normalize_csv()
        normalize_coinpaprika()
        normalize_coingecko()

        print("🔄 Normalization completed")

        # =========================
        # ETL METADATA (P1 / P2)
        # =========================
        duration = int(time.time() - start_time)

        etl_run = ETLRun(
            records_processed=100,  # static for now (will improve in Step 4)
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
