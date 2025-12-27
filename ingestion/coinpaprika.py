import json
import requests
from core.database import SessionLocal
from schemas.crypto import RawCoinPaprika

COINPAPRIKA_URL = "https://api.coinpaprika.com/v1/tickers"


def ingest_coinpaprika():
    db = SessionLocal()

    try:
        response = requests.get(COINPAPRIKA_URL, timeout=10)
        response.raise_for_status()

        data = response.json()

        # Store FULL raw response
        raw_record = RawCoinPaprika(
            payload=json.dumps(data)
        )

        db.add(raw_record)
        db.commit()

    except Exception as e:
        db.rollback()
        print("❌ CoinPaprika ingestion failed:", e)

    finally:
        db.close()
