import json
import requests
from datetime import datetime
from schemas.crypto import RawCoinPaprika
from core.config import COINPAPRIKA_API_URL


def ingest_coinpaprika(db):
    print("🌐 CoinPaprika ingestion started")

    response = requests.get(COINPAPRIKA_API_URL, timeout=30)
    response.raise_for_status()

    data = response.json()

    record = RawCoinPaprika(
        payload=json.dumps(data),
        ingested_at=datetime.utcnow()
    )

    db.add(record)
    db.commit()

    print("🌐 CoinPaprika ingestion completed")
