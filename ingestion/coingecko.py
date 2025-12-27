import json
import requests
from datetime import datetime
from schemas.crypto import RawCoinGecko
from core.config import COINGECKO_API_URL


def ingest_coingecko(db):
    print("🌐 CoinGecko ingestion started")

    response = requests.get(COINGECKO_API_URL, timeout=30)
    response.raise_for_status()

    data = response.json()

    record = RawCoinGecko(
        payload=json.dumps(data),
        ingested_at=datetime.utcnow()
    )

    db.add(record)
    db.commit()

    print("🌐 CoinGecko ingestion completed")
