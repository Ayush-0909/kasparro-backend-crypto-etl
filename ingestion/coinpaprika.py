import json
from datetime import datetime
import requests
from sqlalchemy.orm import Session

from schemas.crypto import RawCoinPaprika
from core.config import COINPAPRIKA_API_URL


def ingest_coinpaprika(db: Session) -> None:
    print("🌐 CoinPaprika ingestion started")

    response = requests.get(COINPAPRIKA_API_URL, timeout=30)
    response.raise_for_status()

    data = response.json()

    record = RawCoinPaprika(
        payload=json.dumps(data),  # ✅ FIX: serialize list → string
        ingested_at=datetime.utcnow()
    )

    db.add(record)
    db.commit()

    print("🌐 CoinPaprika ingestion completed")
