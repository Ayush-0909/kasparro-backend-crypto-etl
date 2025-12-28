import requests
import json
from datetime import datetime

from core.database import SessionLocal
from schemas.crypto import RawCoinGecko
from core.config import COINGECKO_API_URL


def ingest_coingecko(db=None):
    if db is None:
        db = SessionLocal()

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": "false"
    }

    response = requests.get(COINGECKO_API_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    raw = RawCoinGecko(
        payload=json.dumps(data),
        ingested_at=datetime.utcnow()
    )

    db.add(raw)
    db.commit()
