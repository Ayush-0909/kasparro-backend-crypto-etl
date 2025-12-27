import json
import requests
import os

from core.database import SessionLocal
from schemas.crypto import RawCoinGecko

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"


def ingest_coingecko():
    db = SessionLocal()

    try:
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 50,
            "page": 1,
            "sparkline": False
        }

        headers = {}
        api_key = os.getenv("COIN_API_KEY")
        if api_key:
            headers["X-API-KEY"] = api_key

        response = requests.get(
            COINGECKO_URL,
            params=params,
            headers=headers,
            timeout=10
        )
        response.raise_for_status()

        data = response.json()

        # Store FULL raw response
        raw_record = RawCoinGecko(
            payload=json.dumps(data)
        )

        db.add(raw_record)
        db.commit()

    except Exception as e:
        db.rollback()
        print("❌ CoinGecko ingestion failed:", e)

    finally:
        db.close()
