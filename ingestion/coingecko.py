import requests
import json
from datetime import datetime
from sqlalchemy.orm import Session

from schemas.crypto import RawCoinGecko, CryptoAsset
from core.database import SessionLocal

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"


def ingest_coingecko():
    db: Session = SessionLocal()

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 50,
        "page": 1,
        "sparkline": False
    }

    try:
        response = requests.get(COINGECKO_URL, params=params, timeout=10)
        response.raise_for_status()
        coins = response.json()

        for coin in coins:
            # ----------------------
            # Store RAW data (P0)
            # ----------------------
            raw = RawCoinGecko(
                data=json.dumps(coin),
                fetched_at=datetime.utcnow()
            )
            db.add(raw)

            # ----------------------
            # Normalize data (P1)
            # ----------------------
            asset = CryptoAsset(
                coin_name=coin.get("name"),
                symbol=coin.get("symbol"),
                price_usd=coin.get("current_price"),
                market_cap=coin.get("market_cap"),
                volume_24h=coin.get("total_volume"),
                source="coingecko",
                last_updated=datetime.utcnow()
            )
            db.add(asset)

        db.commit()
        print("✅ CoinGecko ingestion completed")

    except Exception as e:
        db.rollback()
        print("❌ CoinGecko ingestion failed:", e)

    finally:
        db.close()
