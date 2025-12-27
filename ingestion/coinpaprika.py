import requests
import json
from datetime import datetime
from sqlalchemy.orm import Session

from schemas.crypto import RawCoinPaprika, CryptoAsset
from core.database import SessionLocal

COINPAPRIKA_URL = "https://api.coinpaprika.com/v1/tickers"


def ingest_coinpaprika():
    db: Session = SessionLocal()

    try:
        response = requests.get(COINPAPRIKA_URL, timeout=10)
        response.raise_for_status()
        coins = response.json()

        for coin in coins[:50]:  # limit to 50 to stay safe
            # ----------------------
            # Store RAW data (P0)
            # ----------------------
            raw = RawCoinPaprika(
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
                price_usd=coin.get("quotes", {}).get("USD", {}).get("price"),
                market_cap=coin.get("quotes", {}).get("USD", {}).get("market_cap"),
                volume_24h=coin.get("quotes", {}).get("USD", {}).get("volume_24h"),
                source="coinpaprika",
                last_updated=datetime.utcnow()
            )
            db.add(asset)

        db.commit()
        print("✅ CoinPaprika ingestion completed")

    except Exception as e:
        db.rollback()
        print("❌ CoinPaprika ingestion failed:", e)

    finally:
        db.close()
