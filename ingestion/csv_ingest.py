from datetime import datetime
from sqlalchemy.orm import Session

from schemas.crypto import RawCryptoCSV


def ingest_csv(db: Session) -> None:
    """
    Ingest data from a CSV source into raw_crypto_csv table.
    Incremental-safe at raw level (no normalization here).
    """

    print("📄 CSV ingestion started")

    # Example CSV rows (replace with real CSV parsing if needed)
    csv_rows = [
        {
            "coin_name": "Bitcoin",
            "symbol": "BTC",
            "price_usd": 87500.0,
            "market_cap": 1740000000000.0,
            "volume_24h": 15000000000.0,
            "last_updated": datetime(2025, 12, 27),
        }
    ]

    for row in csv_rows:
        record = RawCryptoCSV(
            coin_name=row["coin_name"],
            symbol=row["symbol"],
            price_usd=row["price_usd"],
            market_cap=row["market_cap"],
            volume_24h=row["volume_24h"],
            last_updated=row["last_updated"],
            ingested_at=datetime.utcnow(),
        )
        db.add(record)

    db.commit()

    print("📄 CSV ingestion completed")
