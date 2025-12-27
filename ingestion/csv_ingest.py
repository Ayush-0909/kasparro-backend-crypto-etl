from datetime import datetime
from schemas.crypto import RawCryptoCSV

def ingest_csv(db):
    """
    Ingest CSV data into raw_crypto_csv table
    """

    sample_rows = [
        {
            "coin_name": "Bitcoin",
            "symbol": "BTC",
            "price_usd": 87500.0,
            "market_cap": 1740000000000.0,
            "volume_24h": 15000000000.0,
            "last_updated": datetime(2025, 12, 27),
        }
    ]

    for row in sample_rows:
        record = RawCryptoCSV(**row)
        db.add(record)

    db.commit()
