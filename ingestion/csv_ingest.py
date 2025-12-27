import csv
from datetime import datetime
from core.database import SessionLocal
from schemas.crypto import RawCryptoCSV

CSV_PATH = "data/crypto_prices.csv"


def ingest_csv():
    db = SessionLocal()

    with open(CSV_PATH, newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            record = RawCryptoCSV(
                coin_name=row["coin_name"],
                symbol=row["symbol"],
                price_usd=float(row["price_usd"]),
                market_cap=float(row["market_cap"]),
                volume_24h=float(row["volume_24h"]),
                last_updated=datetime.fromisoformat(row["last_updated"])
            )
            db.add(record)

    db.commit()
    db.close()
